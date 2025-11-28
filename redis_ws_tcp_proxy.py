#!/usr/bin/env python3
"""
TCP -> WebSocket proxy for Redis em backends tipo Render.

- Escuta em 127.0.0.1:6380 (config via env LOCAL_HOST/LOCAL_PORT)
- Aceita RESP (redis-cli / ARQ), parseia mensagens completas (suporta pipelining)
- Converte pra comando inline (nome do comando UPPERCASE)
- Envia como texto pro WebSocket upstream
- Intercepta COMMAND localmente (pro redis-cli não travar)
- Encaminha respostas do WS pro cliente TCP
"""

import os
import asyncio
import logging
from typing import Tuple, Optional

from websockets import connect

# ----------------- LOGGING -----------------
LOG_LEVEL = os.getenv("REDIS_PROXY_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("redis-proxy")

# reduzir ruído da lib websockets
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websockets.client").setLevel(logging.WARNING)
logging.getLogger("websockets.asyncio.client").setLevel(logging.WARNING)
logging.getLogger("websockets.http11").setLevel(logging.WARNING)

# ----------------- CONFIG por ENV -----------------
WS_URL = os.getenv("REDIS_WS_URL", "wss://redisrender.onrender.com")
TOKEN = os.getenv("REDIS_TOKEN", "")  # se não precisar de token, deixe vazio

LOCAL_HOST = os.getenv("REDIS_PROXY_HOST", "127.0.0.1")
LOCAL_PORT = int(os.getenv("REDIS_PROXY_PORT", "6380"))
# --------------------------------------------------


# ---------- helpers RESP parsing ----------
def parse_first_resp_message(buf: bytes) -> Tuple[Optional[str], int]:
    """
    Tenta parsear a PRIMEIRA mensagem RESP do buffer `buf`.
    Se bem-sucedido retorna (inline_cmd_str, bytes_consumed).
    Se incomplete/indecifrável retorna (None, 0).
    Suporta apenas arrays de bulk-strings (forma padrão do redis-cli).
    """
    if not buf:
        return None, 0
    if buf[0:1] != b"*":
        # Não é uma mensagem RESP por array — ignoramos (redis-py usa RESP).
        return None, 0

    # encontra fim da primeira linha "*<n>\r\n"
    pos = buf.find(b"\r\n")
    if pos == -1:
        return None, 0
    try:
        num_args = int(buf[1:pos].decode())
    except Exception:
        return None, 0

    idx = pos + 2
    args = []
    for _ in range(num_args):
        if idx >= len(buf):
            return None, 0
        if buf[idx:idx + 1] != b"$":
            return None, 0
        pos2 = buf.find(b"\r\n", idx)
        if pos2 == -1:
            return None, 0
        try:
            arg_len = int(buf[idx + 1:pos2].decode())
        except Exception:
            return None, 0
        idx = pos2 + 2
        if len(buf) < idx + arg_len + 2:
            return None, 0
        arg = buf[idx: idx + arg_len]
        try:
            args.append(arg.decode("utf-8", errors="ignore"))
        except Exception:
            args.append(arg.decode("latin1", errors="ignore"))
        idx += arg_len
        # esperar CRLF
        if buf[idx:idx + 2] != b"\r\n":
            return None, 0
        idx += 2

    # montar inline command (primeira palavra uppercase)
    if not args:
        return None, idx
    inline = " ".join(args)
    parts = inline.split(" ", 1)
    name = parts[0].upper()
    inline_cmd = name if len(parts) == 1 else name + " " + parts[1]
    return inline_cmd, idx


def fake_command_response_bytes() -> bytes:
    """
    Resposta simples para o comando COMMAND (suficiente para redis-cli).
    """
    # Array vazio (redis-cli aceita).
    return b"*0\r\n"


# ---------- handler por cliente TCP ----------
async def handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    log.debug(f"[TCP] Cliente conectado: {peer}")

    # conecta ao WebSocket upstream
    ws_url = f"{WS_URL}?token={TOKEN}" if TOKEN else WS_URL
    try:
        ws = await connect(ws_url)
    except Exception as e:
        log.error(f"[WS] Falha ao conectar ao WS upstream: {e}")
        writer.write(b"-ERR cannot connect to upstream\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    log.info(f"[WS] Conectado ao upstream: {ws_url}")

    # buffers / tasks
    tcp_buffer = bytearray()

    async def tcp_reader_loop():
        nonlocal tcp_buffer
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    log.debug("[TCP] Cliente fechou a conexão")
                    break
                tcp_buffer.extend(data)
                # tenta extrair e processar todas as mensagens RESP completas no buffer
                while True:
                    cmd, consumed = parse_first_resp_message(bytes(tcp_buffer))
                    if consumed == 0 or cmd is None:
                        break
                    # remove bytes consumidos
                    del tcp_buffer[:consumed]

                    log.debug(f"[TCP → WS] {cmd!r}")

                    # intercepta COMMAND localmente
                    if cmd.upper() == "COMMAND":
                        log.debug("[INTERCEPT] Respondendo COMMAND localmente")
                        writer.write(fake_command_response_bytes())
                        await writer.drain()
                        continue

                    # envia para WS como texto + CRLF
                    outbound = cmd + "\r\n"
                    try:
                        await ws.send(outbound)
                    except Exception as e:
                        log.error(f"[WS] Erro ao enviar: {e}")
                        raise
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.exception(f"[tcp_reader_loop] exceção: {e}")
        finally:
            try:
                await ws.close()
            except Exception:
                pass
            try:
                writer.close()
            except Exception:
                pass

    async def ws_reader_loop():
        try:
            async for msg in ws:
                # msg pode ser str (texto) ou bytes (binary)
                if isinstance(msg, str):
                    s = msg
                    # garantir CRLF para o cliente redis-cli
                    if not s.endswith("\r\n"):
                        s = s + "\r\n"
                    b = s.encode()
                else:
                    # bytes - encaminha tal qual (mas se não terminar em CRLF, append)
                    b = bytes(msg)
                    if not b.endswith(b"\r\n"):
                        b = b + b"\r\n"
                log.debug(f"[WS → TCP] {b!r}")
                try:
                    writer.write(b)
                    await writer.drain()
                except Exception as e:
                    log.error(f"[TCP write] erro: {e}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.exception(f"[ws_reader_loop] exceção: {e}")
        finally:
            try:
                writer.close()
            except Exception:
                pass

    # criar tarefas e aguardar conclusão (qualquer uma das duas fechando termina a sessão)
    task_tcp = asyncio.create_task(tcp_reader_loop())
    task_ws = asyncio.create_task(ws_reader_loop())

    done, pending = await asyncio.wait([task_tcp, task_ws], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    # cleanup
    try:
        await ws.close()
    except Exception:
        pass
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass

    log.debug(f"[SESSION] encerrada para {peer}")


# ---------- servidor TCP ----------
async def main():
    server = await asyncio.start_server(handle_tcp_client, LOCAL_HOST, LOCAL_PORT)
    addr = server.sockets[0].getsockname()
    log.info(f"Proxy escutando em tcp://{addr[0]}:{addr[1]}")
    log.info(f"Upstream WS: {WS_URL}  token={'<SET' if TOKEN else '<NONE>'}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrompido pelo usuário")
    except Exception:
        log.exception("Erro no proxy")
