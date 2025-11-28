# redis_ws_tcp_proxy.py

#!/usr/bin/env python3
"""
TCP -> WebSocket proxy for Redis on Render-like WebSocket backends.

- Listens on 127.0.0.1:6380 (alterar LOCAL_HOST/LOCAL_PORT se quiser)
- Accepts RESP from redis-cli, parseia mensagens completas (suporta pipelining)
- Converte para comando inline (FORÇA uppercase no nome do comando)
- Anexa CRLF e envia como texto ao WebSocket upstream
- Intercepta COMMAND e responde localmente (faz o redis-cli não travar)
- Encaminha respostas do WS de volta pro cliente TCP
"""
import asyncio
import logging
from websockets import connect
from typing import Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("redis-proxy")

# CONFIGURE AQUI
WS_URL = "wss://redisrender.onrender.com"   # seu endpoint WebSocket
TOKEN = "SEU_TOKEN_AQUI"                    # token se precisar, ou ""/None
LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 6380

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
    if buf[0:1] != b'*':
        # Não é uma mensagem RESP por array — ignoramos (poderia ser inline, mas redis-cli usa RESP).
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
        if buf[idx:idx+1] != b'$':
            return None, 0
        pos2 = buf.find(b"\r\n", idx)
        if pos2 == -1:
            return None, 0
        try:
            arg_len = int(buf[idx+1:pos2].decode())
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
        if buf[idx:idx+2] != b"\r\n":
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
    Pode ser ajustada se necessário.
    """
    # Vamos retornar um array vazio (o redis-cli aceita e segue).
    # Alternativa: retornar informações reais de comando — aqui simplificamos.
    return b"*0\r\n"


# ---------- handler por cliente TCP ----------
async def handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    log.info(f"[TCP] Cliente conectado: {peer}")

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
                    log.info("[TCP] Cliente fechou a conexão")
                    break
                tcp_buffer.extend(data)
                # tenta extrair e processar todas as mensagens RESP completas no buffer
                while True:
                    cmd, consumed = parse_first_resp_message(bytes(tcp_buffer))
                    if consumed == 0 or cmd is None:
                        break
                    # remove bytes consumidos
                    del tcp_buffer[:consumed]

                    log.info(f"[TCP → WS] {cmd!r}")

                    # intercepta COMMAND localmente
                    if cmd.upper() == "COMMAND":
                        log.info("[INTERCEPT] Respondendo COMMAND localmente")
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
                log.info(f"[WS → TCP] {b!r}")
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

    log.info(f"[SESSION] encerrada para {peer}")


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
