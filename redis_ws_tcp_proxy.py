#!/usr/bin/env python3
"""
TCP -> WebSocket proxy for Redis (preserva RESP frames).

- Escuta em 127.0.0.1:6380 (config via env REDIS_PROXY_HOST/REDIS_PROXY_PORT)
- Aceita RESP do cliente (redis-cli / redis-py)
- Encaminha o FRAME RESP ORIGINAL (bytes) para o upstream WebSocket
  (permanece fiel aos bulk-strings; NÃO converte para inline)
- Intercepta COMMAND localmente (para redis-cli não travar)
- Encaminha respostas do WS de volta pro cliente TCP
"""
import asyncio
import logging
import os
from websockets import connect
from typing import Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("redis-proxy")

# CONFIG
WS_URL = os.getenv("REDIS_WS_URL", "wss://redisrender.onrender.com")
TOKEN = os.getenv("REDIS_TOKEN", "")  # se vazio, sem token
LOCAL_HOST = os.getenv("REDIS_PROXY_HOST", "127.0.0.1")
LOCAL_PORT = int(os.getenv("REDIS_PROXY_PORT", "6380"))

# ---------- helpers RESP parsing ----------
def parse_first_resp_message(buf: bytes) -> Tuple[Optional[int], int]:
    """
    Tenta parsear a PRIMEIRA mensagem RESP do buffer `buf`.
    Se bem-sucedido retorna (start_index, bytes_consumed) sendo start_index normalmente 0.
    Se incompleto/indecifrável retorna (None, 0).

    NOTA: aqui nós apenas detectamos onde termina a *primeira* mensagem RESP
    (suporta arrays de bulk-strings — formato que redis-cli usa).
    Não montamos inline — vamos enviar os BYTES originais.
    """
    if not buf:
        return None, 0
    # só aceitamos ARRAY (`*<n>\r\n`) — forma usada por redis-cli / clientes RESP
    if buf[0:1] != b'*':
        return None, 0
    pos = buf.find(b"\r\n")
    if pos == -1:
        return None, 0
    try:
        num_args = int(buf[1:pos].decode(errors="ignore"))
    except Exception:
        return None, 0
    idx = pos + 2
    for _ in range(num_args):
        if idx >= len(buf):
            return None, 0
        if buf[idx:idx+1] != b'$':
            return None, 0
        pos2 = buf.find(b"\r\n", idx)
        if pos2 == -1:
            return None, 0
        try:
            arg_len = int(buf[idx+1:pos2].decode(errors="ignore"))
        except Exception:
            return None, 0
        idx = pos2 + 2
        # falta bytes do argumento + CRLF?
        if len(buf) < idx + arg_len + 2:
            return None, 0
        idx += arg_len
        # verificar CRLF final do bulk
        if buf[idx:idx+2] != b"\r\n":
            return None, 0
        idx += 2
    # idx agora aponta pra primeira posição após a mensagem completa
    return 0, idx

def fake_command_response_bytes() -> bytes:
    """Resposta simples para o comando COMMAND (suficiente para redis-cli)."""
    # Array vazio RESP
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
                # tenta extrair mensagens RESP completas e repassá-las INTEIRAS (bytes)
                while True:
                    start, consumed = parse_first_resp_message(bytes(tcp_buffer))
                    if consumed == 0 or start is None:
                        break
                    # pega o slice exato RESP (preserva bulk-strings com espaços)
                    outbound_bytes = bytes(tcp_buffer[:consumed])
                    # remove do buffer
                    del tcp_buffer[:consumed]

                    # Se for comando COMMAND (por segurança, sniff), devolve fake response local
                    # Vamos inspecionar o *início* do frame (não textual): por simplicidade,
                    # convertendo a primeira bulk-string (o nome do comando).
                    try:
                        # parse rápido do nome do comando (primeiro bulk-string)
                        # forma: *N\r\n$len\r\nCOMMAND\r\n...
                        # buscamos primeiro '$' após '*' e então pegar os bytes do primeiro bulk
                        buf = outbound_bytes
                        p = buf.find(b'\r\n')
                        # p aponta fim de "*N\r\n"
                        # achar next '$' (começo do bulk)
                        q = buf.find(b'\r\n', p+2)
                        # find the start of the bulk content
                        # but simplest is to find the first occurrence of b'\r\n' after the $len line,
                        # then extract the command name bytes between that and the following \r\n.
                        # fallback: try decode safely
                        # We'll do a safer decode attempt:
                        parts = buf.split(b'\r\n', 4)
                        # parts -> [b'*N', b'$len', b'CMD', ...] if present
                        if len(parts) >= 3:
                            cmd_name = parts[2].decode(errors='ignore').upper()
                        else:
                            cmd_name = ""
                    except Exception:
                        cmd_name = ""

                    if cmd_name == "COMMAND":
                        log.info("[INTERCEPT] Respondendo COMMAND localmente")
                        writer.write(fake_command_response_bytes())
                        await writer.drain()
                        continue

                    # envia os BYTES RESP originais para o WS (binary)
                    try:
                        await ws.send(outbound_bytes)  # envia bytes, preservando o RESP
                        log.debug(f"[TCP → WS] forwarded {len(outbound_bytes)} bytes (RESP preserved)")
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
                    b = msg.encode()
                else:
                    b = bytes(msg)
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
                await writer.wait_closed()
            except Exception:
                pass

    task_tcp = asyncio.create_task(tcp_reader_loop())
    task_ws = asyncio.create_task(ws_reader_loop())
    done, pending = await asyncio.wait([task_tcp, task_ws], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

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
    log.info(f"Upstream WS: {WS_URL}  token={'<SET>' if TOKEN else '<NONE>'}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrompido pelo usuário")
    except Exception:
        log.exception("Erro no proxy")
