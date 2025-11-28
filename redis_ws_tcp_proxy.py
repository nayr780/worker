#!/usr/bin/env python3
"""
TCP -> WebSocket proxy para Redis.

- Escuta em LOCAL_HOST:LOCAL_PORT (config via env REDIS_PROXY_HOST/REDIS_PROXY_PORT)
- Aceita clientes Redis (redis-py / ARQ / redis-cli)
- Encaminha bytes brutos via WebSocket para um serviço WS upstream
  (ex: seu serviço em Render que fala WS <-> Redis TCP).
- Não parseia RESP, não converte pra "inline", não altera comandos.
"""

import os
import asyncio
import logging
from typing import Optional

from websockets import connect

# ----------------- LOGGING -----------------
LOG_LEVEL = os.getenv("REDIS_PROXY_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("redis-proxy")

# diminuir ruído da lib websockets
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


async def handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    log.info(f"[TCP] Cliente conectado: {peer}")

    ws_url = f"{WS_URL}?token={TOKEN}" if TOKEN else WS_URL
    try:
        ws = await connect(ws_url)
    except Exception as e:
        log.error(f"[WS] Falha ao conectar ao WS upstream: {e}")
        try:
            writer.write(b"-ERR cannot connect to upstream\r\n")
            await writer.drain()
        except Exception:
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return

    log.info(f"[WS] Conectado ao upstream: {ws_url}")

    async def tcp_to_ws():
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    log.debug("[TCP] EOF do cliente")
                    break
                # manda bytes crus pro WS (binary frame)
                await ws.send(data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[tcp_to_ws] erro: {e}")
        finally:
            try:
                await ws.close()
            except Exception:
                pass
            try:
                writer.close()
            except Exception:
                pass

    async def ws_to_tcp():
        try:
            async for msg in ws:
                # msg pode ser str ou bytes
                if isinstance(msg, str):
                    data = msg.encode("utf-8", errors="ignore")
                else:
                    data = bytes(msg)
                writer.write(data)
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[ws_to_tcp] erro: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    t1 = asyncio.create_task(tcp_to_ws())
    t2 = asyncio.create_task(ws_to_tcp())

    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    log.info(f"[SESSION] encerrada para {peer}")


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
