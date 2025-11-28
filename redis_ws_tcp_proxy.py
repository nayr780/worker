#!/usr/bin/env python3
"""
TCP -> WebSocket proxy para Redis.

- Escuta em LOCAL_HOST:LOCAL_PORT (env: REDIS_PROXY_HOST / REDIS_PROXY_PORT)
- Aceita clientes Redis (redis-py / ARQ / redis-cli)
- Encaminha bytes crus via WebSocket para um serviço WS upstream
  (ex: seu serviço em Render que faz WS <-> Redis TCP).
- NÃO parseia RESP, NÃO converte comando, NÃO mexe em nada.

Além disso:
- Só abre WebSocket para o upstream DEPOIS de receber algum byte do cliente.
  Isso evita abrir WS para scanners de porta do Render que conectam
  e fecham sem mandar nada.
- Logs de conexão/desconexão estão em nível DEBUG; por padrão o LOG é WARNING.
"""

import os
import asyncio
import logging
from typing import Optional

from websockets import connect

# ----------------- LOGGING -----------------
LOG_LEVEL = os.getenv("REDIS_PROXY_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
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
    log.debug(f"[TCP] Cliente conectado: {peer}")

    # 1) Primeiro, espera receber ALGUM dado do cliente.
    #    Se o cliente conectar e fechar sem mandar nada (scanner de porta),
    #    a gente NÃO abre WebSocket pro upstream.
    try:
        first_chunk = await reader.read(4096)
    except Exception as e:
        log.debug(f"[TCP] erro ao ler primeiro chunk de {peer}: {e}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return

    if not first_chunk:
        # Cliente conectou e fechou sem mandar nada -> provavelmente scanner.
        log.debug(f"[TCP] {peer} fechou sem enviar dados (ignorando, sem WS).")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return

    # 2) Agora sim, abre WebSocket pro upstream.
    ws_url = f"{WS_URL}?token={TOKEN}" if TOKEN else WS_URL
    try:
        ws = await connect(ws_url)
    except Exception as e:
        log.error(f"[WS] Falha ao conectar ao WS upstream {ws_url}: {e}")
        # responde erro estilo Redis pro cliente
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

    log.debug(f"[WS] Conectado ao upstream: {ws_url}")

    async def tcp_to_ws():
        # já temos o primeiro chunk, envia ele antes
        try:
            data = first_chunk
            while data:
                await ws.send(data)
                data = await reader.read(4096)
            log.debug(f"[TCP] EOF do cliente {peer}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug(f"[tcp_to_ws] erro {peer}: {e}")
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
                if isinstance(msg, str):
                    data = msg.encode("utf-8", errors="ignore")
                else:
                    data = bytes(msg)
                writer.write(data)
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug(f"[ws_to_tcp] erro {peer}: {e}")
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

    log.debug(f"[SESSION] encerrada para {peer}")


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
