#!/usr/bin/env python3
"""
main.py — Worker ARQ + helpers comuns (Playwright, logging, flows, proxy Redis WS)

DEV (local):
    python main.py
    - Sobe o proxy redis_ws_tcp_proxy
    - Roda `arq main.WorkerSettings` em subprocess
    - Tudo em um terminal só

RENDER (produção, Web Service):
    Start Command:
        python main.py

    - python main.py:
        - sobe proxy redis_ws_tcp_proxy
        - sobe HTTP de health em 0.0.0.0:$PORT (rota "/")
        - sobe subprocess `arq main.WorkerSettings`
"""

import os
import re
import sys
import asyncio
import logging
import subprocess
import uuid
import socket
from collections import deque, defaultdict
from pathlib import Path
from typing import Any, Optional

# Proxy TCP -> WS Redis (arquivo ao lado)
import redis_ws_tcp_proxy

# Playwright
from playwright.async_api import async_playwright, Browser, TimeoutError as PWTimeoutError  # type: ignore

# TargetClosedError é interno do Playwright; usamos fallback pra evitar quebrar em updates
try:
    from playwright._impl._errors import TargetClosedError  # type: ignore
except Exception:  # pragma: no cover
    class TargetClosedError(Exception):
        """Fallback para caso a API interna do Playwright mude."""
        pass

from arq.connections import RedisSettings, create_pool
from arq.jobs import Job, JobStatus

# ============================================================
# Config & Logging
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("iss-automacao-notas")

# ID do worker (para health check) — pode ser fixado via env se quiser
WORKER_ID = os.getenv("WORKER_ID") or uuid.uuid4().hex[:6]

# Coisas gerais do ambiente/Playwright
HEADLESS = os.getenv("HEADLESS", "true").strip().lower() in ("true", "1", "yes")
BASE_DIR = os.getenv("BASE_DIR", "./saidas")
IS_RENDER = bool(os.getenv("RENDER_SERVICE_ID") or os.getenv("RENDER"))

NAV_TIMEOUT_MS = int(float(os.getenv("NAV_TIMEOUT_SEC", "60")) * 1000)
DEF_TIMEOUT_MS = int(float(os.getenv("DEF_TIMEOUT_SEC", "30")) * 1000)
PER_STEP_TIMEOUT_SEC = int(os.getenv("PER_STEP_TIMEOUT_SEC", "300"))
MAX_TASK_TIMEOUT_SEC = int(os.getenv("MAX_TASK_TIMEOUT_SEC", "800"))

# Namespace de logs no Redis (por job)
REDIS_LOG_NS = os.getenv("REDIS_LOG_NS", "iss")

# Porta HTTP para o health-check (Render usa $PORT)
STATUS_HOST = "0.0.0.0"
STATUS_PORT = int(os.getenv("PORT", "10000"))  # default 10000 se PORT não vier

# Estado compartilhado para os flows (apenas informativo/telemetria)
current_state: dict[str, Any] = {
    "fase": "",
    "cnpj_aberto": "",
    "escrituracao_reaberta": False,
    "passou_encerramento": False,
}

# ============================================================
# Helpers simples
# ============================================================

def somente_digitos(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def limpar_nome(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", s or "").strip()


def normalizar_nome_colaborador(nome: str) -> str:
    import unicodedata

    s = (nome or "").strip().lower()
    # remove acentos (NFKD) e deixa só ASCII
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    # permite apenas a-z e dígitos, transforma outros em underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "colaborador"


def pick_queue_for_colab(colaborador: str) -> str:
    """
    Por enquanto: TODO MUNDO na mesma fila "arq:queue".
    """
    return "arq:queue"


# ============================================================
# Navegação resiliente
# ============================================================

async def resilient_goto(page, url: str, retries: int = 4, timeout: int = 120000, wait_until: str = "load"):
    """
    Navegação resiliente: tenta algumas vezes, respeitando HTTP 5xx e timeout.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[goto] {attempt}/{retries} → {url}")
            resp = await page.goto(url, timeout=timeout, wait_until=wait_until)
            status = getattr(resp, "status", None)
            if status and status >= 500 and attempt < retries:
                logger.warning(f"[goto] HTTP {status}, retrying...")
                await asyncio.sleep(2 * attempt)
                continue
            return resp
        except PWTimeoutError:
            logger.warning(f"[goto] Timeout tentativa {attempt}/{retries}")
            if attempt == retries:
                raise
            await asyncio.sleep(2 * attempt)
        except Exception as e:
            logger.warning(f"[goto] Erro {e} na tentativa {attempt}/{retries}")
            if attempt == retries:
                raise
            await asyncio.sleep(2 * attempt)


# ============================================================
# Playwright singleton
# ============================================================

_playwright = None
_browser: Optional[Browser] = None


async def browser_close():
    global _playwright, _browser
    try:
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            logger.info("[browser] closed")
    except Exception:
        pass
    try:
        if _playwright:
            try:
                await _playwright.stop()
            except Exception:
                pass
            logger.info("[browser] playwright stopped")
    except Exception:
        pass
    _browser = None
    _playwright = None


def ensure_chromium():
    """
    Garante que o Chromium do Playwright está instalado (modo Render/local).
    """
    cache = os.getenv("PLAYWRIGHT_BROWSERS_PATH") or str(Path.home() / ".cache" / "ms-playwright")
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = cache
    version_dir = next(Path(cache).glob("chromium_*"), None)
    if version_dir:
        exe = version_dir / ("chrome-win" if sys.platform.startswith("win") else "chrome-linux") / \
              ("headless_shell.exe" if sys.platform.startswith("win") else "headless_shell")
        if exe.exists():
            logger.info(f"[✓] Chromium já encontrado em {exe}")
            return
    logger.info(f"[i] Chromium não encontrado em {cache}. Instalando...")
    for cmd in (
        [sys.executable, "-m", "playwright", "install", "chromium", "--only-shell"],
        [sys.executable, "-m", "playwright", "install", "chromium"],
    ):
        try:
            subprocess.check_call(cmd)
            logger.info("[✓] Chromium instalado com sucesso.")
            return
        except subprocess.CalledProcessError:
            continue
    logger.error("[✗] Falha ao instalar Chromium.")
    sys.exit(1)


async def _launch_browser() -> Browser:
    global _playwright, _browser
    try:
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"],
        )
        logger.info("[browser] launched")
        return _browser
    except Exception:
        logger.exception("[browser] launch failed, exiting")
        os._exit(1)


async def browser_get() -> Browser:
    """
    Singleton de Browser para todos os jobs (dentro do mesmo processo).
    """
    global _browser
    if _browser is None:
        return await _launch_browser()
    try:
        if not _browser.is_connected():
            await browser_close()
            return await _launch_browser()
    except Exception:
        await browser_close()
        return await _launch_browser()
    return _browser


# ============================================================
# Logs por colaborador (memória + arquivo)
# ============================================================

class MemoryLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.buffers = defaultdict(lambda: deque(maxlen=5000))

    def emit(self, record):
        msg = self.format(record)
        col = getattr(record, "colab", None)
        if col:
            self.buffers[col].append(msg)


memory_log_handler = MemoryLogHandler()
memory_log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(memory_log_handler)


def log_colab(colab: str, level: int, msg: str):
    """
    Log nomeado por colaborador, sem reenviar para o logger global.
    """
    try:
        memory_log_handler.buffers[colab].append(msg)
    except Exception:
        logger.log(level, msg, extra={"colab": colab})


def ensure_log_file(colab_norm: str) -> Path:
    p = Path(BASE_DIR) / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{colab_norm}.log"


async def append_file_log(colab_norm: str, text: str):
    path = ensure_log_file(colab_norm)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")


def read_full_log(colab_norm: str) -> str:
    path = ensure_log_file(colab_norm)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


# ============================================================
# Handler de log por JOB no Redis
# ============================================================

class RedisJobLogHandler(logging.Handler):
    """
    Handler que envia TODOS os logs do processo para uma lista Redis
    específica de um job do ARQ:  {NS}:task:{job_id}:logs
    """
    def __init__(self, redis_pool, job_id: str, limit: int = 5000):
        super().__init__()
        self.redis = redis_pool
        self.job_id = job_id
        self.limit = limit
        self.key = f"{REDIS_LOG_NS}:task:{job_id}:logs"

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)

        async def _push():
            try:
                await self.redis.rpush(self.key, msg)
                await self.redis.ltrim(self.key, -self.limit, -1)
            except Exception:
                pass

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        try:
            loop.create_task(_push())
        except Exception:
            pass


# ============================================================
# Integração com pCloud (upload de diretórios)
# ============================================================

_PCLOUD_CLIENT: Optional["PyCloud"] = None  # type: ignore[name-defined]


def get_pcloud_client() -> Optional["PyCloud"]:  # type: ignore[name-defined]
    """
    Retorna um cliente PyCloud inicializado, ou None se o pCloud estiver desabilitado
    ou mal configurado.
    """
    enabled = os.getenv("PCLOUD_ENABLED", "").strip().lower() in ("1", "true", "yes")
    if not enabled:
        logger.info("[pcloud] Desabilitado (PCLOUD_ENABLED não está ativo).")
        return None

    global _PCLOUD_CLIENT
    if _PCLOUD_CLIENT is not None:
        return _PCLOUD_CLIENT

    user = os.getenv("PCLOUD_USERNAME")
    password = os.getenv("PCLOUD_PASSWORD")
    endpoint = os.getenv("PCLOUD_ENDPOINT", "nearest")

    if not user or not password:
        logger.warning("[pcloud] PCLOUD_USERNAME/PCLOUD_PASSWORD não configurados, upload desativado.")
        return None

    try:
        from pcloud import PyCloud  # type: ignore
        _PCLOUD_CLIENT = PyCloud(user, password, endpoint=endpoint)
        logger.info("[pcloud] Cliente inicializado com sucesso.")
        return _PCLOUD_CLIENT
    except Exception:
        logger.exception("[pcloud] Falha ao inicializar cliente PyCloud.")
        _PCLOUD_CLIENT = None
        return None


async def upload_dir_to_pcloud(local_dir: str, remote_subpath: str) -> Optional[str]:
    """
    Sobe TODO o conteúdo de local_dir para pCloud, preservando subpastas.
    """
    pc = get_pcloud_client()
    if pc is None:
        logger.info("[pcloud] upload_dir_to_pcloud ignorado (sem cliente ativo).")
        return None

    if not os.path.isdir(local_dir):
        logger.warning(f"[pcloud] Diretório local não existe: {local_dir}")
        return None

    base_remote = os.getenv("PCLOUD_BASE_PATH", "/iss-automacao")
    remote_root = f"{base_remote.rstrip('/')}/{remote_subpath.lstrip('/')}"

    def _do_upload():
        pc.createfolderifnotexists(path=remote_root)

        for root, dirs, files in os.walk(local_dir):
            rel = os.path.relpath(root, local_dir)
            if rel in (".", ""):
                remote_dir = remote_root
            else:
                remote_dir = f"{remote_root}/{rel.replace(os.sep, '/')}"

            pc.createfolderifnotexists(path=remote_dir)

            if not files:
                continue

            file_paths = [os.path.join(root, f) for f in files]
            logger.info(f"[pcloud] Enviando {len(file_paths)} arquivos para {remote_dir}")
            pc.uploadfile(files=file_paths, path=remote_dir)

        return remote_root

    return await asyncio.to_thread(_do_upload)


# ============================================================
# Export para flows (__all__)
# ============================================================

__all__ = [
    "BASE_DIR",
    "logger",
    "resilient_goto",
    "browser_get",
    "browser_close",
    "log_colab",
    "append_file_log",
    "normalizar_nome_colaborador",
    "somente_digitos",
    "NAV_TIMEOUT_MS",
    "DEF_TIMEOUT_MS",
    "PER_STEP_TIMEOUT_SEC",
    "current_state",
    "pick_queue_for_colab",
    "REDIS_SETTINGS",
    "stop_job",
    "WorkerSettings",
    "upload_dir_to_pcloud",
]


# ============================================================
# Redis / Proxy settings
# ============================================================

# Defaults para o proxy falar com o Redis upstream (WS)
REDIS_WS_URL = os.getenv(
    "REDIS_WS_URL",
    getattr(redis_ws_tcp_proxy, "WS_URL", "wss://redisrender.onrender.com"),
)
REDIS_TOKEN = os.getenv(
    "REDIS_TOKEN",
    getattr(redis_ws_tcp_proxy, "TOKEN", ""),
)

# Onde o proxy TCP vai escutar (e onde o worker ARQ vai conectar)
PROXY_HOST = os.getenv(
    "REDIS_PROXY_HOST",
    getattr(redis_ws_tcp_proxy, "LOCAL_HOST", "127.0.0.1"),
)
PROXY_PORT = int(os.getenv(
    "REDIS_PROXY_PORT",
    getattr(redis_ws_tcp_proxy, "LOCAL_PORT", 6380),
))

# Atualiza o módulo do proxy com o que vier do env
redis_ws_tcp_proxy.WS_URL = REDIS_WS_URL
redis_ws_tcp_proxy.TOKEN = REDIS_TOKEN
redis_ws_tcp_proxy.LOCAL_HOST = PROXY_HOST
redis_ws_tcp_proxy.LOCAL_PORT = PROXY_PORT

# Onde o worker ARQ enxerga o Redis (sempre via proxy TCP)
REDIS_SETTINGS = RedisSettings(
    host=os.getenv("REDIS_HOST", PROXY_HOST),
    port=int(os.getenv("REDIS_PORT", PROXY_PORT)),
    database=int(os.getenv("REDIS_DB", "0")),
    ssl=False,
)


# ============================================================
# ARQ hooks — proxy + status HTTP dentro do worker
# ============================================================

_proxy_task: Optional[asyncio.Task] = None
_status_task: Optional[asyncio.Task] = None


async def _wait_for_port(host: str, port: int, timeout: float = 10.0):
    """aguarda porta TCP ficar pronta (timeout em segundos)"""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            if loop.time() > deadline:
                raise TimeoutError(f"timeout waiting for {host}:{port}")
            await asyncio.sleep(0.15)


# ============================================================
# HTTP status server (health-check / Render)
# ============================================================

async def _status_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        # lê mas ignora o request
        await reader.read(1024)
    except Exception:
        pass

    body = b"OK"
    headers = [
        b"HTTP/1.1 200 OK",
        b"Content-Type: text/plain; charset=utf-8",
        f"Content-Length: {len(body)}".encode("ascii"),
        b"Connection: close",
        b"",
        b"",
    ]
    resp = b"\r\n".join(headers) + body
    try:
        writer.write(resp)
        await writer.drain()
    except Exception:
        pass
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def start_status_server():
    """
    Servidor HTTP ultra-simples de health-check em 0.0.0.0:$PORT.
    """
    if STATUS_PORT <= 0:
        logger.info("[status] PORT não definido, status HTTP desativado.")
        return

    server = await asyncio.start_server(_status_handler, STATUS_HOST, STATUS_PORT)
    addr = server.sockets[0].getsockname()
    logger.info(f"[status] Servindo health-check HTTP em http://{addr[0]}:{addr[1]}/")

    async with server:
        await server.serve_forever()


async def arq_startup(ctx: dict):
    """
    on_startup do ARQ (usado quando rodar `arq main.WorkerSettings` diretamente):
      - inicia o proxy redis_ws_tcp_proxy.main() como task
      - espera a porta do proxy abrir
      - cria pool Redis (ctx['log_redis'])
      - prepara Playwright
      - (opcional) inicia status HTTP se PORT estiver definido
    """
    global _proxy_task, _status_task
    logger.info(f"[worker] iniciando com id={WORKER_ID}")

    if IS_RENDER:
        logger.info("[ENV] Executando no Render — garantindo Chromium")
        ensure_chromium()
    else:
        logger.info("[ENV] Executando localmente")

    os.makedirs(BASE_DIR, exist_ok=True)
    await browser_get()

    # Proxy Redis (TCP->WS)
    if os.getenv("START_REDIS_PROXY", "1") == "1":
        if _proxy_task is None:
            logger.info("[proxy] iniciando proxy (on_startup)...")
            _proxy_task = asyncio.create_task(redis_ws_tcp_proxy.main())
        try:
            await _wait_for_port(REDIS_SETTINGS.host, REDIS_SETTINGS.port, timeout=15.0)
            logger.info(f"[proxy] porta {REDIS_SETTINGS.host}:{REDIS_SETTINGS.port} pronta.")
        except Exception as exc:
            logger.error(f"[proxy] falha ao esperar porta do proxy: {exc}")
            raise

    # Servidor HTTP de status (apenas se PORT definido)
    if STATUS_PORT > 0 and _status_task is None:
        try:
            _status_task = asyncio.create_task(start_status_server())
        except Exception as exc:
            logger.error(f"[status] falha ao iniciar servidor HTTP: {exc}")

    ctx["log_redis"] = await create_pool(REDIS_SETTINGS)
    ctx["base_dir"] = BASE_DIR
    ctx["is_render"] = IS_RENDER
    ctx["worker_id"] = WORKER_ID
    logger.info("[arq] startup concluído")


async def arq_shutdown(ctx: dict):
    """
    on_shutdown do ARQ
    """
    global _status_task

    try:
        redis = ctx.get("log_redis")
        if redis:
            await redis.aclose()
    except Exception:
        pass

    await browser_close()

    if _status_task:
        try:
            _status_task.cancel()
        except Exception:
            pass

    logger.info("[arq] shutdown concluído")


async def arq_on_job_start(ctx: dict):
    """
    Hook chamado no início de CADA job.
    """
    redis = ctx.get("log_redis")
    job_id = ctx.get("job_id")
    if not redis or not job_id:
        return

    handler = RedisJobLogHandler(redis, job_id)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    root = logging.getLogger()
    root.addHandler(handler)

    ctx["job_log_handler"] = handler
    logger.info(f"[job {job_id}] início")


async def arq_on_job_end(ctx: dict):
    """
    Hook chamado no final de CADA job.
    """
    job_id = ctx.get("job_id")
    handler = ctx.pop("job_log_handler", None)
    if handler:
        try:
            root = logging.getLogger()
            root.removeHandler(handler)
        except Exception:
            pass

    if job_id:
        logger.info(f"[job {job_id}] fim")


# ============================================================
# Importa flows e registra jobs
# ============================================================

ARQ_FUNCTIONS = []

try:
    from flow_notas import job_notas
    ARQ_FUNCTIONS.append(job_notas)
except Exception:
    logger.warning("flow_notas.job_notas não encontrado; notas não serão processadas por este worker.")

try:
    from flow_escrituracao import job_escrituracao
    ARQ_FUNCTIONS.append(job_escrituracao)
except Exception:
    logger.warning("flow_escrituracao.job_escrituracao não encontrado; escrituração não será processada.")

try:
    from flow_certidao import job_certidao
    ARQ_FUNCTIONS.append(job_certidao)
except Exception:
    logger.warning("flow_certidao.job_certidao não encontrado; certidões não serão processadas.")

try:
    from flow_dam import job_dam
    ARQ_FUNCTIONS.append(job_dam)
except Exception:
    logger.warning("flow_dam.job_dam não encontrado; DAM não será processado.")


# ============================================================
# WorkerSettings do ARQ
# ============================================================

class WorkerSettings:
    functions = ARQ_FUNCTIONS or []
    redis_settings = REDIS_SETTINGS

    on_startup = arq_startup
    on_shutdown = arq_shutdown
    on_job_start = arq_on_job_start
    on_job_end = arq_on_job_end

    queue_name = "arq:queue"

    max_jobs = 1
    max_tries = 3
    retry_jobs = True

    job_timeout = MAX_TASK_TIMEOUT_SEC
    job_completion_wait = 0

    health_check_interval = 10
    health_check_key = f"arq:health:{WORKER_ID}"

    log_results = True
    allow_abort_jobs = True


# ============================================================
# Helper para STOP de jobs em ESPERA
# ============================================================

async def stop_job(job_id: str) -> bool:
    """
    Aborta um job do ARQ se ele ainda estiver em espera (queued/deferred).
    """
    redis = await create_pool(REDIS_SETTINGS)
    try:
        job = Job(job_id=job_id, redis=redis)
        status = await job.status()
        if status in (JobStatus.queued, JobStatus.deferred):
            return await job.abort()
        return False
    finally:
        await redis.aclose()


# ============================================================
# Modo desenvolvimento: python main.py
# ============================================================

def run_arq_subprocess():
    """
    Inicia `arq main.WorkerSettings` em subprocess.
    Útil para desenvolvimento (python main.py).
    """
    # No subprocess, NÃO vamos subir proxy de novo
    env = os.environ.copy()
    env["START_REDIS_PROXY"] = "0"

    cmd = ["arq", "main.WorkerSettings"]
    logger.info(f"[dev] executando subprocess: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, env=env)
    try:
        proc.wait()
    except KeyboardInterrupt:
        logger.info("[dev] subprocess interrompido pelo usuário")
        try:
            proc.terminate()
        except Exception:
            pass
        raise
    return proc.returncode


async def _dev_main_async():
    """
    1) Inicia proxy em background (no mesmo processo)
    2) Inicia HTTP status server
    3) Aguarda proxy
    4) Roda subprocess `arq main.WorkerSettings`
    """
    logger.info("[dev] iniciando proxy (modo dev)...")

    # Proxy Redis local (TCP->WS)
    proxy_task = asyncio.create_task(redis_ws_tcp_proxy.main())

    # HTTP status (health) no processo principal
    status_task = None
    if STATUS_PORT > 0:
        try:
            status_task = asyncio.create_task(start_status_server())
        except Exception as exc:
            logger.error(f"[status] falha ao iniciar servidor HTTP (dev): {exc}")
    else:
        logger.info("[status] PORT não definido; status HTTP desativado (dev).")

    # Espera a porta do proxy abrir
    try:
        await _wait_for_port(REDIS_SETTINGS.host, REDIS_SETTINGS.port, timeout=15.0)
    except Exception as exc:
        logger.error(f"[dev] proxy não ficou pronto: {exc}")
        proxy_task.cancel()
        if status_task:
            status_task.cancel()
        raise

    # Agora roda o ARQ em subprocess (bloqueante, então vai pra thread)
    loop = asyncio.get_running_loop()
    rc = await loop.run_in_executor(None, run_arq_subprocess)

    # Quando o subprocess sai, cancelamos proxy e status e encerramos
    try:
        proxy_task.cancel()
    except Exception:
        pass

    if status_task:
        try:
            status_task.cancel()
        except Exception:
            pass

    return rc


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    # DEV (local) e também modo Web Service no Render:
    #   Start Command:  python main.py
    if os.getenv("FORCE_ARQ_SUBPROCESS", "") == "1":
        # atalho: roda só o arq em subprocess, sem proxy
        sys.exit(run_arq_subprocess())

    try:
        exit_code = asyncio.run(_dev_main_async())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Encerrando por KeyboardInterrupt...")
        try:
            asyncio.run(browser_close())
        except Exception:
            pass
    except Exception as exc:
        logger.exception(f"Erro no modo dev: {exc}")
        sys.exit(1)
