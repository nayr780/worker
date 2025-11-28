#!/usr/bin/env python3
# main.py — Worker ARQ + helpers comuns (Playwright, logging, flows)
#
# Roda o worker com:
#   arq main.WorkerSettings
#
# Outro código seu enfileira jobs com algo assim:
#
#   from arq import create_pool
#   from arq.connections import RedisSettings
#   from main import REDIS_SETTINGS, pick_queue_for_colab
#
#   async def enqueue_exemplo():
#       redis = await create_pool(REDIS_SETTINGS)
#       colaborador = "COLABORADOR_X"
#       queue_name = pick_queue_for_colab(colaborador)
#       job = await redis.enqueue_job(
#           "job_notas",
#           colaborador,
#           "12345678000199",
#           "01/2025",
#           "usuario",
#           "senha",
#           "convencional",
#           _queue_name=queue_name,
#       )
#       print("job_id:", job.job_id)

import os
import re
import sys
import asyncio
import logging
import subprocess
import uuid
from collections import deque, defaultdict
from pathlib import Path
from typing import Any, Optional

from playwright.async_api import async_playwright, Browser, TimeoutError as PWTimeoutError  # type: ignore

# TargetClosedError é interno do Playwright; usamos fallback pra evitar quebrar em updates
try:
    from playwright._impl._errors import TargetClosedError  # type: ignore
except Exception:  # pragma: no cover
    class TargetClosedError(Exception):
        """Fallback para caso a API interna do Playwright mude."""
        pass

from arq.worker import Worker
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

# Coisas gerais do ambiente/Playwright (mantive via env porque não tem a ver com ARQ)
HEADLESS = os.getenv("HEADLESS", "true").strip().lower() in ("true", "1", "yes")
BASE_DIR = os.getenv("BASE_DIR", "./saidas")
IS_RENDER = bool(os.getenv("RENDER_SERVICE_ID") or os.getenv("RENDER"))

NAV_TIMEOUT_MS = int(float(os.getenv("NAV_TIMEOUT_SEC", "60")) * 1000)
DEF_TIMEOUT_MS = int(float(os.getenv("DEF_TIMEOUT_SEC", "30")) * 1000)
PER_STEP_TIMEOUT_SEC = int(os.getenv("PER_STEP_TIMEOUT_SEC", "300"))
MAX_TASK_TIMEOUT_SEC = int(os.getenv("MAX_TASK_TIMEOUT_SEC", "800"))

# Namespace de logs no Redis (por job)
REDIS_LOG_NS = os.getenv("REDIS_LOG_NS", "iss")

# Estado compartilhado para os flows (apenas informativo/telemetria)
current_state: dict[str, Any] = {
    "fase": "",
    "cnpj_aberto": "",
    "escrituracao_reaberta": False,
    "passou_encerramento": False,
}


def somente_digitos(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def limpar_nome(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", s or "").strip()


def normalizar_nome_colaborador(nome: str) -> str:
    import re
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

    Vantagens:
      - Nenhum job some.
      - Você pode subir 1, 15, 433 workers, todos iguais, nenhum precisa de env especial.
      - ARQ distribui os jobs entre os workers automaticamente.

    Se um dia você quiser voltar a shardar, aqui é o ponto único de mudança.
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
    Log nomeado por colaborador, SEM reenviar para o logger global
    (pra não duplicar linhas quando o flow já chamou logger.log).

    A ideia é:
      - flow_core.log_flow() já faz logger.log(... line ...)
      - aqui só guardamos num buffer por colaborador, se alguém quiser ler depois.
    """
    try:
        memory_log_handler.buffers[colab].append(msg)
    except Exception:
        # Fallback bem conservador: se der algum problema, loga pelo logger normal.
        logger.log(level, msg, extra={"colab": colab})


def ensure_log_file(colab_norm: str) -> Path:
    p = Path(BASE_DIR) / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{colab_norm}.log"


async def append_file_log(colab_norm: str, text: str):
    """
    Log em arquivo por colaborador (mantido para compatibilidade).
    """
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

    Ele é anexado/removido por job via on_job_start/on_job_end.
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
                # Nunca deixa erro de log derrubar o worker.
                pass

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Sem event loop rodando (por exemplo durante shutdown) → ignora.
            return

        try:
            loop.create_task(_push())
        except Exception:
            # Falha ao agendar task → ignora.
            pass


# ============================================================
# Integração com pCloud (upload de diretórios)
# ============================================================

_PCLOUD_CLIENT: Optional["PyCloud"] = None  # type: ignore[name-defined]


def get_pcloud_client() -> Optional["PyCloud"]:  # type: ignore[name-defined]
    """
    Retorna um cliente PyCloud inicializado, ou None se o pCloud estiver desabilitado
    ou mal configurado.

    Usa as env vars:
      - PCLOUD_ENABLED (true/false)
      - PCLOUD_USERNAME
      - PCLOUD_PASSWORD
      - PCLOUD_ENDPOINT (opcional, default "nearest")
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
        from pcloud import PyCloud  # import local para evitar erro se o pacote não estiver instalado
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

    remote_subpath: caminho relativo dentro de PCLOUD_BASE_PATH, ex:
      'notas/colab_x/01-2025/12345678000199'

    Usa as env vars:
      - PCLOUD_BASE_PATH (default: "/iss-automacao")

    Retorna o caminho remoto final em caso de sucesso, ou None se pCloud estiver desabilitado
    ou se o diretório não existir.
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
        # Garante a pasta raiz no pCloud
        pc.createfolderifnotexists(path=remote_root)

        # Caminha pela árvore local e cria as mesmas subpastas no pCloud
        for root, dirs, files in os.walk(local_dir):
            rel = os.path.relpath(root, local_dir)
            # "./" => raiz
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

    # roda o upload bloqueante em thread separada pra não travar o event loop
    return await asyncio.to_thread(_do_upload)


# ============================================================
# Export para flows
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
    "upload_dir_to_pcloud",  # <- novo export
]


# ============================================================
# Redis / ARQ Worker
# ============================================================

REDIS_SETTINGS = RedisSettings(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", "5000")),   # mesmo proxy/porta que você já usa
    database=int(os.getenv("REDIS_DB", "0")),
    ssl=False,
)


async def arq_startup(ctx: dict):
    """
    Roda uma vez quando o worker ARQ sobe.
    Prepara Chromium, browser singleton e pool Redis para logs.
    """
    logger.info(f"[worker] iniciando com id={WORKER_ID}")

    if IS_RENDER:
        logger.info("[ENV] Executando no Render — garantindo Chromium")
        ensure_chromium()
    else:
        logger.info("[ENV] Executando localmente")

    os.makedirs(BASE_DIR, exist_ok=True)
    await browser_get()

    # pool para logs e para uso geral, se quiser
    ctx["log_redis"] = await create_pool(REDIS_SETTINGS)

    ctx["base_dir"] = BASE_DIR
    ctx["is_render"] = IS_RENDER
    ctx["worker_id"] = WORKER_ID
    logger.info("[arq] startup concluído")


async def arq_shutdown(ctx: dict):
    """
    Roda quando o worker está desligando.
    """
    try:
        redis = ctx.get("log_redis")
        if redis:
            await redis.aclose()
    except Exception:
        pass

    await browser_close()
    logger.info("[arq] shutdown concluído")


async def arq_on_job_start(ctx: dict):
    """
    Hook chamado no início de CADA job.

    Aqui anexamos um handler de log que manda tudo para Redis na chave:
        {REDIS_LOG_NS}:task:{job_id}:logs
    """
    redis = ctx.get("log_redis")
    job_id = ctx.get("job_id")  # arq injeta job_id no ctx do job
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
    Hook chamado no final de CADA job (com sucesso ou erro).
    Remove o handler de log do Redis.
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
except ImportError:
    logger.warning("flow_notas.job_notas não encontrado; notas não serão processadas por este worker.")

try:
    from flow_escrituracao import job_escrituracao
    ARQ_FUNCTIONS.append(job_escrituracao)
except ImportError:
    logger.warning("flow_escrituracao.job_escrituracao não encontrado; escrituração não será processada.")

try:
    from flow_certidao import job_certidao
    ARQ_FUNCTIONS.append(job_certidao)
except ImportError:
    logger.warning("flow_certidao.job_certidao não encontrado; certidões não serão processadas.")

try:
    from flow_dam import job_dam
    ARQ_FUNCTIONS.append(job_dam)
except ImportError:
    logger.warning("flow_dam.job_dam não encontrado; DAM não será processado.")


# ============================================================
# WorkerSettings do ARQ
# ============================================================

class WorkerSettings:
    # IMPORTANTE: precisa ser SEMPRE uma lista
    functions = ARQ_FUNCTIONS or []
    redis_settings = REDIS_SETTINGS

    on_startup = arq_startup
    on_shutdown = arq_shutdown
    on_job_start = arq_on_job_start
    on_job_end = arq_on_job_end

    # Uma única fila global — todo mundo ouve "arq:queue"
    queue_name = "arq:queue"

    # Concurrency / retry / timeout
    # max_jobs = 1 -> cada worker processa só UM job por vez
    # Se quiser mais paralelismo, você sobe MAIS processos workers.
    max_jobs = 1
    max_tries = 3
    retry_jobs = True

    # job_timeout grande (global), corte fino é feito por etapa no flow (PER_STEP_TIMEOUT_SEC)
    job_timeout = MAX_TASK_TIMEOUT_SEC

    # Não ficar segurando job em shutdown
    job_completion_wait = 0

    # Health check bem frequente pra detectar worker morto
    health_check_interval = 10

    # 🔥 Health-check com chave única por worker
    # (ex.: arq:health:7f3a2c, arq:health:b91e00, etc.)
    health_check_key = f"arq:health:{WORKER_ID}"

    # health check, logging, etc.
    log_results = True
    allow_abort_jobs = True


# ============================================================
# Helper para STOP de jobs em ESPERA
# ============================================================

async def stop_job(job_id: str) -> bool:
    """
    Aborta um job do ARQ se ele ainda estiver em espera (queued/deferred).

    - Jobs já em execução NÃO são abortados (retorna False).
    - Requer allow_abort_jobs = True no WorkerSettings.
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
# Entry point opcional (rodar sem CLI do arq)
# ============================================================

async def _main_async():
    """
    Modo alternativo para desenvolvimento:
        python main.py
    (equivalente a: arq main.WorkerSettings)
    """
    worker = Worker(WorkerSettings)
    await worker.async_run()


if __name__ == "__main__":
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        logger.info("Encerrando por KeyboardInterrupt...")
        try:
            asyncio.run(browser_close())
        except RuntimeError:
            # event loop já fechado
            pass
