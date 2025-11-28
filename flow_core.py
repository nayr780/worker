# flow_core.py
import asyncio
import logging
import sys
import importlib
from dataclasses import dataclass
from typing import Any, Optional, Awaitable, Callable

from arq.worker import Retry

from playwright.async_api import TimeoutError as PWTimeoutError  # type: ignore
try:
    from playwright._impl._errors import TargetClosedError  # type: ignore
except Exception:  # pragma: no cover
    class TargetClosedError(Exception):
        """Fallback se a API interna do Playwright mudar."""
        pass


logger = logging.getLogger("iss-flows")


# ============================================================
# Resolver módulo "app/main" (onde está o worker)
# ============================================================

def get_app_module():
    """
    Resolve dinamicamente o módulo principal (main.py/app.py).

    É daqui que pegamos:
      - browser_get(), browser_close(), resilient_goto()
      - somente_digitos, normalizar_nome_colaborador, etc
      - append_file_log, log_colab
      - current_state
    """
    app = sys.modules.get("app") or sys.modules.get("main") or sys.modules.get("__main__")
    if app:
        return app

    for name in ("app", "main"):
        try:
            return importlib.import_module(name)
        except Exception:
            pass

    raise RuntimeError(
        "Não foi possível localizar o módulo principal (main.py/app.py). "
        "Execute via main.py / arq main.WorkerSettings"
    )


# ============================================================
# Contexto padrão de flow + erro de domínio
# ============================================================

@dataclass
class FlowContext:
    """
    Contexto padronizado para qualquer flow (notas, escrituração, dam, certidão, etc).
    """
    flow: str          # ex: "notas"
    colab: str         # colaborador_norm
    cnpj: str
    mes: str
    job_id: Optional[str] = None
    job_try: Optional[int] = None
    step: str = ""     # preenchido dinamicamente


class FlowError(Exception):
    """
    Erro de domínio padronizado em flows (CNPJ inexistente, mismatch, etc).
    Você pode criar subclasses em cada flow se quiser granularidade.
    """
    def __init__(self, code: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


# Você pode criar subclasses aqui ou em cada flow, por exemplo:
class LoginError(FlowError):
    def __init__(self, msg: str):
        super().__init__("LOGIN_ERROR", msg, retryable=False)


class CnpjInexistenteError(FlowError):
    def __init__(self, cnpj: str):
        super().__init__("CNPJ_INEXISTENTE", f"CNPJ não encontrado: {cnpj}", retryable=False)


class CnpjMismatchError(FlowError):
    def __init__(self, esperado: str, encontrado: str):
        super().__init__(
            "CNPJ_MISMATCH",
            f"CNPJ esperado={esperado}, retornado={encontrado}",
            retryable=False,
        )


# ============================================================
# Helpers de log (1 lugar só)
# ============================================================

def _get_log_funcs():
    app = get_app_module()
    log_colab = getattr(app, "log_colab", None)
    append_file_log = getattr(app, "append_file_log", None)
    return log_colab, append_file_log


def _format_prefix(ctx: FlowContext, event: str, code: str, extra: dict[str, Any]) -> str:
    parts = [
        f"[{event}]",
        f"flow={ctx.flow}",
        f"colab={ctx.colab}",
        f"cnpj={ctx.cnpj}",
        f"mes={ctx.mes}",
    ]
    if ctx.job_id:
        parts.append(f"job={ctx.job_id}")
    if ctx.job_try is not None:
        parts.append(f"try={ctx.job_try}")
    if ctx.step:
        parts.append(f"step={ctx.step}")
    if code:
        parts.append(f"code={code}")
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    return " ".join(parts)


async def log_flow(
    ctx: FlowContext,
    msg: str,
    *,
    level: int = logging.INFO,
    event: str = "EVENT",
    code: str = "",
    **extra,
) -> None:
    """
    Log estruturado padrão para todos os flows.
    Vai para:
      - logger global
      - log_colab (se existir)
      - arquivo do colaborador (append_file_log)
    """
    prefix = _format_prefix(ctx, event, code, extra)
    line = f"{prefix} :: {msg}"

    log_colab, append_file_log = _get_log_funcs()

    # logger global
    logger.log(level, line)

    # log por colaborador (Redis + arquivo)
    if log_colab:
        try:
            log_colab(ctx.colab, level, line)
        except Exception:
            logger.log(level, line)

    if append_file_log:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(append_file_log(ctx.colab, line))
        except RuntimeError:
            # sem loop rodando, ignora
            pass


def log_flow_sync(
    ctx: FlowContext,
    msg: str,
    *,
    level: int = logging.INFO,
    event: str = "EVENT",
    code: str = "",
    **extra,
) -> None:
    """
    Versão síncrona (para ser usada em callbacks tipo notificar=... em funções que não são async).
    Só faz create_task; não bloqueia o flow.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    loop.create_task(log_flow(ctx, msg, level=level, event=event, code=code, **extra))


# ============================================================
# Browser context resiliente (compartilhado entre flows)
# ============================================================

async def create_browser_context(browser_get_func: Callable[[], Awaitable[Any]]):
    """
    Cria um context do Playwright com fallback automático caso o browser esteja morto.
    É o mesmo conceito que você já tinha em flow_notas, só centralizado.
    """
    app = get_app_module()
    last_err: Optional[BaseException] = None

    for attempt in range(1, 3):  # tenta 2 vezes (normal + restart)
        try:
            browser = await browser_get_func()
            ctx = await browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 720},
            )
            return ctx
        except TargetClosedError as e:
            last_err = e
            logger.warning(
                "[browser] Browser.new_context falhou (TargetClosedError). "
                f"Tentativa {attempt}. Reiniciando browser..."
            )
            await getattr(app, "browser_close")()
        except Exception as e:
            last_err = e
            logger.warning(
                "[browser] Erro inesperado ao criar context. "
                f"Tentativa {attempt}. Reiniciando browser..."
            )
            await getattr(app, "browser_close")()

    raise last_err or RuntimeError("Falha ao criar browser context")


# ============================================================
# Runner padrão de step (timeout + classificação de erro)
# ============================================================

def _mark_step_in_state(ctx: FlowContext) -> None:
    """
    Atualiza current_state em main.py (opcional, só telemetria).
    """
    try:
        app = get_app_module()
        state = getattr(app, "current_state", None)
        if isinstance(state, dict):
            state["fase"] = ctx.step
            state["cnpj_aberto"] = ctx.cnpj
    except Exception:
        pass


async def run_step(
    ctx: FlowContext,
    name: str,
    coro: Awaitable[Any],
    *,
    per_step_timeout: int,
) -> Any:
    """
    Padrão de execução de uma etapa de flow:

      - Atualiza ctx.step + current_state["fase"].
      - Loga passo (STEP).
      - Aplica timeout por etapa (per_step_timeout).
      - Classifica erros:
          * TargetClosedError / "Target crashed" / timeout (asyncio + PWTimeout)
                → fecha browser + Retry(defer=5)  (erro recuperável)
          * Demais → propaga (vira falha do job / FlowError, etc.).

    Esse é o coração da padronização que você queria.
    """
    ctx.step = name
    _mark_step_in_state(ctx)
    await log_flow(ctx, f"Step: {name}", event="STEP")

    app = get_app_module()

    try:
        return await asyncio.wait_for(coro, timeout=per_step_timeout)

    except TargetClosedError as e:
        await log_flow(
            ctx,
            f"Browser TargetClosedError em '{name}': {e}",
            level=logging.WARNING,
            event="ERROR",
            code="BROWSER_TARGET_CLOSED",
        )
        try:
            await getattr(app, "browser_close")()
        except Exception:
            pass
        # erro recuperável → re-enfileirar rápido
        raise Retry(defer=5)

    except (PWTimeoutError, asyncio.TimeoutError) as e:
        await log_flow(
            ctx,
            f"Timeout na etapa '{name}': {e}",
            level=logging.WARNING,
            event="ERROR",
            code="STEP_TIMEOUT",
        )
        try:
            await getattr(app, "browser_close")()
        except Exception:
            pass
        raise Retry(defer=5)

    except Exception as e:
        text = str(e)
        if "Target crashed" in text:
            await log_flow(
                ctx,
                f"Target crashed em '{name}': {e} — reagendando em 5s",
                level=logging.WARNING,
                event="RETRY",
                code="BROWSER_TARGET_CRASHED",
                retry_in="5s",
            )
            try:
                await getattr(app, "browser_close")()
            except Exception:
                pass
            raise Retry(defer=5)

        # outros erros "de verdade"
        raise

