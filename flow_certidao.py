#!/usr/bin/env python3
# flow_certidao.py — Flow de Certidão usando flow_core

import os
import sys
import re
import html
import asyncio
import logging
from datetime import datetime
from typing import Optional

from flow_core import (
    FlowContext,
    FlowError,
    create_browser_context,
    run_step,
    log_flow,
    get_app_module,
)

logger = logging.getLogger("iss-automacao-certidao")


# ===================== Helpers =====================

def _safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s or "")


class PopupServerError(Exception):
    """Erro indicando que o popup carregou uma página de erro (500 / Internal Server Error)."""

    pass


# ===================== Espera renderização com robustez =====================


async def esperar_tela_certidao_renderizada(
    page,
    total_timeout_sec: float = 30.0,
    extra_wait_controls: float = 8.0,
    poll_interval: float = 0.15,
    base_dir: Optional[str] = None,
    debug_prefix: str = "popup_parcial",
):
    """
    Espera a renderização COMPLETA da tela (com heurísticas):
      - document.readyState pode ser 'complete' (preferível), mas às vezes não reflete JS dinâmico.
      - considera 'estabilizada' quando: body#pagina-inicial presente + loader (#gifAguarde) oculto + form[id^='pmfInclude:pesquisaForm'] presente.
      - depois de estabilizada espera extra_wait_controls segundos (poll) pela presença dos controls:
          select[id$=':tipoCertidao'] e botões Recuperar / Pesquisar.
      - Se detectar "Internal Server Error" no texto do body, levanta PopupServerError imediatamente.
      - Se timeout, lança TimeoutError (sem salvar HTML em disco).
    Retorna None em sucesso (página considerada renderizada).
    """
    logger.info(
        f"[certidao] aguardando renderização completa da página (até {total_timeout_sec:.0f}s)"
    )
    deadline = asyncio.get_event_loop().time() + total_timeout_sec

    async def _eval_safe(js, *args):
        try:
            return await page.evaluate(js, *args)
        except Exception:
            return None

    async def _doc_ready():
        val = await _eval_safe("() => document.readyState")
        return val == "complete"

    async def _body_present():
        try:
            el = await page.query_selector("body#pagina-inicial")
            return el is not None
        except Exception:
            return False

    async def _loader_hidden():
        try:
            el = await page.query_selector("#gifAguarde")
            if not el:
                return True
            # check style attr and is_hidden
            style = (await el.get_attribute("style") or "").replace(" ", "").lower()
            hidden_by_style = "display:none" in style or "display:none;" in style
            try:
                is_hidden = await el.is_hidden()
            except Exception:
                is_hidden = False
            return hidden_by_style or is_hidden
        except Exception:
            return True  # se não achar, não é bloqueante

    async def _form_present():
        try:
            el = await page.query_selector("form[id^='pmfInclude:pesquisaForm']")
            return el is not None
        except Exception:
            return False

    async def _select_present():
        try:
            sel = await page.query_selector("select[id$=':tipoCertidao']")
            if not sel:
                return False
            # verifica opções
            opts = await _eval_safe("(sel) => sel.options ? sel.options.length : 0", sel)
            opts_cnt = int(opts or 0)
            return opts_cnt >= 1
        except Exception:
            return False

    async def _buttons_present():
        try:
            rec = await page.query_selector("input[id$=':btnRecuperar']")
            pesq = await page.query_selector("input[id$=':btnPesquisar']")
            return (rec is not None) and (pesq is not None)
        except Exception:
            return False

    async def _detect_internal_server_error():
        """
        Checa se o corpo contém pistas de 'Internal Server Error' / 500.
        """
        try:
            body_text = await page.evaluate(
                "() => document.body ? document.body.innerText || '' : ''"
            )
            if not body_text:
                return False
            txt = body_text.lower()
            if (
                "internal server error" in txt
                or "the server encountered an internal error" in txt
                or "erro interno" in txt
                or "<h1>internal server error</h1>" in txt
            ):
                return True
            return False
        except Exception:
            return False

    last_log_state = None

    # fase 1: aguarda estabilização (body + loader escondido + form)
    while asyncio.get_event_loop().time() < deadline:
        # detecta erro 500 rapidamente
        if await _detect_internal_server_error():
            logger.warning("[certidao] detectado Internal Server Error na página do popup.")
            raise PopupServerError(
                "Internal Server Error detectado na página do popup."
            )

        ready = await _doc_ready()
        body_ok = await _body_present()
        loader_ok = await _loader_hidden()
        form_ok = await _form_present()

        state = (
            f"ready={ready}, body={body_ok}, loader_hidden={loader_ok}, form={form_ok}"
        )
        if state != last_log_state:
            logger.debug(f"[certidao] checando estabilização... {state}")
            last_log_state = state

        # condição de estabilização: body + loader hidden + form present
        if body_ok and loader_ok and form_ok:
            logger.info(
                "[certidao] página estabilizada (body + loader oculto + form presente). "
                "aguardando controles (select/botões)."
            )
            break

        await asyncio.sleep(poll_interval)

    else:
        # timeout sem estabilizar
        logger.error(
            f"[certidao] timeout estabilização ({total_timeout_sec}s) — page não estabilizou."
        )
        raise TimeoutError(
            f"A tela da certidão não terminou de estabilizar em {total_timeout_sec:.0f}s"
        )

    # fase 2: após estabilizar, aguarda controles por um tempo curto
    controls_deadline = asyncio.get_event_loop().time() + extra_wait_controls
    last_controls_state = None
    while asyncio.get_event_loop().time() < controls_deadline:
        if await _detect_internal_server_error():
            logger.warning(
                "[certidao] detectado Internal Server Error na página do popup "
                "(durante espera de controles)."
            )
            raise PopupServerError(
                "Internal Server Error detectado na página do popup."
            )

        sel_ok = await _select_present()
        btns_ok = await _buttons_present()
        controls_state = f"select={sel_ok}, buttons={btns_ok}"
        if controls_state != last_controls_state:
            logger.debug(f"[certidao] aguardando controles... {controls_state}")
            last_controls_state = controls_state
        if sel_ok and btns_ok:
            logger.info(
                "[certidao] controles presentes (select e botões). renderização OK."
            )
            return
        await asyncio.sleep(poll_interval)

    # se saiu do loop sem ambos controles, decide o que fazer:
    sel_ok = await _select_present()
    btns_ok = await _buttons_present()

    if sel_ok and btns_ok:
        logger.info(
            "[certidao] controles apareceram no fim da espera. renderização OK."
        )
        return

    # controles não apareceram — apenas loga (sem salvar HTML) e retorna
    logger.warning(
        f"[certidao] controles não apareceram após extra wait ({extra_wait_controls}s): "
        f"select={sel_ok}, buttons={btns_ok}"
    )

    # Consideramos a página "renderizada o suficiente" — retornamos e permitimos que etapas seguintes tentem operar.
    # Se preferir tratar isso como erro (para forçar nova tentativa), lance TimeoutError aqui.
    return


# ===================== Fluxo Certidão (com POST + retries) =====================


async def acessar_menu_certidao_via_api(
    page,
    notificar=None,
    passo=None,
    max_tentativas: int = 3,
    about_blank_wait_sec: float = 8.0,
    post_retry_delay_sec: float = 1.5,
):
    """
    Abre a tela 'Consultar Situação Fiscal' via POST (AJAX) em /grpfor/home.seam,
    obtém a URL do popup a partir do retorno (org.ajax4jsf.oncomplete -> abrirPopup(...)),
    cria uma nova aba e navega para essa URL.

    Se a nova aba permanecer em "about:blank" por mais que `about_blank_wait_sec`,
    fecha a aba e tenta o POST novamente (até max_tentativas). Se detectar
    'Internal Server Error' no popup, fecha a aba e re-tenta o POST.

    Retorna a Playwright Page do popup (já navegada) em caso de sucesso.
    """
    base_url = "https://iss.fortaleza.ce.gov.br/grpfor/home.seam"
    ctx = page.context

    try:
        app = get_app_module()
    except Exception:
        app = None

    for tentativa in range(1, max_tentativas + 1):
        logger.info(f"[certidao] tentativa {tentativa}/{max_tentativas}")
        if notificar:
            notificar(
                f"Tentando abrir menu Certidão (tentativa {tentativa}/{max_tentativas})"
            )
        if passo:
            passo()

        # extrai ViewState
        try:
            vs_el = await page.query_selector("input[name='javax.faces.ViewState']")
            viewstate = await (vs_el.get_attribute("value") if vs_el else None)
        except Exception as e:
            logger.debug(f"[certidao] erro ao obter ViewState: {e}")
            viewstate = None

        if not viewstate:
            logger.warning(
                "[certidao] javax.faces.ViewState não encontrado: tentando recarregar home.seam"
            )
            if app and hasattr(app, "resilient_goto"):
                try:
                    await app.resilient_goto(page, base_url)
                    await asyncio.sleep(0.6)
                except Exception as e:
                    logger.debug(f"[certidao] resilient_goto falhou: {e}")
            else:
                try:
                    await page.goto(base_url)
                    await asyncio.sleep(0.6)
                except Exception as e:
                    logger.debug(f"[certidao] page.goto(home) falhou: {e}")
            # tenta pegar novamente
            try:
                vs_el = await page.query_selector("input[name='javax.faces.ViewState']")
                viewstate = await (vs_el.get_attribute("value") if vs_el else None)
            except Exception:
                viewstate = None

        if not viewstate:
            raise Exception("Não foi possível obter javax.faces.ViewState da página atual.")

        # localiza o link pelo texto (preferível)
        link = await page.query_selector("a:has-text('Consultar Situação Fiscal')")
        if not link:
            link = await page.query_selector("[id^='formMenuTopo:menuRelatorios:j_id']")
        if not link:
            raise Exception("Link 'Consultar Situação Fiscal' não encontrado no menu.")
        btn_id = await link.get_attribute("id")
        if not btn_id:
            raise Exception("O link 'Consultar Situação Fiscal' não tem ID válido.")

        # monta payload do A4J
        data = {
            "AJAXREQUEST": "_viewRoot",
            "formMenuTopo": "formMenuTopo",
            "javax.faces.ViewState": viewstate,
            btn_id: btn_id,
            "AJAX:EVENTS_COUNT": "1",
        }

        # fecha abas extras antes de gerar nova popup (mantém apenas page)
        try:
            for p in list(ctx.pages):
                if p is not page:
                    try:
                        logger.debug(
                            f"[certidao] fechando aba extra antes do POST: {p.url}"
                        )
                        await p.close()
                    except Exception:
                        logger.debug(
                            "[certidao] falha ao fechar aba extra (ignorada)"
                        )
            logger.info(
                f"[certidao] abas após limpeza: {[p.url for p in ctx.pages]}"
            )
        except Exception as e:
            logger.debug(f"[certidao] erro ao limpar abas antes do POST: {e}")

        # faz POST
        try:
            resp = await page.request.post(base_url, form=data)
        except Exception as e:
            logger.warning(f"[certidao] falha no POST tentativa {tentativa}: {e}")
            if tentativa >= max_tentativas:
                raise
            await asyncio.sleep(post_retry_delay_sec)
            continue

        logger.info(
            f"[certidao] POST enviado (tentativa {tentativa}), HTTP {resp.status}"
        )
        if resp.status != 200:
            logger.warning(f"[certidao] POST retornou HTTP {resp.status}")
            if tentativa >= max_tentativas:
                raise Exception(
                    f"Erro HTTP {resp.status} ao tentar abrir popup de certidão."
                )
            await asyncio.sleep(post_retry_delay_sec)
            continue

        text = await resp.text()

        # extrai URL do abrirPopup
        m = re.search(
            r"abrirPopup\('([^']*extratoSituacaoFiscal\.seam[^']*)'\)", text
        )
        if not m:
            logger.warning(
                f"[certidao] abrirPopup(...) não encontrado no response (tentativa {tentativa})."
            )
            if tentativa >= max_tentativas:
                raise Exception(
                    "abrirPopup(...) não encontrado na resposta AJAX."
                )
            await asyncio.sleep(post_retry_delay_sec)
            continue

        url_popup = html.unescape(m.group(1)).replace("&amp;", "&")
        if not url_popup.startswith("http"):
            if url_popup.startswith("/"):
                url_popup = "https://iss.fortaleza.ce.gov.br" + url_popup
            else:
                raise Exception(
                    "URL inválida obtida para popup de certidão: " + url_popup
                )

        logger.info(f"[certidao] URL do popup: {url_popup}")

        # cria popup e navega
        popup = await ctx.new_page()
        logger.info(
            f"[certidao] nova aba criada (tentativa {tentativa}). navegando para popup..."
        )
        try:
            # tentativa rápida de goto
            try:
                await popup.goto(
                    url_popup,
                    wait_until="domcontentloaded",
                    timeout=int(about_blank_wait_sec * 1000),
                )
            except Exception as e:
                logger.debug(
                    f"[certidao] popup.goto timeout/erro (possível about:blank): {e}"
                )

            # polling curto para about:blank -> URL válida
            elapsed = 0.0
            interval = 0.15
            ok = False
            max_wait = about_blank_wait_sec
            while elapsed < max_wait:
                cur_url = popup.url
                try:
                    title = await popup.title()
                except Exception:
                    title = None
                logger.debug(
                    f"[certidao] polling popup (t={elapsed:.2f}s): url={cur_url}, title={title}"
                )
                if cur_url and not cur_url.startswith("about:blank"):
                    ok = True
                    logger.info(f"[certidao] popup URL OK: {cur_url}")
                    break
                if (
                    title
                    and title.strip()
                    and title.lower().strip() != "about:blank"
                ):
                    ok = True
                    logger.info(
                        f"[certidao] popup título detectado: {title}"
                    )
                    break
                await asyncio.sleep(interval)
                elapsed += interval

            if not ok:
                logger.warning(
                    f"[certidao] popup permaneceu em about:blank após {about_blank_wait_sec}s "
                    f"(tentativa {tentativa}). Fechando e re-tentando."
                )
                try:
                    await popup.close()
                    logger.info(
                        f"[certidao] aba about:blank fechada (tentativa {tentativa})."
                    )
                except Exception:
                    logger.debug(
                        "[certidao] falha ao fechar aba about:blank (ignorada)"
                    )

                # garante que só resta a aba principal (fecha quaisquer outras que existam)
                try:
                    for p in list(ctx.pages):
                        if p is page:
                            continue
                        try:
                            logger.info(
                                f"[certidao] fechando aba extra pós-falha: {p.url}"
                            )
                            await p.close()
                        except Exception:
                            logger.debug(
                                "[certidao] falha ao fechar aba extra (ignorada)"
                            )
                    logger.info(
                        f"[certidao] após fechamento, abas restantes: {[p.url for p in ctx.pages]}"
                    )
                except Exception as e:
                    logger.debug(
                        f"[certidao] erro ao garantir fechamento de abas extras: {e}"
                    )

                # assegura que a aba principal esteja em home.seam antes de novo POST
                logger.info(
                    "[certidao] garantindo que aba principal esteja em home.seam antes de re-tentar POST"
                )
                if app and hasattr(app, "resilient_goto"):
                    try:
                        await app.resilient_goto(page, base_url)
                        await asyncio.sleep(0.6)
                    except Exception as e:
                        logger.debug(
                            f"[certidao] resilient_goto falhou: {e}"
                        )
                else:
                    try:
                        await page.goto(base_url)
                        await asyncio.sleep(0.6)
                    except Exception as e:
                        logger.debug(f"[certidao] page.goto falhou: {e}")

                if tentativa < max_tentativas:
                    await asyncio.sleep(post_retry_delay_sec)
                    continue
                else:
                    raise Exception(
                        "Popup ficou em about:blank após múltiplas tentativas."
                    )

            # se chegamos aqui, popup tem uma URL não-about:blank; agora espera render completa
            base_dir = getattr(app, "BASE_DIR", ".") if app else "."
            try:
                await esperar_tela_certidao_renderizada(
                    popup,
                    total_timeout_sec=30.0,
                    extra_wait_controls=8.0,
                    poll_interval=0.15,
                    base_dir=base_dir,
                    debug_prefix=f"popup_parcial_tent{tentativa}",
                )
            except PopupServerError as pse:
                logger.warning(
                    f"[certidao] Popup com erro de servidor detectado: {pse}; "
                    "fechando e re-tentando."
                )
                try:
                    if not popup.is_closed():
                        await popup.close()
                except Exception:
                    pass

                # recarrega a home antes de refazer
                if app and hasattr(app, "resilient_goto"):
                    try:
                        await app.resilient_goto(page, base_url)
                        await asyncio.sleep(0.6)
                    except Exception:
                        try:
                            await page.goto(base_url)
                            await asyncio.sleep(0.6)
                        except Exception:
                            pass
                else:
                    try:
                        await page.goto(base_url)
                        await asyncio.sleep(0.6)
                    except Exception:
                        pass

                if tentativa < max_tentativas:
                    await asyncio.sleep(post_retry_delay_sec)
                    continue
                else:
                    raise Exception(
                        "Popup apresentou Internal Server Error em todas as tentativas."
                    )

            except TimeoutError as te:
                # página não terminou de renderizar - apenas loga e tenta recarregar a home e re-tentar
                logger.warning(
                    f"[certidao] tela ainda não renderizada: {te}"
                )

                try:
                    if not popup.is_closed():
                        await popup.close()
                except Exception:
                    pass

                # tenta recarregar a home antes de nova tentativa
                if app and hasattr(app, "resilient_goto"):
                    try:
                        await app.resilient_goto(page, base_url)
                        await asyncio.sleep(0.6)
                    except Exception:
                        try:
                            await page.goto(base_url)
                            await asyncio.sleep(0.6)
                        except Exception:
                            pass
                else:
                    try:
                        await page.goto(base_url)
                        await asyncio.sleep(0.6)
                    except Exception:
                        pass

                if tentativa < max_tentativas:
                    logger.info(
                        "[certidao] re-fazendo POST para abrir popup (próxima tentativa)."
                    )
                    await asyncio.sleep(post_retry_delay_sec)
                    continue
                else:
                    raise Exception(
                        "Popup não terminou de renderizar após múltiplas tentativas."
                    )

            # sucesso: popup navegou e renderizou o suficiente
            logger.info(
                f"[certidao] popup pronto (tentativa {tentativa}). "
                f"abas atuais: {[p.url for p in ctx.pages]}"
            )
            if notificar:
                notificar(f"Popup da certidão aberto: {url_popup}")
            return popup

        except Exception as e:
            logger.debug(
                f"[certidao] exceção durante criação/navegação do popup: {e}"
            )
            try:
                if not popup.is_closed():
                    await popup.close()
            except Exception:
                pass
            if tentativa < max_tentativas:
                logger.info(
                    "[certidao] re-tentando após exceção na criação do popup."
                )
                await asyncio.sleep(post_retry_delay_sec)
                continue
            raise

    raise Exception("Falha ao abrir popup de certidão após múltiplas tentativas.")


# ----------------- restantes: preencher + baixar -----------------


async def preencher_certidao(page, tipo_value: str = "6", notificar=None, passo=None):
    app = get_app_module()
    cnpj = (
        getattr(app, "current_state", {}).get("cnpj_aberto")
        if hasattr(app, "current_state")
        else None
    )
    if notificar:
        notificar(f"{cnpj} - Selecionando tipo de certidão {tipo_value}")
    if passo:
        passo()
    sel = "select[id$=':tipoCertidao']"
    logger.info(f"[certidao] aguardando selector {sel} no popup...")
    await page.wait_for_selector(sel, timeout=10000)
    await page.select_option(sel, value=tipo_value)
    await asyncio.sleep(1)


async def baixar_certidoes(page, pasta_empresa: str, cnpj: str, notificar=None, passo=None):
    pasta_certidoes = os.path.join(pasta_empresa, "certidoes")
    os.makedirs(pasta_certidoes, exist_ok=True)

    # Recuperar
    btn_rec = "input[id$=':btnRecuperar']"
    await page.wait_for_selector(btn_rec, timeout=8000)
    if notificar:
        notificar(f"{cnpj} - Clicando em 'Recuperar'")
    if passo:
        passo()
    await page.click(btn_rec)
    await page.wait_for_load_state("networkidle")

    # Pesquisar
    btn_pesq = "input[id$=':btnPesquisar']"
    await page.wait_for_selector(btn_pesq, timeout=8000)
    if notificar:
        notificar(f"{cnpj} - Clicando em 'Pesquisar'")
    if passo:
        passo()
    await page.click(btn_pesq)
    await page.wait_for_load_state("networkidle")

    # Emitir Certidão (PDF 1)
    btn_emitir = "input[id$=':btnEmitirCertidao']"
    await page.wait_for_selector(btn_emitir, timeout=10000)
    is_disabled = await page.get_attribute(btn_emitir, "disabled") is not None
    if not is_disabled:
        if notificar:
            notificar(f"{cnpj} - Emitindo Certidão (PDF 1)")
        if passo:
            passo()
        async with page.expect_download() as dl1:
            await page.click(btn_emitir)
        download1 = await dl1.value
        caminho_pdf1 = os.path.join(pasta_certidoes, f"{cnpj}_certidao_iss.pdf")
        await download1.save_as(caminho_pdf1)
        logger.info(f"{cnpj} - PDF 1 salvo em {caminho_pdf1}")
    else:
        logger.warning(f"{cnpj} - Botão 'Emitir Certidão' desabilitado")

    # Exportar Resultado (PDF 2)
    btn_export = "input[id$=':btnExportar']"
    await page.wait_for_selector(btn_export, timeout=10000)
    if notificar:
        notificar(f"{cnpj} - Exportando Resultado (PDF 2)")
    if passo:
        passo()
    async with page.expect_download() as dl2:
        await page.click(btn_export)
    download2 = await dl2.value
    caminho_pdf2 = os.path.join(pasta_certidoes, f"{cnpj}_certidao_resultado.pdf")
    await download2.save_as(caminho_pdf2)
    logger.info(f"{cnpj} - PDF 2 salvo em {caminho_pdf2}")


# ===================== Orquestração (run_cnpj) =====================


async def run_cnpj(task, flow_ctx: FlowContext) -> dict:
    """
    Executa o fluxo de Certidão para um TaskLike compatível com a fila/ARQ:
      - task.colaborador_norm
      - task.cnpj
      - task.mes
      - task.usuario
      - task.senha
      - task.tipo_estrutura (convencional/domínio — não usado aqui, mas mantido)
    """
    app = get_app_module()

    browser_get = getattr(app, "browser_get")
    BASE_DIR = getattr(app, "BASE_DIR", ".")
    DEF_TIMEOUT = getattr(app, "DEF_TIMEOUT_MS", 30_000)
    NAV_TIMEOUT = getattr(app, "NAV_TIMEOUT_MS", 60_000)
    PER_STEP = getattr(app, "PER_STEP_TIMEOUT_SEC", 120)

    colab_dirname = task.colaborador_norm
    cnpj = task.cnpj
    mes = task.mes

    # Telemetria
    try:
        app.current_state["cnpj_aberto"] = cnpj
        app.current_state["fase"] = "certidao_iniciando"
    except Exception:
        pass

    await log_flow(flow_ctx, "=== INÍCIO (Certidão) ===", event="FLOW_START")

    context = None
    try:
        # Browser resiliente via flow_core
        context = await create_browser_context(browser_get)
        context.set_default_timeout(DEF_TIMEOUT)
        context.set_default_navigation_timeout(NAV_TIMEOUT)
        page = await context.new_page()

        mesano = mes.replace("/", ".")
        pasta_mesano = os.path.join(BASE_DIR, colab_dirname, mesano)
        os.makedirs(pasta_mesano, exist_ok=True)

        # Reaproveita login/pesquisar_empresa de flow_notas
        from flow_notas import login, pesquisar_empresa

        # 1) Login
        await run_step(
            flow_ctx,
            "Login",
            login(page, task.usuario, task.senha),
            per_step_timeout=PER_STEP,
        )

        # 2) Pesquisar Empresa
        nome_emp, pasta_empresa = await run_step(
            flow_ctx,
            "Pesquisar Empresa",
            pesquisar_empresa(page, cnpj, colab_dirname, BASE_DIR, mes),
            per_step_timeout=PER_STEP,
        )

        # 3) Acessar Relatórios (abre popup Certidão)
        popup = await run_step(
            flow_ctx,
            "Acessar Relatórios",
            acessar_menu_certidao_via_api(page),
            per_step_timeout=PER_STEP,
        )

        try:
            await popup.bring_to_front()
        except Exception:
            pass

        # 4) Preencher Certidão
        await run_step(
            flow_ctx,
            "Preencher Certidão",
            preencher_certidao(popup),
            per_step_timeout=PER_STEP,
        )

        # 5) Baixar PDFs
        await run_step(
            flow_ctx,
            "Baixar PDFs",
            baixar_certidoes(popup, pasta_empresa, cnpj),
            per_step_timeout=PER_STEP,
        )

        await log_flow(
            flow_ctx,
            "=== FIM (Certidão OK) ===",
            event="FLOW_END",
        )

        return {
            "cnpj": cnpj,
            "status": "ok",
            "empresa": nome_emp,
            "pasta": pasta_empresa,
        }

    except FlowError as e:
        await log_flow(
            flow_ctx,
            f"Erro de domínio na certidão: {e.message}",
            level=logging.ERROR,
            event="ERROR",
            code=e.code,
        )
        raise

    except Exception as e:
        await log_flow(
            flow_ctx,
            f"Erro inesperado no fluxo de certidão: {e}",
            level=logging.ERROR,
            event="ERROR",
            code="UNEXPECTED",
        )
        raise

    finally:
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass


# ===================== Job do ARQ =====================


async def job_certidao(ctx, colaborador, cnpj, mes, usuario, senha, regime):
    """
    Entry point do ARQ para Certidão.
    Assinatura compatível com job_escrituracao / job_notas:
      ctx, colaborador, cnpj, mes, usuario, senha, regime
    """
    logger.info(
        f"[job_certidao] colaborador={colaborador}, cnpj={cnpj}, mes={mes}, regime={regime}"
    )

    app = get_app_module()

    # Normaliza CNPJ
    somente_digitos = getattr(app, "somente_digitos")
    cnpj_norm = somente_digitos(cnpj).zfill(14)

    # “convencional” ou “dominio”
    tipo_estrutura = str(regime or "convencional").lower()
    if tipo_estrutura.startswith("dom"):
        tipo_estrutura = "dominio"
    else:
        tipo_estrutura = "convencional"

    # O colaborador DEVE ser normalizado, pois será nome de pasta
    normalizar = getattr(app, "normalizar_nome_colaborador")
    colaborador_norm = normalizar(colaborador)

    class TaskLike:
        __slots__ = ("colaborador_norm", "cnpj", "mes", "usuario", "senha", "tipo_estrutura")

        def __init__(self, colaborador_norm, cnpj, mes, usuario, senha, tipo_estrutura):
            self.colaborador_norm = colaborador_norm
            self.cnpj = cnpj
            self.mes = mes
            self.usuario = usuario
            self.senha = senha
            self.tipo_estrutura = tipo_estrutura

    task = TaskLike(
        colaborador_norm=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        usuario=usuario,
        senha=senha,
        tipo_estrutura=tipo_estrutura,
    )

    # Telemetria simples
    try:
        app.current_state["fase"] = "certidao"
        app.current_state["cnpj_aberto"] = cnpj_norm
    except Exception:
        pass

    # Metadados do job (se existirem)
    job_id = ctx.get("job_id")
    job_try = ctx.get("job_try")

    flow_ctx = FlowContext(
        flow="certidao",
        colab=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        job_id=job_id,
        job_try=job_try,
    )

    await log_flow(flow_ctx, "job_certidao iniciado", event="EVENT")

    result = await run_cnpj(task, flow_ctx)

    await log_flow(flow_ctx, "job_certidao finalizado", event="EVENT")

    return result
