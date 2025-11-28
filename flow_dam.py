#!/usr/bin/env python3
# flow_dam.py — Flow de emissão de DAM usando flow_core

import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from playwright.async_api import TimeoutError as PWTimeoutError  # type: ignore

from flow_core import (
    FlowContext,
    FlowError,
    LoginError,
    CnpjInexistenteError,
    CnpjMismatchError,
    create_browser_context,
    run_step,
    log_flow,
    get_app_module,
)

logger = logging.getLogger("iss-automacao-dam")


# ===================== Login / Empresa (mesma base de notas) =====================

async def login(page, usuario: str, senha: str) -> None:
    """
    Login no portal do ISS. Erros de credencial viram LoginError.
    """
    app = get_app_module()
    resilient_goto = getattr(app, "resilient_goto")

    await resilient_goto(page, "https://iss.fortaleza.ce.gov.br/grpfor/oauth2/login")
    await page.fill("#username", usuario)
    await page.fill("#password", senha)
    await page.click("#botao-entrar")
    await page.wait_for_load_state("networkidle")

    erro_login = await page.query_selector(".login-error-pg .login-error-msg")
    if erro_login:
        msg = (await erro_login.inner_text()).strip()
        # aqui a msg vira LOGIN_ERROR bonitinho no front
        raise LoginError(msg)

    # Modal "Deseja manter logado?" → Não
    try:
        await page.wait_for_selector(
            "form[id='j_id461'] input[type='submit'][value='Não']",
            timeout=3000,
        )
        await page.click("form[id='j_id461'] input[type='submit'][value='Não']")
    except Exception:
        pass


async def pesquisar_empresa(
    page,
    cnpj: str,
    colaborador_norm: str,
    base_dir: str,
    mes: str,
) -> tuple[str, str]:
    """
    Pesquisa empresa pelo CNPJ e seleciona na tela.

    Pode levantar:
      - CnpjInexistenteError
      - CnpjMismatchError
      - FlowError HOME_LOAD_ERROR (erro HTTP / timeout)
    """
    app = get_app_module()
    resilient_goto = getattr(app, "resilient_goto")
    somente_digitos = getattr(app, "somente_digitos")
    limpar_nome = getattr(app, "limpar_nome")

    logger.info(f"[DAM] Pesquisando empresa {cnpj}")
    tentativas = 3

    for tentativa in range(1, tentativas + 1):
        try:
            response = await resilient_goto(
                page,
                "https://iss.fortaleza.ce.gov.br/grpfor/home.seam",
                timeout=120_000,
            )
            status = getattr(response, "status", None)
            if status is None or status >= 500:
                # salva HTML para debug (só pra dev, não vai pro cliente)
                html = await page.content()
                mesano = mes.replace("/", ".")
                err_dir = os.path.join(base_dir, colaborador_norm, mesano)
                os.makedirs(err_dir, exist_ok=True)
                path = os.path.join(err_dir, f"{cnpj}_erro_tentativa{tentativa}.html")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.warning(f"[DAM] Tentativa {tentativa}: HTTP {status}. HTML salvo em {path}")
                if tentativa == tentativas:
                    raise FlowError(
                        "HOME_LOAD_ERROR",
                        f"Erro ao carregar home.seam para {cnpj} (HTTP {status})",
                        retryable=True,
                    )
                await asyncio.sleep(2)
                continue
            break
        except Exception as e:
            logger.warning(f"[DAM] Tentativa {tentativa} falhou: {e}")
            if tentativa == tentativas:
                raise FlowError(
                    "HOME_LOAD_ERROR",
                    f"Erro ao carregar home.seam para {cnpj}: {e}",
                    retryable=True,
                )

    await page.wait_for_selector("input[id$='cpfPesquisa']", timeout=15_000)
    await page.check("input[value='CNPJ']")
    await asyncio.sleep(1.4)

    inp = "input[id$='cpfPesquisa']"
    await page.fill(inp, "")
    await asyncio.sleep(1.2)
    await page.fill(inp, cnpj)
    await asyncio.sleep(1.4)
    await page.evaluate("document.querySelector(\"input[id$='cpfPesquisa']\").blur()")
    await asyncio.sleep(1.4)

    # limpa tabela
    await page.evaluate(
        """
        const tbody = document.querySelector("table[id$='empresaDataTable'] tbody");
        if (tbody) tbody.innerHTML = "";
        """
    )
    await page.click("input[id$='btnPesquisar']")

    try:
        await page.wait_for_selector("#mpProgressoContainer", state="visible", timeout=5_000)
    except Exception:
        pass
    await page.wait_for_selector("#mpProgressoContainer", state="hidden", timeout=40_000)

    # "Nenhum registro encontrado"
    try:
        msg_elem = await page.wait_for_selector(
            "//span[@id='alteraInscricaoForm:j_id369']/span",
            timeout=4_000,
        )
        msg_text = (await msg_elem.inner_text()).strip()
        if "Nenhum registro encontrado" in msg_text:
            raise CnpjInexistenteError(cnpj)
    except (PWTimeoutError, TimeoutError):
        pass

    cnpj_ret_raw = await page.inner_text(
        "table[id$='empresaDataTable'] tbody tr td:nth-child(2) a"
    )
    nome_emp = await page.inner_text(
        "table[id$='empresaDataTable'] tbody tr td:nth-child(4) a"
    )

    def _norm(c: str) -> str:
        return somente_digitos(c).zfill(14)

    if _norm(cnpj) != _norm(cnpj_ret_raw):
        raise CnpjMismatchError(_norm(cnpj), _norm(cnpj_ret_raw))

    mesano = mes.replace("/", ".")
    nome_dir = f"{limpar_nome(nome_emp)} - {_norm(cnpj_ret_raw)}"
    path = os.path.join(base_dir, colaborador_norm, mesano, nome_dir)

    # Seleciona a empresa (A4J AJAX)
    await page.evaluate(
        """
        A4J.AJAX.Submit('alteraInscricaoForm', event, {
          'similarityGroupingId':'alteraInscricaoForm:empresaDataTable:0:linkNome',
          'parameters':{'alteraInscricaoForm:empresaDataTable:0:linkNome':'alteraInscricaoForm:empresaDataTable:0:linkNome'}
        });
        """
    )
    await asyncio.sleep(2)

    try:
        modal = await page.wait_for_selector("#mensagensModalContentDiv", timeout=5_000)
        if modal and await modal.is_visible():
            raise FlowError(
                "EMPRESA_SELECT_ERROR",
                "Erro ao selecionar empresa: mensagens exibidas no modal.",
                retryable=False,
            )
    except (PWTimeoutError, TimeoutError):
        pass

    return nome_emp, path


# ===================== Navegação DAM (menus / competência) =====================

async def acessar_menu_dam(page, ctx: FlowContext) -> None:
    app = get_app_module()
    base_dir = getattr(app, "BASE_DIR", "?")
    await log_flow(
        ctx,
        f"{base_dir} - Acessando menu Recolhimento → Emitir DAM",
        event="STEP_DETAIL",
    )

    await page.hover("text=Recolhimento")
    await asyncio.sleep(1.5)

    btn_id = await page.evaluate(
        """() => {
            const el = document.querySelector("[id^='formMenuTopo:menuRecolhimento:j_id']");
            return el ? el.id : null;
        }"""
    )
    if not btn_id:
        raise FlowError(
            "DAM_MENU_NOT_FOUND",
            "Não foi possível localizar o item de menu 'Recolhimento'.",
            retryable=False,
        )

    await page.evaluate(
        f"""
        A4J.AJAX.Submit('formMenuTopo', event, {{
            'similarityGroupingId':'{btn_id}',
            'parameters':{{'{btn_id}':'{btn_id}'}}
        }});
        """
    )

    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(1.5)


async def selecionar_mes_especifico_dam(
    page,
    mes_num: int,
    ano: int,
    ctx: FlowContext,
) -> None:
    await log_flow(
        ctx,
        f"Selecionando competência {mes_num:02}/{ano} para DAM",
        event="STEP_DETAIL",
    )
    logger.info(f"[DAM] Selecionando competência {mes_num:02}/{ano}")

    await page.wait_for_selector("div.rich-calendar-tool-btn", timeout=5000)
    await page.click("div.rich-calendar-tool-btn")
    await asyncio.sleep(1.5)

    await page.wait_for_selector("#competenciaDateEditorLayout", timeout=4000)

    ano_selector_base = "#competenciaDateEditorLayoutY".replace(":", "\\:")
    for i in range(10):
        div = await page.query_selector(f"{ano_selector_base}{i}")
        if div:
            texto = (await div.inner_text()).strip()
            if texto == str(ano):
                await div.click()
                await asyncio.sleep(1.5)
                break

    mes_selector = f"#competenciaDateEditorLayoutM{mes_num-1}".replace(":", "\\:")
    await page.click(mes_selector)
    await page.click("#competenciaDateEditorButtonOk")
    await asyncio.sleep(1.5)
    logger.info("[DAM] Competência selecionada com sucesso.")


# ===================== Emissão / Download de DAM =====================

async def consultar_e_exportar_tipo_iss(
    page,
    valor: str,
    ctx: FlowContext,
) -> bool:
    """
    Consulta e emite DAM para um tipo específico de ISS (0/1/2).
    Retorna True se baixou PDF, False se não tinha nada.
    """
    try:
        await log_flow(ctx, f"Consultando ISS tipo {valor}", event="STEP_DETAIL")
        logger.info(f"[DAM] Selecionando tipo de imposto: {valor}")

        # Espera loaders sumirem
        for loader_id in ("#mpProgressoContainer", "#mpProgressoCDiv"):
            try:
                if await page.query_selector(loader_id):
                    await page.wait_for_selector(loader_id, state="hidden", timeout=40_000)
            except Exception:
                logger.debug(f"[DAM] Loader {loader_id} ainda visível após timeout, seguindo adiante.")
                break

        combo = page.locator("select#comboImposto")
        await combo.wait_for(state="visible", timeout=40_000)
        await combo.select_option(value=valor)
        await asyncio.sleep(1.5)

        # Modal QR Code bloqueando?
        try:
            if await page.is_visible("#panelQrdCodeDiv"):
                logger.info("[DAM] Modal de QR Code detectado — tentando fechar.")
                try:
                    await page.click("img#hidePanelConfirma")
                    await page.wait_for_selector("#panelQrdCodeDiv", state="hidden", timeout=5000)
                    logger.info("[DAM] Modal de QR Code fechado com sucesso.")
                except Exception as fe:
                    logger.warning(f"[DAM] Não foi possível fechar o modal de QR Code com o X: {fe}")
                    try:
                        await page.evaluate("Richfaces.hideModalPanel('panelQrdCode');")
                        await page.wait_for_selector("#panelQrdCodeDiv", state="hidden", timeout=5000)
                        logger.info("[DAM] Modal de QR Code fechado via JS.")
                    except Exception as fe2:
                        logger.error(f"[DAM] Falha ao tentar forçar fechamento do modal: {fe2}")
        except Exception:
            pass

        logger.info("[DAM] Clicando em Consultar")
        await page.click("input#btnConsultar")

        for loader_id in ("#mpProgressoContainer", "#mpProgressoCDiv"):
            try:
                await page.wait_for_selector(loader_id, state="hidden", timeout=40_000)
            except Exception:
                logger.debug(f"[DAM] Após consultar, loader {loader_id} não sumiu em 30s.")
        await asyncio.sleep(1.5)

        # Espera aparecer resultado ou alerta
        for _ in range(50):
            if await page.is_visible("dt.alert"):
                break
            if await page.is_visible("table#datatable_emissao_dam"):
                break
            if await page.query_selector("#mpProgressoContainer") or await page.query_selector("#mpProgressoCDiv"):
                break
            await asyncio.sleep(1.5)

        if await page.is_visible("dt.alert"):
            try:
                msg = (await page.inner_text("dt.alert")).strip()
            except Exception:
                msg = "Alerta exibido."
            logger.warning(f"[DAM] Alerta exibido no tipo {valor}: {msg}")
            await log_flow(
                ctx,
                f"Alerta ao consultar tipo {valor}: {msg}",
                level=logging.WARNING,
                event="WARN",
                code="DAM_ALERT",
            )
            return False

        if await page.query_selector("#mpProgressoContainer") or await page.query_selector("#mpProgressoCDiv"):
            try:
                await page.wait_for_selector("#mpProgressoContainer", state="hidden", timeout=40_000)
                await page.wait_for_selector("#mpProgressoCDiv", state="hidden", timeout=40_000)
            except Exception:
                logger.warning("[DAM] Progresso não ficou hidden no tempo esperado (seguindo mesmo assim).")

            if await page.is_visible("dt.alert"):
                try:
                    msg = (await page.inner_text("dt.alert")).strip()
                except Exception:
                    msg = "Alerta exibido."
                logger.warning(f"[DAM] Alerta exibido no tipo {valor}: {msg}")
                await log_flow(
                    ctx,
                    f"Alerta ao consultar tipo {valor}: {msg}",
                    level=logging.WARNING,
                    event="WARN",
                    code="DAM_ALERT",
                )
                return False

        if not await page.is_visible("table#datatable_emissao_dam"):
            try:
                await page.wait_for_selector("table#datatable_emissao_dam", timeout=5000)
            except Exception:
                logger.warning(f"[DAM] Nenhum registro (tabela não apareceu) para tipo {valor}")
                await log_flow(
                    ctx,
                    f"Nenhum registro para DAM tipo {valor}",
                    level=logging.INFO,
                    event="INFO",
                    code="DAM_NO_RECORDS",
                )
                return False

        logger.info("[DAM] Tabela encontrada, emitindo DAM")

        # Marcar todos
        try:
            marcar_btn = await page.query_selector("input#btnMarcarTodos")
            if marcar_btn:
                valor_btn = (await marcar_btn.get_attribute("value")) or ""
                if "Marcar" in valor_btn:
                    logger.info("[DAM] Clicando em 'Marcar Todos'")
                    await marcar_btn.click()
                    await asyncio.sleep(1.5)
        except Exception:
            logger.warning("[DAM] Botão 'Marcar Todos' não encontrado")

        # Emitir DAM
        try:
            logger.info("[DAM] Clicando em 'Emitir DAM'")
            await page.click("input#btnEmitir")
            await asyncio.sleep(1.5)
        except Exception as e:
            await log_flow(
                ctx,
                f"Falha ao clicar em 'Emitir DAM' (tipo {valor}): {e}",
                level=logging.ERROR,
                event="ERROR",
                code="DAM_EMIT_CLICK_FAIL",
            )
            raise FlowError(
                "DAM_EMIT_CLICK_FAIL",
                f"Falha ao clicar em 'Emitir DAM' (tipo {valor})",
                retryable=False,
            )

        await page.wait_for_selector("input#btnConfirma", timeout=20_000)
        await log_flow(ctx, f"Confirmando emissão (tipo {valor})", event="STEP_DETAIL")

        # Download do PDF
        async with page.expect_download(timeout=20_000) as dl:
            await page.click("input#btnConfirma")
        download = await dl.value

        nome = f"DAM_tipo_{valor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        app = get_app_module()
        pasta_base = getattr(app, "BASE_DIR", ".")
        pasta_empresa = getattr(app, "_DAM_PASTA_EMPRESA", None) or pasta_base
        os.makedirs(pasta_empresa, exist_ok=True)
        caminho = os.path.join(pasta_empresa, nome)
        await download.save_as(caminho)

        logger.info(f"[DAM] DAM baixado com sucesso: {nome}")
        await log_flow(
            ctx,
            f"DAM tipo {valor} baixado com sucesso: {nome}",
            event="STEP_DETAIL",
        )

        # Fecha modal se existir
        try:
            if await page.is_visible("a#j_id401"):
                logger.info("[DAM] Fechando modal de download (clicando no 'X')")
                await page.click("a#j_id401")
                await asyncio.sleep(1.5)
        except Exception as e:
            logger.warning(f"[DAM] Não foi possível clicar no 'X' do modal: {e}")

        return True

    except FlowError:
        # já logado bonito acima
        raise
    except Exception as e:
        # Erro “genérico” mas sem stack no log do cliente
        await log_flow(
            ctx,
            f"Erro ao processar tipo {valor}: {e}",
            level=logging.ERROR,
            event="ERROR",
            code="DAM_TIPO_ERROR",
        )
        # Propaga; quem chama decide se trata ou não
        raise


async def consultar_todos_tipos_iss(
    page,
    ctx: FlowContext,
) -> Dict[str, bool]:
    """
    Loop nos tipos 0/1/2. Nunca explode a execução inteira por causa de um tipo.
    """
    await log_flow(ctx, "Iniciando consulta de todos os tipos de ISS para DAM", event="STEP_DETAIL")
    logger.info("[DAM] Loopando tipos de ISS do dropdown")

    try:
        await page.wait_for_selector("select#comboImposto", timeout=20_000)
    except Exception:
        logger.error("[DAM] Dropdown 'comboImposto' não carregou. Interrompendo processo.")
        raise FlowError(
            "DAM_COMBO_NOT_FOUND",
            "Combo de tipo de imposto (comboImposto) não apareceu.",
            retryable=False,
        )

    resultados: Dict[str, bool] = {}
    for valor in ["0", "1", "2"]:
        await log_flow(ctx, f"Iniciando processamento do tipo {valor}", event="STEP_DETAIL")
        logger.info(f"[DAM] Processando tipo {valor}")
        try:
            ok = await consultar_e_exportar_tipo_iss(page, valor, ctx)
            resultados[valor] = bool(ok)
        except FlowError as e:
            # Registra e segue pros outros tipos
            resultados[valor] = False
            await log_flow(
                ctx,
                f"Tipo {valor} falhou: {e.message}",
                level=logging.WARNING,
                event="WARN",
                code=e.code,
            )
        except Exception as e:
            resultados[valor] = False
            await log_flow(
                ctx,
                f"Tipo {valor} falhou com erro inesperado: {e}",
                level=logging.ERROR,
                event="ERROR",
                code="DAM_TIPO_UNEXPECTED",
            )

    if not any(resultados.values()):
        await log_flow(ctx, "Nenhum DAM disponível para este CNPJ.", event="INFO")

    return resultados


# ===================== Orquestração: run_cnpj (DAM) =====================

async def run_cnpj(task, flow_ctx: FlowContext) -> Dict[str, Any]:
    """
    Executa o fluxo de DAM para um TaskLike compatível com a fila/ARQ:
      - task.colaborador_norm
      - task.cnpj
      - task.mes  (MM/AAAA)
      - task.usuario
      - task.senha
      - task.tipo_estrutura (opcional: "dominio" ou "convencional")
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

    # Telemetria global
    try:
        app.current_state["cnpj_aberto"] = cnpj
        app.current_state["fase"] = "dam"
    except Exception:
        pass

    await log_flow(flow_ctx, "=== INÍCIO (DAM) ===", event="FLOW_START")

    context = None
    try:
        context = await create_browser_context(browser_get)
        context.set_default_timeout(DEF_TIMEOUT)
        context.set_default_navigation_timeout(NAV_TIMEOUT)
        page = await context.new_page()

        mesano = mes.replace("/", ".")
        pasta_mesano = os.path.join(BASE_DIR, colab_dirname, mesano)

        # 1) Login
        await run_step(
            flow_ctx,
            "Login",
            login(page, task.usuario, task.senha),
            per_step_timeout=PER_STEP,
        )

        # 2) Pesquisar empresa
        nome_empresa, pasta_empresa = await run_step(
            flow_ctx,
            "Pesquisar Empresa",
            pesquisar_empresa(page, cnpj, colab_dirname, BASE_DIR, mes),
            per_step_timeout=PER_STEP,
        )

        # Info auxiliar para outras partes do sistema
        try:
            app._DAM_CNPJ = cnpj
            app._DAM_PASTA_EMPRESA = pasta_empresa
        except Exception:
            pass

        # 3) Acessar menu DAM
        await run_step(
            flow_ctx,
            "DAM: Acessar menu",
            acessar_menu_dam(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        # 4) Selecionar competência
        mes_num, ano = map(int, mes.split("/"))
        await run_step(
            flow_ctx,
            "DAM: Selecionar competência",
            selecionar_mes_especifico_dam(page, mes_num, ano, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        # 5) Consultar/Emitir/Download tipos 0/1/2
        resultados = await run_step(
            flow_ctx,
            "DAM: Consultar/Emitir/Download (tipos 0/1/2)",
            consultar_todos_tipos_iss(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        tipos_ok = [k for k, v in (resultados or {}).items() if v]
        await log_flow(
            flow_ctx,
            f"DAM(s) baixado(s) para tipos: {', '.join(tipos_ok) if tipos_ok else 'nenhum'}",
            event="INFO",
        )

        await log_flow(
            flow_ctx,
            "=== FIM (DAM OK) ===",
            event="FLOW_END",
        )

        return {
            "cnpj": cnpj,
            "status": "ok",
            "empresa": nome_empresa,
            "pasta": pasta_empresa,
            "tipos_ok": tipos_ok,
        }

    except FlowError as e:
        await log_flow(
            flow_ctx,
            f"Erro de domínio no DAM: {e.message}",
            level=logging.ERROR,
            event="ERROR",
            code=e.code,
        )
        raise

    except Exception as e:
        await log_flow(
            flow_ctx,
            f"Erro inesperado no fluxo de DAM: {e}",
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

async def job_dam(ctx, colaborador, cnpj, mes, usuario, senha, tipo_estrutura):
    """
    Entry point do ARQ para DAM.

    ctx: dict de contexto do ARQ (job_id, job_try, etc.).
    """
    logger.info(
        f"[job_dam] colaborador={colaborador}, cnpj={cnpj}, mes={mes}, tipo_estrutura={tipo_estrutura}"
    )

    app = get_app_module()

    # Normaliza CNPJ
    somente_digitos = getattr(app, "somente_digitos")
    cnpj_norm = somente_digitos(cnpj).zfill(14)

    # O painel envia “convencional” ou “dominio”
    tipo_estrutura_str = str(tipo_estrutura or "convencional").lower()
    if tipo_estrutura_str.startswith("dom"):
        tipo_estrutura_str = "dominio"
    else:
        tipo_estrutura_str = "convencional"

    # O colaborador DEVE ser normalizado (nome de pasta)
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
        tipo_estrutura=tipo_estrutura_str,
    )

    # Telemetria global
    try:
        app.current_state["fase"] = "dam"
        app.current_state["cnpj_aberto"] = cnpj_norm
    except Exception:
        pass

    # Metadados do job (se existirem no ctx)
    job_id = ctx.get("job_id")
    job_try = ctx.get("job_try")

    flow_ctx = FlowContext(
        flow="dam",
        colab=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        job_id=job_id,
        job_try=job_try,
    )

    await log_flow(flow_ctx, "job_dam iniciado", event="EVENT")

    result = await run_cnpj(task, flow_ctx)

    await log_flow(flow_ctx, "job_dam finalizado", event="EVENT")

    return result
