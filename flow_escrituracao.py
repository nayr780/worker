#!/usr/bin/env python3
# flow_escrituracao.py — Flow de Escrituração usando flow_core

import os
import asyncio
import logging
from datetime import datetime
from typing import Tuple

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

logger = logging.getLogger("iss-automacao-escrituracao")


# ===================== Login / Empresa =====================

async def login(page, usuario: str, senha: str) -> None:
    """
    Login no portal do ISS. Erros de credencial viram LoginError.
    """
    app = get_app_module()
    resilient = getattr(app, "resilient_goto")

    await resilient(page, "https://iss.fortaleza.ce.gov.br/grpfor/oauth2/login")
    await page.fill("#username", usuario)
    await page.fill("#password", senha)
    await page.click("#botao-entrar")
    await page.wait_for_load_state("networkidle")

    erro_login = await page.query_selector(".login-error-pg .login-error-msg")
    if erro_login:
        msg = (await erro_login.inner_text()).strip()
        raise LoginError(msg)

    # possível modal de confirmação "Não"
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
) -> Tuple[str, str]:
    """
    Pesquisa empresa pelo CNPJ e seleciona na tela.
    Pode levantar:
      - CnpjInexistenteError
      - CnpjMismatchError
      - FlowError("HOME_LOAD_ERROR"/"EMPRESA_SELECT_ERROR")
    """
    app = get_app_module()
    resilient = getattr(app, "resilient_goto")
    somente_dig = getattr(app, "somente_digitos")
    limpar_nome = getattr(app, "limpar_nome")

    logger.info(f"Pesquisando empresa {cnpj}")
    tentativas = 3
    for tentativa in range(1, tentativas + 1):
        try:
            response = await resilient(
                page,
                "https://iss.fortaleza.ce.gov.br/grpfor/home.seam",
                timeout=120000,
            )
            status = getattr(response, "status", None)
            if status is None or status >= 500:
                logger.warning(
                    f"   → Tentativa {tentativa}: Erro HTTP {status} ao carregar página inicial."
                )
                if tentativa == tentativas:
                    raise FlowError(
                        "HOME_LOAD_ERROR",
                        f"Erro ao carregar página inicial para {cnpj} (HTTP {status})",
                        retryable=True,
                    )
                await asyncio.sleep(2)
                continue
            break
        except Exception as e:
            logger.warning(f"   → Tentativa {tentativa}: falhou com exceção: {e}")
            if tentativa == tentativas:
                raise FlowError(
                    "HOME_LOAD_ERROR",
                    f"Erro ao carregar página inicial para {cnpj}: {e}",
                    retryable=True,
                )

    await page.wait_for_selector("input[id$='cpfPesquisa']", timeout=15000)
    await page.check("input[value='CNPJ']")
    await asyncio.sleep(2)

    inp = "input[id$='cpfPesquisa']"
    await page.fill(inp, "")
    await asyncio.sleep(2)
    await page.fill(inp, cnpj)
    await asyncio.sleep(2)
    await page.evaluate(
        "document.querySelector(\"input[id$='cpfPesquisa']\").blur()"
    )
    await asyncio.sleep(2)

    # limpa tabela
    await page.evaluate(
        """
        const tbody = document.querySelector("table[id$='empresaDataTable'] tbody");
        if (tbody) tbody.innerHTML = "";
        """
    )
    await page.click("input[id$='btnPesquisar']")
    logger.info("   → Clicou em 'Pesquisar'")

    try:
        await page.wait_for_selector(
            "#mpProgressoContainer", state="visible", timeout=5000
        )
    except Exception:
        pass
    await page.wait_for_selector(
        "#mpProgressoContainer", state="hidden", timeout=20000
    )

    # "Nenhum registro encontrado"
    try:
        msg_elem = await page.wait_for_selector(
            "//span[@id='alteraInscricaoForm:j_id369']/span", timeout=3000
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
    logger.info(f"   → CNPJ retornado: {cnpj_ret_raw}")

    def _norm(c: str) -> str:
        return somente_dig(c).zfill(14)

    cnpj_busca = _norm(cnpj)
    cnpj_ret = _norm(cnpj_ret_raw)
    if cnpj_busca != cnpj_ret:
        raise CnpjMismatchError(cnpj_busca, cnpj_ret)

    mesano = mes.replace("/", ".")
    nome_dir = f"{limpar_nome(nome_emp)} - {cnpj_ret}"
    path = os.path.join(base_dir, colaborador_norm, mesano, nome_dir)

    # seleciona empresa
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
        modal = await page.wait_for_selector(
            "#mensagensModalContentDiv", timeout=5000
        )
        if modal and await modal.is_visible():
            raise FlowError(
                "EMPRESA_SELECT_ERROR",
                "Erro ao selecionar empresa: mensagens na tela.",
                retryable=False,
            )
    except (PWTimeoutError, TimeoutError):
        pass

    return nome_emp, path


# ===================== Navegação Escrituração =====================

async def acessar_escrituracao(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Acessando menu Escrituração", event="STEP_DETAIL")
    await page.hover("text=Escrituração")
    await asyncio.sleep(2)
    btn_id = await page.evaluate(
        """() => {
        const el = document.querySelector("[id^='formMenuTopo:menuEscrituracao:j_id']");
        return el ? el.id : null;
    }"""
    )
    if not btn_id:
        raise FlowError(
            "MENU_ESCRITURACAO_NOT_FOUND",
            "Não foi possível localizar o item de menu 'Escrituração'.",
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
    await asyncio.sleep(2)


async def selecionar_mes_especifico(
    page, btn_selector: str, prefixo: str, mes_num: int, ano: int
) -> None:
    await page.click(btn_selector)
    await asyncio.sleep(2)
    ano_selector_base = f"#{prefixo}DateEditorLayoutY".replace(":", "\\:")
    for i in range(10):
        div = await page.query_selector(f"{ano_selector_base}{i}")
        if div:
            texto = (await div.inner_text()).strip()
            if texto == str(ano):
                await div.click()
                await asyncio.sleep(2)
                break
    mes_selector = f"#{prefixo}DateEditorLayoutM{mes_num-1}".replace(":", "\\:")
    ok_selector = f"#{prefixo}DateEditorButtonOk".replace(":", "\\:")
    await page.click(mes_selector)
    await page.click(ok_selector)
    await asyncio.sleep(2)


async def preencher_calendarios(page, mes: str, ctx: FlowContext) -> None:
    mes_num, ano = map(int, mes.split("/"))
    await log_flow(
        ctx,
        f"Preenchendo calendários de competência: {mes}",
        event="STEP_DETAIL",
    )
    await page.wait_for_selector(".rich-calendar-tool-btn", timeout=15000)
    # data inicial
    await selecionar_mes_especifico(
        page,
        btn_selector=".rich-calendar-tool-btn",
        prefixo="manterEscrituracaoForm:dataInicial",
        mes_num=mes_num,
        ano=ano,
    )
    # data final
    await page.wait_for_selector(
        "#manterEscrituracaoForm\\:dataFinal .rich-calendar-tool-btn",
        timeout=15000,
    )
    await selecionar_mes_especifico(
        page,
        btn_selector="#manterEscrituracaoForm\\:dataFinal .rich-calendar-tool-btn",
        prefixo="manterEscrituracaoForm:dataFinal",
        mes_num=mes_num,
        ano=ano,
    )


async def clicar_consultar(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Clicando em Consultar escrituração", event="STEP_DETAIL")
    await page.click("#manterEscrituracaoForm\\:btnConsultar")
    await asyncio.sleep(2)


async def clicar_escriturar(page, ctx: FlowContext) -> tuple[bool, bool]:
    """
    Tenta escriturar; se estiver desabilitado, reabre antes.
    Retorna (ja_escriturado, reabriu_agora)
    """
    await log_flow(ctx, "Escriturar/Reabrir", event="STEP_DETAIL")
    reabriu = False
    desab = await page.query_selector("span[id$=':linkEscriturarDesabilitado']")
    if desab:
        # clicar reabrir
        await page.click("a[id$=':linkReabrir']")
        await asyncio.sleep(2)
        reabriu = True
    # clicar escriturar
    await page.click("a[id$=':linkEscriturar']")
    await asyncio.sleep(2)
    return (desab is not None), reabriu


async def aba_servicos_existe(page) -> bool:
    return bool(await page.query_selector("a[id$=':abaServicosPendentes']")) or bool(
        await page.query_selector_all("#aba_servicos_pendentes_shifted")
    )


async def clicar_aba_servicos_pendentes(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Acessando aba Serviços Pendentes", event="STEP_DETAIL")
    try:
        await page.click("#aba_servicos_pendentes_shifted")
        await page.wait_for_selector("form#servicos_pendentes_form", timeout=25000)
    except Exception:
        await page.click("a[id$=':abaServicosPendentes']")
        await page.wait_for_selector("form#servicos_pendentes_form", timeout=25000)


async def aceitar_todos_servicos_pendentes(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Aceitando serviços pendentes", event="STEP_DETAIL")
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    botoes = [
        (
            "Tomados",
            "#servicos_pendentes_form\\:idLinkaceitarDocTomados",
            "#aceite_todos_doc_tomados_modal_panel_form\\:btnSim",
        ),
        (
            "Prestados",
            "#servicos_pendentes_form\\:idLinkaceitarDocPrestados",
            "#aceite_todos_doc_prestados_modal_panel_form\\:btnSim",
        ),
    ]
    for nome, link, confirmar in botoes:
        try:
            await page.wait_for_selector(link, timeout=20000)
            await page.click(link)
            await asyncio.sleep(2)
            await page.click(confirmar)
            await asyncio.sleep(2)
        except Exception:
            # botão pode não existir, ok
            pass


async def aba_simples_existe(page) -> bool:
    return bool(
        await page.query_selector("#abaEspelhoSimplesNacional_shifted")
    ) or bool(await page.query_selector("a[id$=':abaEspelhoSimplesNacional']"))


async def clicar_aba_simples(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Acessando aba Simples Nacional", event="STEP_DETAIL")
    try:
        await page.click("#abaEspelhoSimplesNacional_shifted")
    except Exception:
        await page.click("a[id$=':abaEspelhoSimplesNacional']")
    await asyncio.sleep(2.5)


async def extrair_simples(page, pasta_empresa: str, ctx: FlowContext) -> None:
    await log_flow(ctx, "Extraindo HTML do espelho Simples Nacional", event="STEP_DETAIL")
    sel = "#abaEspelhoSimplesNacionalForm\\:dataTableEspelhoSimplesNacional"
    await page.wait_for_selector(sel, timeout=30000)
    os.makedirs(pasta_empresa, exist_ok=True)
    html = await page.content()
    path = os.path.join(pasta_empresa, "espelho_simples.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


async def gerar_pdf_simples_via_screenshot(
    page, pasta_empresa: str, ctx: FlowContext
) -> None:
    await log_flow(ctx, "Gerando PDF (screenshot) do Simples Nacional", event="STEP_DETAIL")
    await page.wait_for_selector("form#abaEspelhoSimplesNacionalForm", timeout=30000)
    os.makedirs(pasta_empresa, exist_ok=True)
    png_path = os.path.join(pasta_empresa, "segregacao_receitas.png")
    pdf_path = os.path.join(pasta_empresa, "segregacao_receitas.pdf")
    form = await page.query_selector("form#abaEspelhoSimplesNacionalForm")
    await form.screenshot(path=png_path)
    try:
        from PIL import Image

        Image.open(png_path).convert("RGB").save(pdf_path)
    except Exception:
        # se não tiver PIL, mantém PNG mesmo
        pdf_path = png_path
    finally:
        if os.path.exists(png_path) and pdf_path != png_path:
            try:
                os.remove(png_path)
            except Exception:
                pass


async def voltar_encerramento(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Voltando para aba Encerramento", event="STEP_DETAIL")
    try:
        await page.click("#abaEncerramento_shifted")
    except Exception:
        await page.click("a[id$=':abaEncerramento']")
    await asyncio.sleep(2.5)
    await page.wait_for_selector(
        "#abaEncerramentoForm\\:btnEncerrarEscrituracao", timeout=30000
    )


async def clicar_encerrar(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Clicando em Encerrar escrituração", event="STEP_DETAIL")
    await page.click("#abaEncerramentoForm\\:btnEncerrarEscrituracao")
    await asyncio.sleep(2.5)


async def extrair_confirmacao(page, pasta_empresa: str, ctx: FlowContext) -> None:
    await log_flow(ctx, "Extraindo HTML de confirmação de encerramento", event="STEP_DETAIL")
    await page.wait_for_selector("div.panel-body table.table", timeout=40000)
    os.makedirs(pasta_empresa, exist_ok=True)
    html = await page.content()
    path = os.path.join(pasta_empresa, "confirmacao_encerramento.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


async def clicar_sim(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Confirmando encerramento (Sim)", event="STEP_DETAIL")
    await page.click("#formEncerramento\\:btnSim")
    await asyncio.sleep(2)


async def clicar_certificado_encerramento(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Abrindo certificado de encerramento", event="STEP_DETAIL")
    btn_id = await page.evaluate(
        """
      () => {
        const els = Array.from(document.querySelectorAll('input[id][value]'));
        const el = els.find(e => (e.value || '').includes('Certificado de Encerramento'));
        return el ? el.id : null;
      }
    """
    )
    if not btn_id:
        raise FlowError(
            "CERT_BUTTON_NOT_FOUND",
            "Botão de certificado de encerramento não localizado.",
            retryable=False,
        )
    await page.evaluate("(id) => document.getElementById(id).click()", btn_id)
    await asyncio.sleep(2.5)
    await page.wait_for_load_state("networkidle")


async def gerar_pdf_certificado(page, pasta_empresa: str, ctx: FlowContext) -> None:
    await log_flow(ctx, "Gerando PDF do certificado de encerramento", event="STEP_DETAIL")
    # simplifica o DOM para imprimir apenas o certificado
    await page.evaluate(
        """
      let principal = document.getElementById('docPrincipal')?.outerHTML;
      if (principal) document.body.innerHTML = principal;
    """
    )
    os.makedirs(pasta_empresa, exist_ok=True)
    out = os.path.join(pasta_empresa, "certificado.pdf")
    await page.pdf(
        path=out,
        format="A4",
        margin={"top": "20px", "bottom": "20px", "left": "20px", "right": "20px"},
        scale=0.9,
        print_background=True,
    )


# ===================== Exportação =====================

async def acessar_exportacao(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Acessando tela de exportação de escrituração", event="STEP_DETAIL")
    app = get_app_module()
    resilient = getattr(app, "resilient_goto")
    await resilient(
        page,
        "https://iss.fortaleza.ce.gov.br/grpfor/pages/escrituracao/exportarEscrituracao.seam",
    )
    await asyncio.sleep(2)


async def preencher_competencia_exportacao(
    page, mes: str, ctx: FlowContext
) -> None:
    await log_flow(ctx, f"Selecionando competência para exportação: {mes}", event="STEP_DETAIL")
    mes_num, ano = map(int, mes.split("/"))
    btn_selector = "#exportarEscrituracaoForm\\:competenciaHeader .rich-calendar-tool-btn"
    await page.wait_for_selector(btn_selector, timeout=20000)
    await page.click(btn_selector)
    await asyncio.sleep(2)

    ano_selector_base = "#exportarEscrituracaoForm\\:competenciaDateEditorLayoutY"
    for i in range(10):
        div = await page.query_selector(f"{ano_selector_base}{i}")
        if div and (await div.inner_text()).strip() == str(ano):
            await div.click()
            await asyncio.sleep(2)
            break

    mes_selector = (
        f"#exportarEscrituracaoForm\\:competenciaDateEditorLayoutM{mes_num-1}"
    )
    await page.click(mes_selector)
    ok_selector = "#exportarEscrituracaoForm\\:competenciaDateEditorButtonOk"
    await page.wait_for_selector(ok_selector, timeout=5005)
    await page.click(ok_selector)
    await page.wait_for_selector(
        "#exportarEscrituracaoForm\\:competenciaEditor",
        state="hidden",
        timeout=5000,
    )


async def clicar_gerar_exportacao(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Clicando em Gerar exportação", event="STEP_DETAIL")
    await page.click("#exportarEscrituracaoForm\\:btnGerar")
    await asyncio.sleep(2)


async def baixar_exportacao(page, cnpj: str, pasta_empresa: str, ctx: FlowContext) -> None:
    await log_flow(ctx, "Baixando arquivo de exportação", event="STEP_DETAIL")
    os.makedirs(pasta_empresa, exist_ok=True)
    btn_selector = "#exportarEscrituracaoForm\\:fileButton"
    nenhum_selector = "#nenhumText"
    try:
        await page.wait_for_selector(btn_selector, state="visible", timeout=60000)
        async with page.expect_download() as download_info:
            await page.click(btn_selector)
        download = await download_info.value
        destino = os.path.join(pasta_empresa, f"exportacao_{cnpj}.xls")
        await download.save_as(destino)
    except Exception:
        # Se não encontrou botão, possivelmente "nenhum" — não é falha fatal.
        if await page.query_selector(nenhum_selector):
            logger.info("Nenhum arquivo de exportação disponível para a competência.")
        else:
            raise


# ===================== Orquestração: run_cnpj =====================

async def run_cnpj(task, flow_ctx: FlowContext) -> dict:
    """
    Executa todo o fluxo de escrituração para 1 TaskLike.
    Usa o runner/timeout padronizado de flow_core.
    """
    app = get_app_module()

    browser_get = getattr(app, "browser_get")
    BASE_DIR = getattr(app, "BASE_DIR", ".")
    DEF_TIMEOUT = getattr(app, "DEF_TIMEOUT_MS", 30000)
    NAV_TIMEOUT = getattr(app, "NAV_TIMEOUT_MS", 60000)
    PER_STEP = getattr(app, "PER_STEP_TIMEOUT_SEC", 120)

    colab_dirname = task.colaborador_norm
    cnpj = task.cnpj
    mes = task.mes

    # Telemetria global
    try:
        app.current_state["cnpj_aberto"] = cnpj
        app.current_state["fase"] = "escrituracao_iniciando"
        app.current_state["escrituracao_reaberta"] = False
        app.current_state["passou_encerramento"] = False
    except Exception:
        pass

    await log_flow(flow_ctx, "=== INÍCIO (Escrituração) ===", event="FLOW_START")

    context = None
    try:
        # Browser resiliente via flow_core
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

        # Info auxiliar em main.py (se quiser usar depois)
        try:
            app._ESCRIT_CNPJ = cnpj
            app._ESCRIT_PASTA_EMPRESA = pasta_empresa
        except Exception:
            pass

        # 3) Acessar tela de escrituração
        await run_step(
            flow_ctx,
            "Acessar Escrituração",
            acessar_escrituracao(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        # 4) Calendários + consulta
        await run_step(
            flow_ctx,
            "Preencher Calendários",
            preencher_calendarios(page, mes, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Consultar",
            clicar_consultar(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        # 5) Escriturar / Reabrir
        ja_esc, reabriu_agora = await run_step(
            flow_ctx,
            "Escriturar/Reabrir",
            clicar_escriturar(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        try:
            app.current_state["escrituracao_reaberta"] = bool(reabriu_agora)
        except Exception:
            pass

        # 6) Serviços pendentes (opcional)
        if await aba_servicos_existe(page):
            await run_step(
                flow_ctx,
                "Aba Serviços (opcional)",
                clicar_aba_servicos_pendentes(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )
            await run_step(
                flow_ctx,
                "Aceitar Pendentes (opcional)",
                aceitar_todos_servicos_pendentes(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )
            await run_step(
                flow_ctx,
                "Voltar Encerramento",
                voltar_encerramento(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )
        else:
            # garante que está em Encerramento
            await run_step(
                flow_ctx,
                "Voltar Encerramento",
                voltar_encerramento(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )

        # 7) Simples Nacional (opcional)
        if await aba_simples_existe(page):
            await run_step(
                flow_ctx,
                "Aba Simples (opcional)",
                clicar_aba_simples(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )
            await run_step(
                flow_ctx,
                "Extrair Simples",
                extrair_simples(page, pasta_empresa, flow_ctx),
                per_step_timeout=PER_STEP,
            )
            await run_step(
                flow_ctx,
                "Gerar PDF Simples",
                gerar_pdf_simples_via_screenshot(page, pasta_empresa, flow_ctx),
                per_step_timeout=PER_STEP,
            )
            await run_step(
                flow_ctx,
                "Voltar Encerramento",
                voltar_encerramento(page, flow_ctx),
                per_step_timeout=PER_STEP,
            )

        # 8) Encerrar + confirmação
        await run_step(
            flow_ctx,
            "Encerrar",
            clicar_encerrar(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Extrair Confirmação",
            extrair_confirmacao(page, pasta_empresa, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Confirmar Sim",
            clicar_sim(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        try:
            app.current_state["passou_encerramento"] = True
        except Exception:
            pass

        # 9) Certificado
        await run_step(
            flow_ctx,
            "Certificado",
            clicar_certificado_encerramento(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "PDF Certificado",
            gerar_pdf_certificado(page, pasta_empresa, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        # 10) Exportação
        await run_step(
            flow_ctx,
            "Exportação: Acessar",
            acessar_exportacao(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Exportação: Competência",
            preencher_competencia_exportacao(page, mes, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Exportação: Gerar",
            clicar_gerar_exportacao(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Exportação: Baixar",
            baixar_exportacao(page, cnpj, pasta_empresa, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        await log_flow(
            flow_ctx,
            "=== FIM (Escrituração OK) ===",
            event="FLOW_END",
        )

        return {
            "cnpj": cnpj,
            "status": "ok",
            "empresa": nome_empresa,
            "pasta": pasta_empresa,
        }

    except FlowError as e:
        # Erros de domínio conhecidos (sem retry automático aqui;
        # quem decide é quem chamou o job / fila).
        await log_flow(
            flow_ctx,
            f"Erro de domínio na escrituração: {e.message}",
            level=logging.ERROR,
            event="ERROR",
            code=e.code,
        )
        raise

    except Exception as e:
        await log_flow(
            flow_ctx,
            f"Erro inesperado no fluxo de escrituração: {e}",
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

async def job_escrituracao(ctx, colaborador, cnpj, mes, usuario, senha, regime):
    """
    Entry point do ARQ para escrituração.

    ctx: dict de contexto do ARQ (job_id, job_try, etc.).
    """
    logger.info(
        f"[job_escrituracao] colaborador={colaborador}, cnpj={cnpj}, mes={mes}, regime={regime}"
    )

    app = get_app_module()

    # Normaliza CNPJ
    somente_digitos = getattr(app, "somente_digitos")
    cnpj_norm = somente_digitos(cnpj).zfill(14)

    # “convencional” ou “dominio” (mantém compatibilidade de assinatura)
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

    # Telemetria básica
    try:
        app.current_state["fase"] = "escrituracao"
        app.current_state["cnpj_aberto"] = cnpj_norm
    except Exception:
        pass

    # Metadados do job (se existirem)
    job_id = ctx.get("job_id")
    job_try = ctx.get("job_try")

    flow_ctx = FlowContext(
        flow="escrituracao",
        colab=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        job_id=job_id,
        job_try=job_try,
    )

    await log_flow(flow_ctx, "job_escrituracao iniciado", event="EVENT")

    result = await run_cnpj(task, flow_ctx)

    await log_flow(flow_ctx, "job_escrituracao finalizado", event="EVENT")

    return result
