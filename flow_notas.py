#!/usr/bin/env python3
# flow_notas.py — Flow de NFS-e (Notas) usando flow_core

import os
import re
import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

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
    log_flow_sync,
    get_app_module,
)

logger = logging.getLogger("iss-automacao-notas")


# ===================== Namespaces e util XML =====================

NFE_NS_DEFAULT = "http://www.ginfes.com.br/tipos"
DS_NS = "http://www.w3.org/2000/09/xmldsig#"
NS = {
    "": NFE_NS_DEFAULT,
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "ds": DS_NS,
}
ET.register_namespace("", NS[""])
ET.register_namespace("xsi", NS["xsi"])


def remover_canceladas_xml(path_xml: str, cnpj_log: Optional[str] = None) -> None:
    """
    Remove blocos <Nfse> ou <ds:Nfse> que contenham <Cancelamento>
    (compatível Fortaleza/Eusébio).
    """
    try:
        tree = ET.parse(path_xml)
        root = tree.getroot()
    except ET.ParseError as e:
        logger.warning(f"{cnpj_log or ''} - XML inválido ao tentar remover canceladas: {e}")
        return

    removidas = 0

    # percorre todos os filhos diretos do root (geralmente ns2:Nfse)
    for bloco in list(root):
        tag_sem_ns = bloco.tag.split("}")[-1]
        if tag_sem_ns.lower() != "nfse":
            continue

        # Verifica se há Cancelamento como filho direto OU em qualquer lugar do bloco
        cancel = bloco.find(".//Cancelamento")
        if cancel is None:
            for child in bloco:
                if child.tag.split("}")[-1].lower() == "cancelamento":
                    cancel = child
                    break

        if cancel is not None:
            num_el = bloco.find(".//Numero")
            num = num_el.text if num_el is not None else "???"
            logger.info(f"{cnpj_log or ''} - Removendo NFS-e CANCELADA: {num}")
            root.remove(bloco)
            removidas += 1

    if removidas:
        tree.write(path_xml, encoding="utf-8", xml_declaration=True)
        logger.info(f"{cnpj_log or ''} - {removidas} notas canceladas removidas.")
    else:
        logger.info(f"{cnpj_log or ''} - Nenhuma NFS-e cancelada encontrada.")


# ===================== Ações ISS — login / empresa =====================

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
        raise LoginError(msg)

    # Modal "Deseja manter logado?" → Não
    try:
        await page.wait_for_selector("form[id='j_id461'] input[type='submit'][value='Não']", timeout=3000)
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
      - FlowError genérico se não conseguir selecionar.
    """
    app = get_app_module()
    resilient_goto = getattr(app, "resilient_goto")
    somente_digitos = getattr(app, "somente_digitos")
    limpar_nome = getattr(app, "limpar_nome")

    logger.info(f"Pesquisando empresa {cnpj}")
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
                # salva HTML para debug
                html = await page.content()
                mesano = mes.replace("/", ".")
                err_dir = os.path.join(base_dir, colaborador_norm, mesano)
                os.makedirs(err_dir, exist_ok=True)
                path = os.path.join(err_dir, f"{cnpj}_erro_tentativa{tentativa}.html")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.warning(f"   → Tentativa {tentativa}: HTTP {status}. HTML salvo em {path}")
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
            logger.warning(f"   → Tentativa {tentativa} falhou: {e}")
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
        # sem mensagem = segue o baile
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


# ===================== NFS-e (menus/abas/consulta) =====================

async def acessar_menu_nfse_consulta(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Acessando NFS-e → Consultar", event="STEP_DETAIL")
    await page.click("a.dropdown-toggle:has-text('NFS-e')")
    await page.click("a:has-text('Consultar NFS-e')")
    await page.wait_for_selector("form[id^='consultarnfseForm']", timeout=15_000)
    await asyncio.sleep(1.2)


async def clicar_aba_competencia_tomador(page, tipo, ctx):
    await log_flow(ctx, f"Aba Competência/Tomador ({tipo})", event="STEP_DETAIL")

    await esperar_overlay_sumir(page)

    aba_id = (
        "#consultarnfseForm\\:competencia_prestador_tab_lbl"
        if tipo == "prestadas"
        else "#consultarnfseForm\\:abaPorCompetenciaTomador_tab_lbl"
    )

    aba = await page.wait_for_selector(aba_id, timeout=20_000)

    cls = (await aba.get_attribute("class")) or ""
    if "rich-tab-active" not in cls:
        await aba.click()
        await esperar_overlay_sumir(page)
        await asyncio.sleep(1.0)



async def selecionar_mes_especifico_calendario(
    page,
    tipo: str,
    mes_num: int,
    ano: int,
    ctx: FlowContext,
) -> None:
    await log_flow(ctx, f"Selecionando competência {mes_num:02}/{ano} ({tipo})", event="STEP_DETAIL")

    prefixo = "competencia" if tipo == "prestadas" else "competenciaTomador"
    await page.click(".rich-calendar-tool-btn")
    await asyncio.sleep(1.8)

    # escolhe ano
    for i in range(10):
        sel_year = f"#consultarnfseForm\\:{prefixo}DateEditorLayoutY{i}"
        div = await page.query_selector(sel_year)
        if div and (await div.inner_text()).strip() == str(ano):
            await div.click()
            await asyncio.sleep(1.3)
            break

    # escolhe mês
    await page.click(f"#consultarnfseForm\\:{prefixo}DateEditorLayoutM{mes_num - 1}")
    await page.click(f"#consultarnfseForm\\:{prefixo}DateEditorButtonOk")
    await asyncio.sleep(1.2)


async def esperar_overlay_sumir(page, timeout: int = 20_000) -> None:
    """
    Espera o overlay #mpProgressoDiv sumir (usado pelo JSF/RichFaces).
    Robusto contra ausência do elemento.
    """
    try:
        for _ in range(int(timeout / 500)):
            try:
                visible = await page.is_visible("#mpProgressoDiv")
                if not visible:
                    return
            except Exception:
                return
            await asyncio.sleep(0.5)
    except Exception:
        pass


async def clicar_botao_consultar(page, ctx: FlowContext) -> None:
    await log_flow(ctx, "Clicando em Consultar", event="STEP_DETAIL")
    await esperar_overlay_sumir(page)
    await page.click("input[value='Consultar']")
    await esperar_overlay_sumir(page)
    await asyncio.sleep(1.2)


# ===================== Download das páginas (XML) =====================

async def baixar_paginas(
    page,
    tipo: str,
    nome_empresa: str,
    estrutura_dominio: bool,
    pasta_mesano: str,
    pasta_empresa: str,
    cnpj: str,
    ctx: FlowContext,
    codigo_dominio: Optional[str] = None,
) -> bool:
    """
    Percorre a paginação da consulta e baixa os XMLs em lotes (100 linhas).

    Quando estrutura_dominio=True:
      - pasta = <pasta_mesano>/importar/<tipo>/<codigoDominio>-<NomeEmpresaNormalizado>
        (se codigo_dominio for None, cai no CNPJ como fallback)
    """
    await log_flow(ctx, f"Iniciando download de notas {tipo}", event="STEP_DETAIL")

    # 1) Sem registros?
    msg = await page.query_selector("span.rich-messages-label")
    if msg and "Nenhum registro foi encontrado" in (await msg.inner_text()).strip():
        logger.info(f"{cnpj} - Sem notas {tipo}.")
        await log_flow(ctx, f"Sem notas {tipo} para este CNPJ", event="INFO")
        return False

    # 2) Pasta destino
    if estrutura_dominio:
        codigo = codigo_dominio or cnpj
        nome_emp = re.sub(r"[^A-Za-z0-9_\-]", "_", nome_empresa.strip().replace(" ", "_"))
        pasta = os.path.join(pasta_mesano, "importar", tipo, f"{codigo}-{nome_emp}")
    else:
        pasta = os.path.join(pasta_empresa, tipo)
    os.makedirs(pasta, exist_ok=True)

    # 3) Descobre total de páginas
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(0.5)
    tds = await page.query_selector_all(
        "#consultarnfseForm\\:dataTable\\:j_id374_table td.rich-datascr-inact, td.rich-datascr-act"
    )
    paginas = [
        int((await td.inner_text()).strip())
        for td in tds
        if (await td.inner_text()).strip().isdigit()
    ]
    total_paginas = max(paginas) if paginas else 1
    logger.debug(f"{cnpj} - Total de páginas detectadas: {total_paginas}")

    pagina = 1
    lote = 1
    acumulado = 0
    baixou_algo = False

    # Loop pelas páginas
    while True:
        logger.debug(f"{cnpj} - Entrando na página {pagina}/{total_paginas}")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(0.5)

        # Conta linhas da página
        rows = await page.query_selector_all("#consultarnfseForm\\:dataTable\\:tb tr")
        qtd = len(rows)
        if qtd == 0:
            logger.info(f"{cnpj} - Página {pagina} sem linhas, pulando.")
        else:
            await esperar_overlay_sumir(page)
            btn_all = await page.query_selector("input#consultarnfseForm\\:j_id324")
            if btn_all:
                logger.debug(f"{cnpj} - Selecionando {qtd} notas na página {pagina}.")
                await esperar_overlay_sumir(page)
                try:
                    await btn_all.click()
                except Exception as e:
                    logger.warning(f"{cnpj} - Erro ao clicar em 'Selecionar todas': {e}")
                    await asyncio.sleep(1.0)
                    await esperar_overlay_sumir(page)
                    btn_all = await page.query_selector("input#consultarnfseForm\\:j_id324")
                    if btn_all:
                        await btn_all.click()
                await esperar_overlay_sumir(page)
                await asyncio.sleep(1.0)
            else:
                logger.warning(f"{cnpj} - [Página {pagina}] botão 'Selecionar todas' não encontrado.")

            acumulado += qtd
            baixou_algo = True
            logger.info(f"{cnpj} - Página {pagina}: {qtd} notas (acumulado={acumulado})")

        # Detecta se há próxima página
        next_btn = await page.query_selector(
            f"td.rich-datascr-button:not(.rich-datascr-button-dsbld):has-text('{pagina+1}')"
        ) or await page.query_selector("td.rich-datascr-button[onclick*=\"'next'\"]")
        ultima = next_btn is None

        # Se acumulou 100 OU última página com algo acumulado, exporta
        if acumulado >= 100 or (ultima and acumulado > 0):
            nome_arquivo = (
                f"{datetime.now():%Y-%m}_lote{lote:02}.xml"
                if not estrutura_dominio
                else f"nota_{lote}.xml"
            )
            destino = os.path.join(pasta, nome_arquivo)

            await esperar_overlay_sumir(page)
            export_btn = await page.query_selector("input[name='consultarnfseForm:j_id325']")
            if not export_btn:
                logger.error(f"{cnpj} - Botão 'Exportar XML' não encontrado no lote {lote}.")
                await log_flow(
                    ctx,
                    f"Botão 'Exportar XML' não encontrado no lote {lote}",
                    level=logging.ERROR,
                    event="ERROR",
                    code="EXPORT_BUTTON_MISSING",
                )
                return baixou_algo

            logger.info(f"{cnpj} - Exportando lote {lote} ({acumulado} notas)...")
            await log_flow(
                ctx,
                f"Exportando lote {lote} ({acumulado} notas)...",
                event="STEP_DETAIL",
            )

            async with page.expect_download() as dl_info:
                await esperar_overlay_sumir(page)
                try:
                    await export_btn.click()
                except Exception as e:
                    logger.warning(
                        f"{cnpj} - Erro ao clicar em 'Exportar XML': {e}, tentando novamente em 2s..."
                    )
                    await asyncio.sleep(2)
                    await esperar_overlay_sumir(page)
                    export_btn = await page.query_selector("input[name='consultarnfseForm:j_id325']")
                    if export_btn:
                        await export_btn.click()

            download = await dl_info.value
            await download.save_as(destino)
            logger.info(f"{cnpj} - XML salvo em: {destino}")

            if tipo == "tomadas":
                remover_canceladas_xml(destino, cnpj_log=cnpj)

            acumulado = 0
            lote += 1

        if ultima:
            logger.info(f"{cnpj} - Processamento concluído em {pagina}/{total_paginas}.")
            break

        pagina += 1
        await next_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1.8)

    return baixou_algo


# -------------------- Helper interno: selecionar tipo + consultar --------------------

async def _selecionar_tipo_consultar(
    page,
    tipo: str,
    mes_num: int,
    ano: int,
    ctx: FlowContext,
) -> None:
    """
    Seleciona o tipo (prestadas/tomadas), define competência e dispara a consulta.
    """
    texto = "Serviços Prestados" if tipo == "prestadas" else "Serviços Tomados"
    await log_flow(ctx, f"Seleção de tipo: {texto}", event="STEP_DETAIL")

    await page.wait_for_selector(f"label:has-text('{texto}')", timeout=15_000)
    await page.locator(f"label:has-text('{texto}')").click()
    await asyncio.sleep(1.8)

    await clicar_aba_competencia_tomador(page, tipo, ctx)
    await selecionar_mes_especifico_calendario(page, tipo, mes_num, ano, ctx)
    await clicar_botao_consultar(page, ctx)


# ===================== Orquestração: run_cnpj =====================

async def run_cnpj(task, flow_ctx: FlowContext) -> dict:
    """
    Executa o fluxo de Notas para um TaskLike compatível com a fila/ARQ:
      - task.colaborador_norm
      - task.cnpj
      - task.mes  (MM/AAAA)
      - task.usuario
      - task.senha
      - task.tipo_estrutura (opcional: "dominio" ou "convencional")
      - task.codigo_dominio (opcional; usado em estrutura_domínio)
    """
    app = get_app_module()

    browser_get = getattr(app, "browser_get")
    BASE_DIR = getattr(app, "BASE_DIR", ".")
    DEF_TIMEOUT = getattr(app, "DEF_TIMEOUT_MS", 120_000)
    NAV_TIMEOUT = getattr(app, "NAV_TIMEOUT_MS", 90_000)
    PER_STEP = getattr(app, "PER_STEP_TIMEOUT_SEC", 300)

    colab_dirname = task.colaborador_norm
    cnpj = task.cnpj
    mes = task.mes
    estrutura_dominio = str(getattr(task, "tipo_estrutura", "convencional")).lower().startswith("dom")
    codigo_dominio = getattr(task, "codigo_dominio", None)

    # Telemetria global em main.py
    try:
        app.current_state["cnpj_aberto"] = cnpj
        app.current_state["fase"] = "iniciando"
    except Exception:
        pass

    await log_flow(flow_ctx, "=== INÍCIO (Notas) ===", event="FLOW_START")

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

        # Info auxiliar (se alguém quiser em outro lugar)
        try:
            app._NOTAS_CNPJ = cnpj
            app._NOTAS_PASTA_EMPRESA = pasta_empresa
        except Exception:
            pass

        # 3) Ir para tela de consulta de NFS-e
        await run_step(
            flow_ctx,
            "NFS-e: Consultar",
            acessar_menu_nfse_consulta(page, flow_ctx),
            per_step_timeout=PER_STEP,
        )

        mes_num, ano = map(int, mes.split("/"))

        # 4) Prestadas
        await run_step(
            flow_ctx,
            "Prestadas: selecionar/consultar",
            _selecionar_tipo_consultar(page, "prestadas", mes_num, ano, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Prestadas: baixar",
            baixar_paginas(
                page,
                "prestadas",
                nome_empresa,
                estrutura_dominio,
                pasta_mesano,
                pasta_empresa,
                cnpj,
                flow_ctx,
                codigo_dominio,
            ),
            per_step_timeout=PER_STEP,
        )

        # 5) Tomadas
        await run_step(
            flow_ctx,
            "Tomadas: selecionar/consultar",
            _selecionar_tipo_consultar(page, "tomadas", mes_num, ano, flow_ctx),
            per_step_timeout=PER_STEP,
        )
        await run_step(
            flow_ctx,
            "Tomadas: baixar",
            baixar_paginas(
                page,
                "tomadas",
                nome_empresa,
                estrutura_dominio,
                pasta_mesano,
                pasta_empresa,
                cnpj,
                flow_ctx,
                codigo_dominio,
            ),
            per_step_timeout=PER_STEP,
        )

        await log_flow(
            flow_ctx,
            "=== FIM (Notas OK) ===",
            event="FLOW_END",
        )

        return {
            "cnpj": cnpj,
            "status": "ok",
            "empresa": nome_empresa,
            "pasta": pasta_empresa,
        }

    except FlowError as e:
        # Erros de domínio conhecidos (sem retry automático aqui).
        await log_flow(
            flow_ctx,
            f"Erro de domínio: {e.message}",
            level=logging.ERROR,
            event="ERROR",
            code=e.code,
        )
        raise

    except Exception as e:
        await log_flow(
            flow_ctx,
            f"Erro inesperado no fluxo de notas: {e}",
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

async def job_notas(ctx, colaborador, cnpj, mes, usuario, senha, regime, codigo_dominio=None):
    """
    Entry point do ARQ para notas.

    ctx: dict de contexto do ARQ (job_id, job_try, etc.).
    codigo_dominio: opcional; quando informado e regime for "dominio",
                    será usado no nome da pasta (codigoDominio-nomeEmpresa).
    """
    logger.info(
        f"[job_notas] colaborador={colaborador}, cnpj={cnpj}, mes={mes}, regime={regime}, codigo_dominio={codigo_dominio}"
    )

    app = get_app_module()

    # Normaliza CNPJ
    somente_digitos = getattr(app, "somente_digitos")
    cnpj_norm = somente_digitos(cnpj).zfill(14)

    # O painel envia “convencional” ou “dominio”
    tipo_estrutura = str(regime or "convencional").lower()
    if tipo_estrutura.startswith("dom"):
        tipo_estrutura = "dominio"
    else:
        tipo_estrutura = "convencional"

    # O colaborador DEVE ser normalizado, pois será nome de pasta
    normalizar = getattr(app, "normalizar_nome_colaborador")
    colaborador_norm = normalizar(colaborador)

    class TaskLike:
        __slots__ = (
            "colaborador_norm",
            "cnpj",
            "mes",
            "usuario",
            "senha",
            "tipo_estrutura",
            "codigo_dominio",
        )

        def __init__(self, colaborador_norm, cnpj, mes, usuario, senha, tipo_estrutura, codigo_dominio=None):
            self.colaborador_norm = colaborador_norm
            self.cnpj = cnpj
            self.mes = mes
            self.usuario = usuario
            self.senha = senha
            self.tipo_estrutura = tipo_estrutura
            self.codigo_dominio = codigo_dominio

    task = TaskLike(
        colaborador_norm=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        usuario=usuario,
        senha=senha,
        tipo_estrutura=tipo_estrutura,
        codigo_dominio=codigo_dominio,
    )

    # Alimenta current_state básico
    try:
        app.current_state["fase"] = "notas"
        app.current_state["cnpj_aberto"] = cnpj_norm
    except Exception:
        pass

    # Extrai metadados do job (se existirem no ctx)
    job_id = ctx.get("job_id")
    job_try = ctx.get("job_try")

    flow_ctx = FlowContext(
        flow="notas",
        colab=colaborador_norm,
        cnpj=cnpj_norm,
        mes=mes,
        job_id=job_id,
        job_try=job_try,
    )

    await log_flow(flow_ctx, "job_notas iniciado", event="EVENT")

    result = await run_cnpj(task, flow_ctx)

    await log_flow(flow_ctx, "job_notas finalizado", event="EVENT")

    return result
