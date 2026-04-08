import fs from "fs/promises";
import { createClient } from "@supabase/supabase-js";

const { chromium } = await import("playwright");

const TIMEOUT_CURTO = 8000;
const TIMEOUT_MEDIO = 15000;
const TIMEOUT_LONGO = 35000;
const TIMEOUT_MUITO_LONGO = 60000;
const TIMEOUT_EMISSAO_FINAL = 120000;

const STORAGE_BUCKET = "nfse-files";

const supabaseAdmin = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL || "",
  process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

function limparDocumento(valor) {
  return String(valor || "").replace(/\D/g, "");
}

function formatarDataParaPortal(dataIso) {
  if (!dataIso) return "";
  const partes = dataIso.split("-");
  if (partes.length === 3) {
    const [ano, mes, dia] = partes;
    return `${dia}/${mes}/${ano}`;
  }
  return dataIso;
}

function normalizarTexto(valor) {
  return String(valor || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function normalizarMunicipioParaPortal(valor) {
  return String(valor || "")
    .replace(/\s*-\s*/g, "/")
    .replace(/\s*\/\s*/g, "/")
    .replace(/\s+/g, " ")
    .trim();
}

function extrairNomeMunicipio(valor) {
  const normalizado = normalizarMunicipioParaPortal(valor);
  return normalizado.split("/")[0]?.trim() || normalizado;
}

function extrairUfMunicipio(valor) {
  const normalizado = normalizarMunicipioParaPortal(valor);
  const partes = normalizado.split("/");
  return (partes[1] || "").trim().toLowerCase();
}

function formatarValorParaDigitacaoNoPortal(valor) {
  if (typeof valor === "number") {
    if (!Number.isFinite(valor)) {
      throw new Error(`Valor do serviço inválido: ${valor}`);
    }
    return valor.toFixed(2).replace(".", ",");
  }

  const bruto = String(valor ?? "").trim();

  if (!bruto) {
    throw new Error("Valor do serviço não informado.");
  }

  const possuiVirgula = bruto.includes(",");
  const possuiPonto = bruto.includes(".");

  let numero;

  if (possuiVirgula && possuiPonto) {
    numero = Number(bruto.replace(/\./g, "").replace(",", "."));
  } else if (possuiVirgula) {
    numero = Number(bruto.replace(",", "."));
  } else {
    numero = Number(bruto);
  }

  if (!Number.isFinite(numero)) {
    throw new Error(`Valor do serviço inválido: ${valor}`);
  }

  return numero.toFixed(2).replace(".", ",");
}

function erroEhTransitorio(erro) {
  const msg = String(
    erro instanceof Error ? erro.message : erro || ""
  ).toLowerCase();

  const errosTransitorios = [
    "timeout",
    "navigation",
    "net::",
    "waiting",
    "locator",
    "target closed",
    "browser has been closed",
    "context closed",
    "execution context was destroyed",
    "não foi possível clicar",
    "não foi possível preencher",
    "não foi possível localizar",
    "não foi possível selecionar",
    "a tela pessoas não foi identificada",
    "nenhuma opção de município apareceu",
    "nenhuma opção apareceu para o código de tributação",
    "não encontrei botão",
    "a emissão não foi confirmada na tela final",
  ];

  return errosTransitorios.some((trecho) => msg.includes(trecho));
}

async function esperarRedeEstabilizar(page, atraso = 800) {
  await page.waitForLoadState("domcontentloaded").catch(() => null);
  await page.waitForLoadState("networkidle").catch(() => null);
  await page.waitForTimeout(atraso);
}

async function esperarPaginaPronta(page, nomeEtapa, atraso = 900) {
  console.log(`Aguardando estabilização da etapa: ${nomeEtapa}`);
  await esperarRedeEstabilizar(page, atraso);
}

async function aguardarComTentativas(
  fn,
  tentativas = 3,
  esperaMs = 700,
  nome = "operação"
) {
  let ultimoErro = null;

  for (let i = 1; i <= tentativas; i++) {
    try {
      return await fn();
    } catch (error) {
      ultimoErro = error;
      console.log(`Falha na ${nome} - tentativa ${i}/${tentativas}:`, error);
      if (i < tentativas) {
        await new Promise((resolve) => setTimeout(resolve, esperaMs));
      }
    }
  }

  throw ultimoErro instanceof Error
    ? ultimoErro
    : new Error(`Falha ao executar ${nome}.`);
}

async function obterLocatorVisivel(page, selectors, timeout = TIMEOUT_MEDIO) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count().catch(() => 0);
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout });
      return locator;
    } catch {}
  }

  return null;
}

async function preencherCampoComFallback(page, selectors, valor, nomeCampo) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: TIMEOUT_LONGO });
      await locator.scrollIntoViewIfNeeded().catch(() => null);
      await locator.click({ timeout: TIMEOUT_MEDIO });
      await page.waitForTimeout(150);

      await locator.press("Control+A").catch(() => null);
      await locator.press("Meta+A").catch(() => null);
      await locator.fill("").catch(() => null);
      await page.waitForTimeout(120);

      await locator.type(valor, { delay: 35 });

      const valorAtual = await locator.inputValue().catch(() => "");
      console.log(`Campo ${nomeCampo} preenchido com seletor ${selector}:`, valorAtual);

      if (!String(valorAtual || "").trim()) {
        await locator.fill(valor).catch(() => null);
      }

      return;
    } catch {}
  }

  throw new Error(`Não foi possível preencher o campo ${nomeCampo}.`);
}

async function clicarComFallback(page, selectors, nome) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: TIMEOUT_LONGO });
      await locator.scrollIntoViewIfNeeded().catch(() => null);
      await page.waitForTimeout(150);
      await locator.click({ timeout: TIMEOUT_MEDIO, force: true });
      console.log(`Clique realizado em ${nome} com seletor: ${selector}`);
      return;
    } catch {}
  }

  throw new Error(`Não foi possível clicar em ${nome}.`);
}

async function clicarSeExistir(page, selectors, nome) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      const visivel = await locator.isVisible().catch(() => false);
      if (!visivel) continue;

      await locator.scrollIntoViewIfNeeded().catch(() => null);
      await locator.click({ timeout: TIMEOUT_CURTO, force: true });
      console.log(`Clique opcional em ${nome} com seletor: ${selector}`);
      return true;
    } catch {}
  }

  console.log(`Elemento opcional não encontrado: ${nome}`);
  return false;
}

async function esperarQualquerUm(page, selectors, timeout = TIMEOUT_MEDIO) {
  const inicio = Date.now();

  while (Date.now() - inicio < timeout) {
    for (const selector of selectors) {
      try {
        const locator = page.locator(selector).first();
        const count = await locator.count().catch(() => 0);
        if (!count) continue;

        const visivel = await locator.isVisible().catch(() => false);
        if (!visivel) continue;

        console.log("Elemento identificado:", selector);
        return selector;
      } catch {}
    }

    await page.waitForTimeout(300);
  }

  return null;
}

async function obterInputSelect2Visivel(page) {
  const inputs = page.locator("input.select2-search__field");
  const total = await inputs.count().catch(() => 0);

  for (let i = total - 1; i >= 0; i--) {
    const input = inputs.nth(i);
    const visivel = await input.isVisible().catch(() => false);
    if (visivel) return input;
  }

  return null;
}

async function abrirSelect2ECapturarBusca(page, campo, nomeCampo) {
  for (let tentativa = 1; tentativa <= 5; tentativa++) {
    await campo.scrollIntoViewIfNeeded().catch(() => null);
    await page.waitForTimeout(150);
    await campo.click({ force: true, timeout: TIMEOUT_MEDIO }).catch(() => null);
    await page.waitForTimeout(450);

    const inputBusca = await obterInputSelect2Visivel(page);
    if (inputBusca) {
      await inputBusca.waitFor({ state: "visible", timeout: TIMEOUT_MEDIO });
      return inputBusca;
    }

    console.log(
      `Tentativa ${tentativa} sem input Select2 visível em ${nomeCampo}. Reabrindo...`
    );

    await page.keyboard.press("Escape").catch(() => null);
    await page.waitForTimeout(350);
  }

  throw new Error(`Não foi possível abrir o campo Select2 de ${nomeCampo}.`);
}

async function fecharPossiveisModaisOuAvisos(page) {
  await clicarSeExistir(
    page,
    [
      'button:has-text("Fechar")',
      'button:has-text("OK")',
      'button:has-text("Ok")',
      'button:has-text("Entendi")',
      "button.btn-close",
      '[aria-label="Close"]',
    ],
    "modal/aviso"
  ).catch(() => null);

  await page.keyboard.press("Escape").catch(() => null);
  await page.waitForTimeout(200);
}

async function fazerLogin(page, cnpj, senha) {
  console.log("Etapa: login");

  await page.goto("https://www.nfse.gov.br/EmissorNacional/Login", {
    waitUntil: "domcontentloaded",
    timeout: TIMEOUT_MUITO_LONGO,
  });

  await esperarPaginaPronta(page, "login", 1200);
  await fecharPossiveisModaisOuAvisos(page);

  await preencherCampoComFallback(
    page,
    [
      'input[placeholder*="CPF/CNPJ"]',
      'input[placeholder*="CNPJ"]',
      'input[name="Inscricao"]',
      'input[id*="Inscricao"]',
      'form input[type="text"]',
    ],
    cnpj,
    "CNPJ de login"
  );

  await preencherCampoComFallback(
    page,
    [
      'input[placeholder*="Senha"]',
      'input[name="Senha"]',
      'input[id*="Senha"]',
      'input[type="password"]',
    ],
    senha,
    "Senha de login"
  );

  await clicarComFallback(
    page,
    ['button:has-text("Entrar")', 'input[value="Entrar"]', 'text="Entrar"'],
    "botão Entrar"
  );

  await page.waitForTimeout(1800);

  const erroLogin = await page
    .locator('text="Usuário e/ou senha inválidos"')
    .first()
    .isVisible()
    .catch(() => false);

  if (erroLogin) {
    throw new Error("Usuário e/ou senha inválidos no portal do emissor.");
  }

  console.log("Login concluído.");
}

async function abrirTelaPessoas(page) {
  console.log("Etapa: abrir tela Pessoas");

  await page.goto("https://www.nfse.gov.br/EmissorNacional/DPS/Pessoas", {
    waitUntil: "domcontentloaded",
    timeout: TIMEOUT_MUITO_LONGO,
  });

  await esperarPaginaPronta(page, "Pessoas", 1400);
  await fecharPossiveisModaisOuAvisos(page);

  const marcadores = [
    'text="Pessoas"',
    'text="Data de Competência"',
    'text="EMITENTE DA NFS-E"',
    'text="TOMADOR DO SERVIÇO"',
    'button:has-text("Avançar")',
  ];

  const ok = await esperarQualquerUm(page, marcadores, TIMEOUT_LONGO);
  if (ok) {
    console.log("Tela Pessoas identificada.");
    return;
  }

  throw new Error("A tela Pessoas não foi identificada.");
}

async function preencherDataCompetencia(page, dataIso) {
  const dataPtBr = formatarDataParaPortal(dataIso);

  const seletores = [
    'xpath=//label[contains(., "Data de Competência")]/following::input[1]',
    'xpath=//span[contains(., "Data de Competência")]/following::input[1]',
    'input[id*="Competencia"]',
    'input[name*="Competencia"]',
    "input.form-control",
  ];

  for (const selector of seletores) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: TIMEOUT_LONGO });
      await locator.scrollIntoViewIfNeeded().catch(() => null);
      await locator.click({ timeout: TIMEOUT_MEDIO });
      await page.waitForTimeout(150);
      await locator.press("Control+A").catch(() => null);
      await locator.press("Meta+A").catch(() => null);
      await locator.fill("");
      await page.waitForTimeout(120);
      await locator.type(dataPtBr, { delay: 40 });

      const valorAtual = await locator.inputValue().catch(() => "");
      if (valorAtual.includes("/")) {
        console.log("Data de competência preenchida com sucesso.");
        return;
      }
    } catch {}
  }

  throw new Error("Não foi possível preencher a data de competência.");
}

async function clicarEspacoEmBrancoParaCarregarEmitente(page) {
  const tentativas = [
    async () => page.mouse.click(1200, 260),
    async () => page.mouse.click(1100, 320),
    async () => page.locator("body").click({ position: { x: 1100, y: 260 } }),
    async () => page.locator("body").click({ position: { x: 900, y: 240 } }),
  ];

  for (const tentar of tentativas) {
    try {
      await tentar();
      await page.waitForTimeout(700);
      return;
    } catch {}
  }
}

async function esperarEmitentePreenchido(page, cnpj) {
  const candidatos = [
    `text="${cnpj}"`,
    `xpath=//*[contains(text(), "${cnpj}")]`,
  ];

  for (const selector of candidatos) {
    try {
      await page
        .locator(selector)
        .first()
        .waitFor({ state: "visible", timeout: TIMEOUT_MEDIO });
      console.log("Dados do emitente preenchidos automaticamente.");
      return;
    } catch {}
  }

  console.log("Não consegui confirmar visualmente o preenchimento do emitente.");
}

async function selecionarBrasilTomador(page) {
  const seletores = [
    'xpath=//div[contains(., "TOMADOR DO SERVIÇO")]//label[contains(., "Brasil")]',
    'xpath=(//label[contains(., "Brasil")])[1]',
    'label:has-text("Brasil")',
    'text="Brasil"',
  ];

  for (const selector of seletores) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: TIMEOUT_LONGO });
      await locator.scrollIntoViewIfNeeded().catch(() => null);
      await locator.click({ timeout: TIMEOUT_MEDIO, force: true });
      await page.waitForTimeout(500);
      return;
    } catch {}
  }

  throw new Error("Não foi possível selecionar Brasil no Tomador do Serviço.");
}

async function preencherDocumentoTomador(page, documentoTomador) {
  await preencherCampoComFallback(
    page,
    [
      'xpath=//label[contains(., "CPF/CNPJ")]/following::input[1]',
      'xpath=//label[contains(., "CPF")]/following::input[1]',
      'xpath=//label[contains(., "CNPJ")]/following::input[1]',
      'input[id*="CpfCnpj"]',
      'input[name*="CpfCnpj"]',
      'input[placeholder*="CPF"]',
      'input[placeholder*="CNPJ"]',
    ],
    documentoTomador,
    "Documento do tomador"
  );

  await page.waitForTimeout(400);

  await clicarComFallback(
    page,
    [
      'xpath=//label[contains(., "CPF/CNPJ")]/following::button[1]',
      'xpath=//label[contains(., "CPF")]/following::button[1]',
      'xpath=//label[contains(., "CNPJ")]/following::button[1]',
      'button[title*="Pesquisar"]',
      'button[aria-label*="Pesquisar"]',
      'button[title*="Buscar"]',
      'button[aria-label*="Buscar"]',
    ],
    "lupa do tomador"
  );

  const carregou = await esperarQualquerUm(
    page,
    [
      'button:has-text("Avançar")',
      'text="Serviço"',
      'text="Município"',
      'text="Código de Tributação Nacional"',
      'text="Descrição do Serviço"',
    ],
    TIMEOUT_LONGO
  );

  if (!carregou) {
    throw new Error("A pesquisa do tomador não retornou para avançar no fluxo.");
  }

  await page.waitForTimeout(500);
}

async function preencherEtapaPessoas(page, input) {
  const documentoTomador = limparDocumento(input.tomadorDocumento);
  const cnpjEmitente = limparDocumento(input.cnpjEmpresa);

  await preencherDataCompetencia(page, input.competencyDate);
  await page.waitForTimeout(300);

  await clicarEspacoEmBrancoParaCarregarEmitente(page);
  await esperarEmitentePreenchido(page, cnpjEmitente);

  await selecionarBrasilTomador(page);
  await preencherDocumentoTomador(page, documentoTomador);

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Pessoas"
  );

  await esperarPaginaPronta(page, "transição Pessoas > Serviço", 1200);
}

async function selecionarMunicipioPrestacao(page, municipioCompleto) {
  const municipioOriginal = normalizarMunicipioParaPortal(municipioCompleto);
  const nomeMunicipio = extrairNomeMunicipio(municipioOriginal);
  const ufMunicipio = extrairUfMunicipio(municipioOriginal);

  if (!nomeMunicipio || nomeMunicipio.length < 3) {
    throw new Error("Município inválido para o Select2.");
  }

  const campoMunicipio = await obterLocatorVisivel(
    page,
    [
      'xpath=//label[contains(normalize-space(.), "Município")]/following::span[contains(@class,"select2-selection")][1]',
      'xpath=//label[contains(normalize-space(.), "Municipio")]/following::span[contains(@class,"select2-selection")][1]',
      'xpath=//*[contains(normalize-space(.), "Município")]/following::span[contains(@class,"select2-selection")][1]',
      'xpath=//*[contains(normalize-space(.), "Municipio")]/following::span[contains(@class,"select2-selection")][1]',
    ],
    TIMEOUT_LONGO
  );

  if (!campoMunicipio) {
    throw new Error("Não foi possível localizar o campo Select2 do Município.");
  }

  const inputBusca = await abrirSelect2ECapturarBusca(page, campoMunicipio, "Município");
  await inputBusca.click({ force: true }).catch(() => null);
  await inputBusca.fill("").catch(() => null);
  await inputBusca.type(nomeMunicipio, { delay: 45 });
  await page.waitForTimeout(1200);

  const opcoes = page.locator("li.select2-results__option");
  let total = await opcoes.count().catch(() => 0);

  if (!total) {
    await page.waitForTimeout(900);
    total = await opcoes.count().catch(() => 0);
  }

  if (!total) {
    throw new Error(`Nenhuma opção de município apareceu para: ${nomeMunicipio}`);
  }

  let melhorOpcao = null;
  let melhorPontuacao = -1;

  for (let i = 0; i < total; i++) {
    const opcao = opcoes.nth(i);
    const texto = ((await opcao.textContent().catch(() => "")) || "").trim();
    const textoNormalizado = normalizarTexto(texto);

    if (!textoNormalizado) continue;
    if (textoNormalizado.includes("digite pelo menos 3 caracteres")) continue;
    if (textoNormalizado.includes("pesquisando")) continue;
    if (textoNormalizado.includes("carregando")) continue;
    if (textoNormalizado.includes("nenhum resultado")) continue;

    let pontuacao = 0;

    if (textoNormalizado === normalizarTexto(municipioOriginal)) pontuacao += 1000;
    if (textoNormalizado.startsWith(normalizarTexto(municipioOriginal))) pontuacao += 600;
    if (textoNormalizado.includes(normalizarTexto(municipioOriginal))) pontuacao += 400;

    if (textoNormalizado === normalizarTexto(nomeMunicipio)) pontuacao += 250;
    if (textoNormalizado.startsWith(normalizarTexto(nomeMunicipio))) pontuacao += 180;
    if (textoNormalizado.includes(normalizarTexto(nomeMunicipio))) pontuacao += 120;

    if (ufMunicipio) {
      if (textoNormalizado.includes(`/${ufMunicipio}`)) pontuacao += 220;
      if (textoNormalizado.includes(` ${ufMunicipio}`)) pontuacao += 120;
      if (textoNormalizado.endsWith(ufMunicipio)) pontuacao += 120;
    }

    const nomeMunicipioNormalizado = normalizarTexto(nomeMunicipio);
    const somenteNomeExato =
      textoNormalizado === nomeMunicipioNormalizado ||
      textoNormalizado.startsWith(`${nomeMunicipioNormalizado}/`) ||
      textoNormalizado.startsWith(`${nomeMunicipioNormalizado} -`);

    if (!ufMunicipio && somenteNomeExato) {
      pontuacao += 80;
    }

    if (pontuacao > melhorPontuacao) {
      melhorPontuacao = pontuacao;
      melhorOpcao = opcao;
    }
  }

  if (!melhorOpcao || melhorPontuacao < 150) {
    throw new Error(
      `Não foi possível encontrar a opção correta do município: ${municipioOriginal}`
    );
  }

  const textoSelecionado = ((await melhorOpcao.textContent().catch(() => "")) || "").trim();
  console.log("Município solicitado:", municipioOriginal);
  console.log("Município selecionado no Select2:", textoSelecionado);

  await melhorOpcao.scrollIntoViewIfNeeded().catch(() => null);
  await melhorOpcao.click({ force: true, timeout: TIMEOUT_MEDIO });
  await page.waitForTimeout(700);
}

async function localizarCampoCodigoTributacao(page) {
  const seletores = [
    'xpath=//label[contains(normalize-space(.), "Código de Tributação Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//label[contains(normalize-space(.), "Codigo de Tributacao Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Código de Tributação Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Codigo de Tributacao Nacional")]/following::span[contains(@class,"select2-selection")][1]',
  ];

  const locator = await obterLocatorVisivel(page, seletores, TIMEOUT_LONGO);
  if (locator) return locator;

  throw new Error("Não foi possível localizar o campo Código de Tributação Nacional.");
}

async function selecionarCodigoTributacao(page, taxCode) {
  const codigoOriginal = limparDocumento(taxCode);
  if (!codigoOriginal) {
    throw new Error("Código de Tributação não informado.");
  }

  const campo = await localizarCampoCodigoTributacao(page);
  const inputBusca = await abrirSelect2ECapturarBusca(
    page,
    campo,
    "Código de Tributação Nacional"
  );

  await inputBusca.click({ force: true }).catch(() => null);
  await inputBusca.fill("").catch(() => null);
  await inputBusca.type(codigoOriginal, { delay: 45 });
  await page.waitForTimeout(1200);

  const opcoes = page.locator("li.select2-results__option");
  let total = await opcoes.count().catch(() => 0);

  if (!total) {
    await page.waitForTimeout(900);
    total = await opcoes.count().catch(() => 0);
  }

  if (!total) {
    throw new Error(`Nenhuma opção apareceu para o código de tributação: ${codigoOriginal}`);
  }

  let melhorOpcao = null;
  let melhorPontuacao = -1;

  for (let i = 0; i < total; i++) {
    const opcao = opcoes.nth(i);
    const texto = ((await opcao.textContent().catch(() => "")) || "").trim();
    const textoNormalizado = normalizarTexto(texto);

    if (!textoNormalizado) continue;
    if (textoNormalizado.includes("pesquisando")) continue;
    if (textoNormalizado.includes("carregando")) continue;
    if (textoNormalizado.includes("nenhum resultado")) continue;

    let pontuacao = 0;

    if (textoNormalizado === normalizarTexto(codigoOriginal)) pontuacao += 1000;
    if (textoNormalizado.startsWith(normalizarTexto(codigoOriginal))) pontuacao += 700;
    if (textoNormalizado.includes(normalizarTexto(codigoOriginal))) pontuacao += 300;

    if (pontuacao > melhorPontuacao) {
      melhorPontuacao = pontuacao;
      melhorOpcao = opcao;
    }
  }

  if (!melhorOpcao) {
    throw new Error(`Não foi possível selecionar o código de tributação: ${codigoOriginal}`);
  }

  await melhorOpcao.scrollIntoViewIfNeeded().catch(() => null);
  await melhorOpcao.click({ force: true, timeout: TIMEOUT_MEDIO });

  await page.waitForTimeout(1300);

  await esperarQualquerUm(
    page,
    [
      'xpath=//label[contains(., "não incidência do ISSQN?")]',
      'xpath=//label[contains(., "nao incidencia do ISSQN?")]',
      'xpath=//label[contains(., "Descrição do Serviço")]',
      'xpath=//label[contains(., "Descricao do Servico")]',
      "textarea",
    ],
    TIMEOUT_LONGO
  );
}

async function marcarOpcaoNao(page) {
  const seletores = [
    'xpath=//label[contains(., "não incidência do ISSQN?")]/following::label[contains(normalize-space(.), "Não")][1]',
    'xpath=//label[contains(., "nao incidencia do ISSQN?")]/following::label[contains(normalize-space(.), "Não")][1]',
    'label:has-text("Não")',
    'text="Não"',
  ];

  for (const selector of seletores) {
    try {
      const loc = page.locator(selector).first();
      const count = await loc.count().catch(() => 0);
      if (!count) continue;
      const visivel = await loc.isVisible().catch(() => false);
      if (!visivel) continue;

      await loc.scrollIntoViewIfNeeded().catch(() => null);
      await loc.click({ force: true, timeout: TIMEOUT_MEDIO });
      await page.waitForTimeout(400);
      return;
    } catch {}
  }

  throw new Error('Não foi possível marcar a opção "Não".');
}

async function preencherDescricaoServico(page, descricao) {
  await preencherCampoComFallback(
    page,
    [
      'xpath=//label[contains(., "Descrição do Serviço")]/following::textarea[1]',
      'xpath=//label[contains(., "Descricao do Servico")]/following::textarea[1]',
      'textarea[name*="descr"]',
      'textarea[id*="descr"]',
      'textarea[placeholder*="descr"]',
      "textarea",
    ],
    descricao,
    "Descrição do serviço"
  );
}

async function esperarTelaServico(page) {
  const marcador = await esperarQualquerUm(
    page,
    [
      'text="Serviço"',
      'text="Município"',
      'text="Código de Tributação Nacional"',
      'text="Descrição do Serviço"',
    ],
    TIMEOUT_LONGO
  );

  if (!marcador) {
    throw new Error("A etapa Serviço não foi carregada corretamente.");
  }
}

async function preencherEtapaServico(page, input) {
  await esperarPaginaPronta(page, "Serviço", 1100);
  await esperarTelaServico(page);
  await page.waitForTimeout(400);

  await selecionarMunicipioPrestacao(page, input.serviceCity);
  await page.waitForTimeout(450);

  await selecionarCodigoTributacao(page, input.taxCode);
  await page.waitForTimeout(750);

  await marcarOpcaoNao(page);
  await page.waitForTimeout(400);

  await preencherDescricaoServico(page, input.serviceDescription);
  await page.waitForTimeout(450);

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Serviço"
  );

  await esperarPaginaPronta(page, "transição Serviço > Valores", 1200);
}

async function preencherEtapaValores(page, input) {
  await esperarPaginaPronta(page, "Valores", 900);

  const valorDigitacao = formatarValorParaDigitacaoNoPortal(input.serviceValue);
  console.log("Valor original recebido:", input.serviceValue);
  console.log("Valor enviado para digitação no portal:", valorDigitacao);

  await preencherCampoComFallback(
    page,
    [
      'input[name*="valor"]',
      'input[id*="valor"]',
      'input[placeholder*="Valor"]',
      'xpath=//label[contains(., "Valor")]/following::input[1]',
    ],
    valorDigitacao,
    "Valor do serviço"
  );

  await clicarSeExistir(
    page,
    [
      'label:has-text("Não reter")',
      'text="Não reter"',
      'label:has-text("Não")',
      'text="Não"',
    ],
    "opção não reter"
  );

  await page.waitForTimeout(350);

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Valores"
  );

  const carregouPrevia = await esperarQualquerUm(
    page,
    [
      'button:has-text("Emitir NFS-e")',
      'a:has-text("Emitir NFS-e")',
      'text="PRÉVIA DOS VALORES DA NFS-E"',
      'text="Visualizar NFS-e"',
      'text="Voltar"',
    ],
    TIMEOUT_LONGO
  );

  if (!carregouPrevia) {
    throw new Error("A tela final de prévia da emissão não foi carregada.");
  }

  await esperarPaginaPronta(page, "Prévia da emissão", 1300);
}

async function salvarDownloadComExtensao(download, nomeArquivo) {
  const pathTemp = await download.path();

  if (!pathTemp) {
    throw new Error(`Download não retornou caminho do arquivo: ${nomeArquivo}`);
  }

  console.log(`Arquivo ${nomeArquivo} baixado em:`, pathTemp);

  return pathTemp;
}

async function uploadBufferToStorage(buffer, destinationPath, contentType) {
  const { error } = await supabaseAdmin.storage
    .from(STORAGE_BUCKET)
    .upload(destinationPath, buffer, {
      contentType,
      upsert: true,
    });

  if (error) {
    throw new Error(`Erro ao enviar arquivo para o Storage: ${error.message}`);
  }

  const { data } = supabaseAdmin.storage
    .from(STORAGE_BUCKET)
    .getPublicUrl(destinationPath);

  return data?.publicUrl || null;
}

async function baixarArquivoPorBotao(page, selectors, nomeLogico, nomeArquivo) {
  for (const selector of selectors) {
    try {
      const botao = page.locator(selector).first();
      const count = await botao.count().catch(() => 0);
      if (!count) continue;

      const visivel = await botao.isVisible().catch(() => false);
      if (!visivel) continue;

      console.log(`Tentando baixar ${nomeLogico} com seletor: ${selector}`);

      const [download] = await Promise.all([
        page.waitForEvent("download", { timeout: TIMEOUT_LONGO }),
        botao.click({ force: true }),
      ]);

      const path = await salvarDownloadComExtensao(download, nomeArquivo);

      console.log(`${nomeLogico} salvo em: ${path}`);
      console.log(`${nomeLogico} nome sugerido pelo portal: ${download.suggestedFilename()}`);

      return {
        path,
        suggestedFilename: download.suggestedFilename(),
      };
    } catch (error) {
      console.log(`Falha ao baixar ${nomeLogico} com seletor ${selector}:`, error);
    }
  }

  console.log(`Não foi possível baixar ${nomeLogico}.`);
  return null;
}

async function localizarBotaoFinalEmitir(page) {
  const candidatos = [
    page.locator('button:has-text("Emitir NFS-e")'),
    page.locator('a:has-text("Emitir NFS-e")'),
    page.locator('input[value*="Emitir NFS-e"]'),
    page.locator('text="Emitir NFS-e"'),
  ];

  for (const grupo of candidatos) {
    const total = await grupo.count().catch(() => 0);
    if (!total) continue;

    for (let i = total - 1; i >= 0; i--) {
      const item = grupo.nth(i);
      const visivel = await item.isVisible().catch(() => false);
      if (!visivel) continue;

      const texto = ((await item.textContent().catch(() => "")) || "").trim();
      console.log(`Botão final candidato encontrado [${i}]:`, texto || "[sem texto]");
      return item;
    }
  }

  throw new Error('Não encontrei botão "Emitir NFS-e" na tela final.');
}

async function emitirNotaNaTelaFinal(page) {
  await esperarPaginaPronta(page, "Tela final antes da emissão", 1800);

  const marcador = await esperarQualquerUm(
    page,
    [
      'button:has-text("Emitir NFS-e")',
      'a:has-text("Emitir NFS-e")',
      'text="PRÉVIA DOS VALORES DA NFS-E"',
      'text="Voltar"',
    ],
    TIMEOUT_LONGO
  );

  if (!marcador) {
    throw new Error('Não encontrei os marcadores da tela final de emissão.');
  }

  const botaoFinal = await localizarBotaoFinalEmitir(page);

  await botaoFinal.scrollIntoViewIfNeeded().catch(() => null);
  await botaoFinal.waitFor({ state: "visible", timeout: TIMEOUT_LONGO });
  await page.waitForTimeout(1200);

  await botaoFinal.click({ timeout: TIMEOUT_LONGO, force: true });

  console.log("Clique realizado no botão final Emitir NFS-e.");

  const apareceuAlgo = await esperarQualquerUm(
    page,
    [
      'text="Baixar DANFSe"',
      'text="Baixar XML"',
      'text="Visualizar NFS-e"',
      'text="NFS-e emitidas"',
      'text="A NFS-e foi gerada com sucesso"',
      'text="Processando"',
      'text="Aguarde"',
    ],
    TIMEOUT_LONGO
  );

  if (!apareceuAlgo) {
    throw new Error("Após clicar em Emitir NFS-e, a tela não apresentou resposta.");
  }
}

async function esperarConclusaoEmissao(page) {
  const inicio = Date.now();

  while (Date.now() - inicio < TIMEOUT_EMISSAO_FINAL) {
    const sucesso = await esperarQualquerUm(
      page,
      [
        'text="Baixar DANFSe"',
        'text="Baixar XML"',
        'text="Visualizar NFS-e"',
        'text="NFS-e emitidas"',
        'text="A NFS-e foi gerada com sucesso"',
      ],
      3000
    );

    if (sucesso) {
      console.log("Confirmação de emissão encontrada:", sucesso);
      return;
    }

    const mensagemErro = await esperarQualquerUm(
      page,
      [
        'text="Erro"',
        'text="Não foi possível"',
        'text="Tente novamente"',
        'text="Falha"',
      ],
      1200
    );

    if (mensagemErro) {
      console.log("Mensagem de erro detectada durante conclusão:", mensagemErro);
    }

    await page.waitForTimeout(800);
  }

  throw new Error("A emissão não foi confirmada na tela final.");
}

async function capturarLinksResultado(page) {
  const pdfUrl =
    (await page
      .locator('a:has-text("Baixar DANFSe")')
      .first()
      .getAttribute("href")
      .catch(() => null)) || null;

  const xmlUrl =
    (await page
      .locator('a:has-text("Baixar XML")')
      .first()
      .getAttribute("href")
      .catch(() => null)) || null;

  return { pdfUrl, xmlUrl };
}

async function capturarChaveOuNumeroNfse(page) {
  for (let i = 0; i < 12; i++) {
    const textoPagina = (await page.textContent("body").catch(() => "")) || "";

    const match44 = textoPagina.match(/\b\d{44}\b/);
    if (match44) {
      return match44[0];
    }

    const matchChave = textoPagina.match(/Chave\s*(de acesso)?\s*[:\-]?\s*(\d{30,})/i);
    if (matchChave) {
      return matchChave[2];
    }

    const matchNumero = textoPagina.match(/NFS-e\s*[:\-]?\s*(\d+)/i);
    if (matchNumero) {
      return matchNumero[1];
    }

    await page.waitForTimeout(700);
  }

  return null;
}

async function concluirEmissao(page) {
  await emitirNotaNaTelaFinal(page);
  await esperarConclusaoEmissao(page);

  const { pdfUrl, xmlUrl } = await capturarLinksResultado(page);
  const nfseKey = await capturarChaveOuNumeroNfse(page);

  const xmlFile = await baixarArquivoPorBotao(
    page,
    ['a:has-text("Baixar XML")', 'button:has-text("Baixar XML")', 'text="Baixar XML"'],
    "XML",
    "nfse.xml"
  );

  const pdfFile = await baixarArquivoPorBotao(
    page,
    ['a:has-text("Baixar DANFSe")', 'button:has-text("Baixar DANFSe")', 'text="Baixar DANFSe"'],
    "DANFSe",
    "nfse.pdf"
  );

  let pdfStorageUrl = null;
  let xmlStorageUrl = null;
  let pdfBase64 = null;
  let xmlBase64 = null;

  if (pdfFile?.path && nfseKey) {
    const pdfBuffer = await fs.readFile(pdfFile.path);
    pdfBase64 = pdfBuffer.toString("base64");
    pdfStorageUrl = await uploadBufferToStorage(
      pdfBuffer,
      `worker/${nfseKey}.pdf`,
      "application/pdf"
    );
    console.log("PDF enviado ao Supabase Storage:", pdfStorageUrl);
  }

  if (xmlFile?.path && nfseKey) {
    const xmlBuffer = await fs.readFile(xmlFile.path);
    xmlBase64 = xmlBuffer.toString("base64");
    xmlStorageUrl = await uploadBufferToStorage(
      xmlBuffer,
      `worker/${nfseKey}.xml`,
      "application/xml"
    );
    console.log("XML enviado ao Supabase Storage:", xmlStorageUrl);
  }

  return {
    success: true,
    message: "NFS-e emitida com sucesso.",
    pdfUrl: pdfStorageUrl || pdfUrl,
    xmlUrl: xmlStorageUrl || xmlUrl,
    nfseKey,
    pdfBase64,
    xmlBase64,
  };
}

async function executarFluxoCompleto(page, input) {
  await fazerLogin(page, limparDocumento(input.cnpjEmpresa), input.senhaEmpresa);
  await abrirTelaPessoas(page);
  await preencherEtapaPessoas(page, input);
  await preencherEtapaServico(page, input);
  await preencherEtapaValores(page, input);
  return await concluirEmissao(page);
}

async function criarContexto(browser) {
  return await browser.newContext({
    viewport: { width: 1440, height: 900 },
    acceptDownloads: true,
    ignoreHTTPSErrors: true,
  });
}

export async function emitirNfseViaAutomacao(input) {
  let ultimoErro = null;
  const maxTentativas = 2;

  for (let tentativa = 1; tentativa <= maxTentativas; tentativa++) {
    let browser = null;

    try {
      console.log(`🚀 Iniciando automação da NFS-e - tentativa ${tentativa}/${maxTentativas}`);
      console.log("CNPJ enviado:", limparDocumento(input.cnpjEmpresa));
      console.log("Senha enviada:", input.senhaEmpresa ? "[PREENCHIDA]" : "[VAZIA]");
      console.log("Data original:", input.competencyDate);
      console.log("Data formatada:", formatarDataParaPortal(input.competencyDate));
      console.log("Documento do tomador:", limparDocumento(input.tomadorDocumento));
      console.log("Código tributário:", input.taxCode);
      console.log("Cidade do serviço:", input.serviceCity);
      console.log("Valor do serviço:", input.serviceValue);
      console.log("Descrição do serviço:", input.serviceDescription);

      const headless =
        String(process.env.PLAYWRIGHT_HEADLESS || "true").toLowerCase() === "true";

      console.log("PLAYWRIGHT_HEADLESS:", process.env.PLAYWRIGHT_HEADLESS);
      console.log("PLAYWRIGHT_BROWSERS_PATH:", process.env.PLAYWRIGHT_BROWSERS_PATH);
      console.log("Executando navegador em modo headless:", headless);

      browser = await chromium.launch({
        headless,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-blink-features=AutomationControlled",
          "--single-process",
        ],
        slowMo: headless ? 0 : 60,
      });

      const context = await criarContexto(browser);
      const page = await context.newPage();

      page.setDefaultTimeout(TIMEOUT_LONGO);
      page.setDefaultNavigationTimeout(TIMEOUT_MUITO_LONGO);

      const resultado = await aguardarComTentativas(
        () => executarFluxoCompleto(page, input),
        1,
        0,
        "fluxo completo da automação"
      );

      console.log("✅ Resultado da automação:", resultado);

      await context.close().catch(() => null);
      await browser.close().catch(() => null);

      return resultado;
    } catch (error) {
      ultimoErro = error;
      console.log(`❌ Erro na tentativa ${tentativa}:`, error);

      await browser?.close().catch(() => null);

      const podeTentarNovamente =
        tentativa < maxTentativas && erroEhTransitorio(error);

      if (!podeTentarNovamente) {
        console.log("⛔ Erro definitivo ou limite de tentativas atingido.");
        break;
      }

      console.log("🔁 Erro transitório detectado. Tentando novamente...");
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }

  return {
    success: false,
    message:
      ultimoErro instanceof Error
        ? ultimoErro.message
        : "Erro ao emitir nota automaticamente após múltiplas tentativas",
    pdfUrl: null,
    xmlUrl: null,
    nfseKey: null,
    pdfBase64: null,
    xmlBase64: null,
  };
}