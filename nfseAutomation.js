import { chromium } from "playwright";

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
  ];

  return errosTransitorios.some((trecho) => msg.includes(trecho));
}

async function preencherCampoComFallback(page, selectors, valor, nomeCampo) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: 3500 });
      await locator.click({ timeout: 3500 });
      await locator.press("Control+A").catch(() => null);
      await locator.press("Meta+A").catch(() => null);
      await locator.fill("");
      await locator.type(valor, { delay: 30 });

      const valorAtual = await locator.inputValue().catch(() => "");
      console.log(`Campo ${nomeCampo} preenchido com seletor ${selector}:`, valorAtual);
      return;
    } catch {
      // tenta próximo
    }
  }

  throw new Error(`Não foi possível preencher o campo ${nomeCampo}.`);
}

async function clicarComFallback(page, selectors, nome) {
  for (const selector of selectors) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: 3500 });
      await locator.click({ timeout: 3500 });
      console.log(`Clique realizado em ${nome} com seletor: ${selector}`);
      return;
    } catch {
      // tenta próximo
    }
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

      await locator.click({ timeout: 2500 });
      console.log(`Clique opcional em ${nome} com seletor: ${selector}`);
      return true;
    } catch {
      // tenta próximo
    }
  }

  console.log(`Elemento opcional não encontrado: ${nome}`);
  return false;
}

async function esperarQualquerUm(page, selectors, timeout = 5000) {
  for (const selector of selectors) {
    try {
      await page.locator(selector).first().waitFor({ state: "visible", timeout });
      console.log("Elemento identificado:", selector);
      return selector;
    } catch {
      // tenta próximo
    }
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
  for (let tentativa = 1; tentativa <= 3; tentativa++) {
    await campo.scrollIntoViewIfNeeded().catch(() => null);
    await campo.click({ force: true, timeout: 3500 });
    await page.waitForTimeout(220);

    const inputBusca = await obterInputSelect2Visivel(page);
    if (inputBusca) {
      await inputBusca.waitFor({ state: "visible", timeout: 5000 });
      return inputBusca;
    }

    console.log(
      `Tentativa ${tentativa} sem input Select2 visível em ${nomeCampo}. Reabrindo...`
    );
    await page.keyboard.press("Escape").catch(() => null);
    await page.waitForTimeout(180);
  }

  throw new Error(`Não foi possível abrir o campo Select2 de ${nomeCampo}.`);
}

async function fazerLogin(page, cnpj, senha) {
  console.log("Etapa: login");

  await page.goto("https://www.nfse.gov.br/EmissorNacional/Login", {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });

  await page.waitForLoadState("networkidle").catch(() => null);

  await preencherCampoComFallback(
    page,
    [
      'input[placeholder*="CPF/CNPJ"]',
      'input[placeholder*="CNPJ"]',
      'input[name="Inscricao"]',
      'input[id*="Inscricao"]',
      'input[type="text"]',
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

  await page.waitForTimeout(900);

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
    timeout: 60000,
  });

  await page.waitForLoadState("networkidle").catch(() => null);

  const marcadores = [
    'text="Pessoas"',
    'text="Data de Competência"',
    'text="EMITENTE DA NFS-E"',
    'text="TOMADOR DO SERVIÇO"',
    'button:has-text("Avançar")',
  ];

  const ok = await esperarQualquerUm(page, marcadores, 4500);
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
    'input.form-control',
  ];

  for (const selector of seletores) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: 3500 });
      await locator.click({ timeout: 3500 });
      await locator.press("Control+A").catch(() => null);
      await locator.press("Meta+A").catch(() => null);
      await locator.fill("");
      await locator.type(dataPtBr, { delay: 40 });

      const valorAtual = await locator.inputValue().catch(() => "");
      if (valorAtual.includes("/")) {
        console.log("Data de competência preenchida com sucesso.");
        return;
      }
    } catch {
      // tenta próximo
    }
  }

  throw new Error("Não foi possível preencher a data de competência.");
}

async function clicarEspacoEmBrancoParaCarregarEmitente(page) {
  const tentativas = [
    async () => page.mouse.click(1200, 260),
    async () => page.mouse.click(1100, 320),
    async () => page.locator("body").click({ position: { x: 1100, y: 260 } }),
  ];

  for (const tentar of tentativas) {
    try {
      await tentar();
      await page.waitForTimeout(300);
      return;
    } catch {
      // tenta próxima
    }
  }
}

async function esperarEmitentePreenchido(page, cnpj) {
  const candidatos = [
    `text="${cnpj}"`,
    `xpath=//*[contains(text(), "${cnpj}")]`,
  ];

  for (const selector of candidatos) {
    try {
      await page.locator(selector).first().waitFor({ state: "visible", timeout: 2500 });
      console.log("Dados do emitente preenchidos automaticamente.");
      return;
    } catch {
      // tenta próximo
    }
  }

  console.log("Não consegui confirmar visualmente o preenchimento do emitente.");
}

async function selecionarBrasilTomador(page) {
  const seletores = [
    'xpath=//div[contains(., "TOMADOR DO SERVIÇO")]//label[contains(., "Brasil")]',
    'xpath=(//label[contains(., "Brasil")])[1]',
  ];

  for (const selector of seletores) {
    try {
      const locator = page.locator(selector).first();
      const count = await locator.count();
      if (!count) continue;

      await locator.waitFor({ state: "visible", timeout: 3500 });
      await locator.click({ timeout: 3500 });
      await page.waitForTimeout(250);
      return;
    } catch {
      // tenta próximo
    }
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

  await page.waitForTimeout(120);

  await clicarComFallback(
    page,
    [
      'xpath=//label[contains(., "CPF/CNPJ")]/following::button[1]',
      'xpath=//label[contains(., "CPF")]/following::button[1]',
      'xpath=//label[contains(., "CNPJ")]/following::button[1]',
      'button[title*="Pesquisar"]',
      'button[aria-label*="Pesquisar"]',
      'button:has(svg)',
    ],
    "lupa do tomador"
  );

  await esperarQualquerUm(
    page,
    [
      'button:has-text("Avançar")',
      'text="Serviço"',
      'text="Município"',
      'text="Código de Tributação Nacional"',
    ],
    1800
  );

  await page.waitForTimeout(180);
}

async function preencherEtapaPessoas(page, input) {
  const documentoTomador = limparDocumento(input.tomadorDocumento);
  const cnpjEmitente = limparDocumento(input.cnpjEmpresa);

  await preencherDataCompetencia(page, input.competencyDate);
  await page.waitForTimeout(120);

  await clicarEspacoEmBrancoParaCarregarEmitente(page);
  await esperarEmitentePreenchido(page, cnpjEmitente);

  await selecionarBrasilTomador(page);
  await preencherDocumentoTomador(page, documentoTomador);

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Pessoas"
  );
}

async function selecionarMunicipioPrestacao(page, municipioCompleto) {
  const municipioOriginal = normalizarMunicipioParaPortal(municipioCompleto);
  const nomeMunicipio = extrairNomeMunicipio(municipioOriginal);
  const ufMunicipio = extrairUfMunicipio(municipioOriginal);

  if (!nomeMunicipio || nomeMunicipio.length < 3) {
    throw new Error("Município inválido para o Select2.");
  }

  const seletoresCampo = [
    'xpath=//label[contains(normalize-space(.), "Município")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//label[contains(normalize-space(.), "Municipio")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Município")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Municipio")]/following::span[contains(@class,"select2-selection")][1]',
    'span.select2-selection.select2-selection--single',
  ];

  let campoMunicipio = null;

  for (const selector of seletoresCampo) {
    try {
      const loc = page.locator(selector).first();
      const count = await loc.count().catch(() => 0);
      if (!count) continue;
      const visivel = await loc.isVisible().catch(() => false);
      if (!visivel) continue;
      campoMunicipio = loc;
      break;
    } catch {
      // tenta próximo
    }
  }

  if (!campoMunicipio) {
    throw new Error("Não foi possível localizar o campo Select2 do Município.");
  }

  const inputBusca = await abrirSelect2ECapturarBusca(page, campoMunicipio, "Município");
  await inputBusca.click({ force: true }).catch(() => null);
  await inputBusca.fill("").catch(() => null);
  await inputBusca.type(nomeMunicipio, { delay: 40 });
  await page.waitForTimeout(500);

  const opcoes = page.locator("li.select2-results__option");
  let total = await opcoes.count().catch(() => 0);

  if (!total) {
    await page.waitForTimeout(350);
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
  await melhorOpcao.click({ force: true, timeout: 3500 });
  await page.waitForTimeout(250);
}

async function localizarCampoCodigoTributacao(page) {
  const seletores = [
    'xpath=//label[contains(normalize-space(.), "Código de Tributação Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//label[contains(normalize-space(.), "Codigo de Tributacao Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Código de Tributação Nacional")]/following::span[contains(@class,"select2-selection")][1]',
    'xpath=//*[contains(normalize-space(.), "Codigo de Tributacao Nacional")]/following::span[contains(@class,"select2-selection")][1]',
  ];

  for (const selector of seletores) {
    try {
      const loc = page.locator(selector).first();
      const count = await loc.count().catch(() => 0);
      if (!count) continue;
      const visivel = await loc.isVisible().catch(() => false);
      if (!visivel) continue;
      return loc;
    } catch {
      // tenta próximo
    }
  }

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
  await inputBusca.type(codigoOriginal, { delay: 40 });
  await page.waitForTimeout(650);

  const opcoes = page.locator("li.select2-results__option");
  let total = await opcoes.count().catch(() => 0);

  if (!total) {
    await page.waitForTimeout(450);
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
    if (textoNormalizado.includes(normalizarTexto(codigoOriginal))) pontuacao += 100;

    if (pontuacao > melhorPontuacao) {
      melhorPontuacao = pontuacao;
      melhorOpcao = opcao;
    }
  }

  if (!melhorOpcao) {
    throw new Error(`Não foi possível selecionar o código de tributação: ${codigoOriginal}`);
  }

  await melhorOpcao.scrollIntoViewIfNeeded().catch(() => null);
  await melhorOpcao.click({ force: true, timeout: 3500 });

  await page.waitForTimeout(1000);

  await esperarQualquerUm(
    page,
    [
      'xpath=//label[contains(., "não incidência do ISSQN?")]',
      'xpath=//label[contains(., "nao incidencia do ISSQN?")]',
      'xpath=//label[contains(., "Descrição do Serviço")]',
      'xpath=//label[contains(., "Descricao do Servico")]',
      "textarea",
    ],
    4500
  );
}

async function marcarOpcaoNao(page) {
  const seletores = [
    'xpath=//label[contains(., "não incidência do ISSQN?")]/following::label[contains(normalize-space(.), "Não")][1]',
    'xpath=//label[contains(., "nao incidencia do ISSQN?")]/following::label[contains(normalize-space(.), "Não")][1]',
    'xpath=//label[contains(normalize-space(.), "Não")][1]',
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

      await loc.click({ force: true, timeout: 3500 });
      await page.waitForTimeout(220);
      return;
    } catch {
      // tenta próximo
    }
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

async function preencherEtapaServico(page, input) {
  await page.waitForTimeout(550);
  await page
    .locator('text="Serviço"')
    .first()
    .waitFor({ state: "visible", timeout: 6000 })
    .catch(() => null);
  await page.waitForTimeout(180);

  await selecionarMunicipioPrestacao(page, input.serviceCity);
  await page.waitForTimeout(180);

  await selecionarCodigoTributacao(page, input.taxCode);
  await page.waitForTimeout(500);

  await marcarOpcaoNao(page);
  await page.waitForTimeout(220);

  await preencherDescricaoServico(page, input.serviceDescription);
  await page.waitForTimeout(260);

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Serviço"
  );
}

async function preencherEtapaValores(page, input) {
  await page.waitForTimeout(250);

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

  await clicarComFallback(
    page,
    ['button:has-text("Avançar")', 'text="Avançar"'],
    "botão Avançar da etapa Valores"
  );

  await esperarQualquerUm(
    page,
    [
      'button:has-text("Emitir NFS-e")',
      'a:has-text("Emitir NFS-e")',
      'text="PRÉVIA DOS VALORES DA NFS-E"',
      'text="Voltar"',
    ],
    8000
  );
}

async function salvarDownloadComExtensao(download, caminhoDestino) {
  await download.saveAs(caminhoDestino);
  return caminhoDestino;
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
        page.waitForEvent("download", { timeout: 8000 }),
        botao.click({ force: true }),
      ]);

      const path = `/tmp/${nomeArquivo}`;
      await salvarDownloadComExtensao(download, path);

      console.log(`${nomeLogico} salvo em: ${path}`);
      console.log(`${nomeLogico} nome sugerido pelo portal: ${download.suggestedFilename()}`);

      return path;
    } catch (error) {
      console.log(`Falha ao baixar ${nomeLogico} com seletor ${selector}:`, error);
    }
  }

  console.log(`Não foi possível baixar ${nomeLogico}.`);
  return null;
}

async function emitirNotaNaTelaFinal(page) {
  await esperarQualquerUm(
    page,
    [
      'button:has-text("Emitir NFS-e")',
      'a:has-text("Emitir NFS-e")',
      'text="PRÉVIA DOS VALORES DA NFS-E"',
      'text="Voltar"',
    ],
    6000
  );

  const botoes = page.locator('button:has-text("Emitir NFS-e"), a:has-text("Emitir NFS-e")');
  const total = await botoes.count().catch(() => 0);

  if (!total) {
    throw new Error('Não encontrei botão "Emitir NFS-e" na tela final.');
  }

  const indice = total > 1 ? 1 : 0;
  const botaoFinal = botoes.nth(indice);

  await botaoFinal.scrollIntoViewIfNeeded().catch(() => null);
  await botaoFinal.click({ force: true, timeout: 2000 });

  console.log(`Clique realizado no botão Emitir NFS-e índice ${indice}.`);

  await esperarQualquerUm(
    page,
    [
      'text="Baixar DANFSe"',
      'text="Baixar XML"',
      'text="Visualizar NFS-e"',
      'text="NFS-e emitidas"',
      'text="A NFS-e foi gerada com sucesso"',
    ],
    7000
  );
}

async function esperarConclusaoEmissao(page) {
  const timeoutTotal = 10000;
  const inicio = Date.now();

  while (Date.now() - inicio < timeoutTotal) {
    const sucesso = await esperarQualquerUm(
      page,
      [
        'text="Baixar DANFSe"',
        'text="Baixar XML"',
        'text="Visualizar NFS-e"',
        'text="NFS-e emitidas"',
        'text="A NFS-e foi gerada com sucesso"',
      ],
      1500
    );

    if (sucesso) return;

    await page.waitForTimeout(300);
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
  for (let i = 0; i < 5; i++) {
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

    await page.waitForTimeout(500);
  }

  return null;
}

async function concluirEmissao(page) {
  await emitirNotaNaTelaFinal(page);
  await esperarConclusaoEmissao(page);

  const { pdfUrl, xmlUrl } = await capturarLinksResultado(page);
  const nfseKey = await capturarChaveOuNumeroNfse(page);

  const xmlPath = await baixarArquivoPorBotao(
    page,
    ['a:has-text("Baixar XML")', 'button:has-text("Baixar XML")', 'text="Baixar XML"'],
    "XML",
    "nfse.xml"
  );

  const pdfPath = await baixarArquivoPorBotao(
    page,
    ['a:has-text("Baixar DANFSe")', 'button:has-text("Baixar DANFSe")', 'text="Baixar DANFSe"'],
    "DANFSe",
    "nfse.pdf"
  );

  return {
    success: true,
    message: "NFS-e emitida com sucesso.",
    pdfUrl,
    xmlUrl,
    nfseKey,
    pdfPath,
    xmlPath,
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
      console.log("Executando navegador em modo headless:", headless);

      browser = await chromium.launch({
        headless,
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
        slowMo: headless ? 0 : 60,
      });

      const context = await browser.newContext({
        viewport: { width: 1440, height: 900 },
        acceptDownloads: true,
      });

      const page = await context.newPage();

      const resultado = await executarFluxoCompleto(page, input);
      console.log("✅ Resultado da automação:", resultado);

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
    pdfPath: null,
    xmlPath: null,
  };
}