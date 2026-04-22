import express from "express";
import { createClient } from "@supabase/supabase-js";
import { emitirNfseViaAutomacao } from "./nfseAutomation.js";

const app = express();

app.use(express.json({ limit: "10mb" }));

const SUPABASE_URL = String(process.env.NEXT_PUBLIC_SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE_KEY = String(
  process.env.SUPABASE_SERVICE_ROLE_KEY || ""
).trim();

const supabaseAdmin = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

const fila = [];
let emExecucao = 0;
const MAX_CONCORRENCIA = Number(process.env.MAX_CONCORRENCIA || 1);

const INTERVALO_FILA_LOCAL_MS = Number(
  process.env.PROCESSAR_FILA_INTERVALO_MS || 3000
);

const STALE_JOB_MINUTES = 8;
const PROCESSANDO_EM_LOOP = {
  ativo: false,
};

function mascararValor(valor = "") {
  if (!valor) return "[VAZIO]";
  if (valor.length <= 10) return `${valor.slice(0, 2)}***${valor.slice(-2)}`;
  return `${valor.slice(0, 6)}...${valor.slice(-6)}`;
}

function onlyDigits(value) {
  return String(value ?? "").replace(/\D/g, "");
}

function getStaleJobIsoDate() {
  return new Date(Date.now() - STALE_JOB_MINUTES * 60 * 1000).toISOString();
}

function descreverErro(error) {
  if (!error) return "Erro desconhecido.";
  if (typeof error === "string") return error;
  if (error instanceof Error) return `${error.name}: ${error.message}`;

  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

async function testarConexaoSupabase() {
  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      throw new Error(
        "As variáveis NEXT_PUBLIC_SUPABASE_URL e/ou SUPABASE_SERVICE_ROLE_KEY estão ausentes."
      );
    }

    const { data, error } = await supabaseAdmin
      .from("invoice_jobs")
      .select("id")
      .limit(1);

    if (error) {
      throw new Error(`Falha no teste do Supabase: ${error.message}`);
    }

    console.log(
      "✅ Teste de conexão com Supabase OK.",
      Array.isArray(data) ? `Linhas consultadas: ${data.length}` : ""
    );

    return true;
  } catch (error) {
    console.error("❌ Falha ao testar conexão com Supabase:", error);
    return false;
  }
}

function processarFilaMemoria() {
  while (emExecucao < MAX_CONCORRENCIA && fila.length > 0) {
    const job = fila.shift();
    emExecucao += 1;

    (async () => {
      try {
        console.log("Nova requisição recebida no worker.");
        console.log(job.input);

        const resultado = await emitirNfseViaAutomacao(job.input);
        job.resolve(resultado);
      } catch (error) {
        console.error("Erro no worker /emitir:", error);

        job.reject(
          error instanceof Error ? error.message : "Erro interno no worker."
        );
      } finally {
        emExecucao -= 1;
        processarFilaMemoria();
      }
    })();
  }
}

async function logJob(params) {
  try {
    await supabaseAdmin.from("invoice_job_logs").insert({
      job_id: params.jobId,
      invoice_id: params.invoiceId,
      level: params.level || "info",
      message: params.message,
      meta: params.meta ?? null,
    });
  } catch (err) {
    console.error("Erro ao salvar log do job:", err);
  }
}

async function buscarCliente(clientId) {
  const { data, error } = await supabaseAdmin
    .from("clients")
    .select("id, cnpj, password, emissor_password, partner_company_id, is_active")
    .eq("id", clientId)
    .single();

  if (error || !data) {
    throw new Error("Cliente do job não encontrado.");
  }

  return data;
}

function getSenhaEmissor(cliente) {
  return String(cliente.emissor_password || cliente.password || "").trim();
}

async function atualizarInvoiceParaPending(invoiceId, mensagem = null) {
  const { error } = await supabaseAdmin
    .from("invoices")
    .update({
      status: "pending",
      error_message: mensagem,
      updated_at: new Date().toISOString(),
    })
    .eq("id", invoiceId);

  if (error) {
    throw new Error(error.message);
  }
}

async function atualizarInvoiceParaProcessing(invoiceId) {
  const { error } = await supabaseAdmin
    .from("invoices")
    .update({
      status: "processing",
      error_message: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", invoiceId);

  if (error) {
    throw new Error(error.message);
  }
}

async function atualizarInvoiceParaError(invoiceId, mensagem) {
  const { error } = await supabaseAdmin
    .from("invoices")
    .update({
      status: "error",
      error_message: mensagem,
      updated_at: new Date().toISOString(),
    })
    .eq("id", invoiceId);

  if (error) {
    throw new Error(error.message);
  }
}

async function atualizarInvoiceParaCanceled(
  invoiceId,
  mensagem = "Emissão cancelada pelo usuário."
) {
  const { error } = await supabaseAdmin
    .from("invoices")
    .update({
      status: "canceled",
      error_message: mensagem,
      updated_at: new Date().toISOString(),
    })
    .eq("id", invoiceId);

  if (error) {
    throw new Error(error.message);
  }
}

async function atualizarInvoiceParaSuccess(invoiceId, resultado) {
  const nfseKey = String(resultado?.nfseKey || resultado?.nfse_key || "").trim() || null;
  const pdfUrl = String(resultado?.pdfUrl || resultado?.pdf_url || "").trim() || null;
  const xmlUrl = String(resultado?.xmlUrl || resultado?.xml_url || "").trim() || null;
  const pdfPath = String(resultado?.pdfPath || resultado?.pdf_path || pdfUrl || "").trim() || null;
  const xmlPath = String(resultado?.xmlPath || resultado?.xml_path || xmlUrl || "").trim() || null;

  const { error } = await supabaseAdmin
    .from("invoices")
    .update({
      status: "success",
      error_message: null,
      nfse_key: nfseKey,
      pdf_url: pdfUrl,
      xml_url: xmlUrl,
      pdf_path: pdfPath,
      xml_path: xmlPath,
      updated_at: new Date().toISOString(),
    })
    .eq("id", invoiceId);

  if (error) {
    throw new Error(error.message);
  }
}

async function liberarJobsTravados() {
  const staleIso = getStaleJobIsoDate();

  let data;
  let error;

  try {
    const resposta = await supabaseAdmin
      .from("invoice_jobs")
      .select("*")
      .eq("status", "processing")
      .eq("job_type", "emit_nfse")
      .lt("locked_at", staleIso);

    data = resposta.data;
    error = resposta.error;
  } catch (erroConsulta) {
    throw new Error(`Erro ao buscar jobs travados: ${descreverErro(erroConsulta)}`);
  }

  if (error) {
    throw new Error(`Erro ao buscar jobs travados: ${error.message}`);
  }

  const jobsTravados = data || [];

  for (const job of jobsTravados) {
    const excedeu = Number(job.attempts || 0) >= Number(job.max_attempts || 3);

    if (excedeu) {
      await supabaseAdmin
        .from("invoice_jobs")
        .update({
          status: "error",
          finished_at: new Date().toISOString(),
          locked_at: null,
          error_message:
            "Job travado em processing por tempo excedido. Limite de tentativas atingido.",
          updated_at: new Date().toISOString(),
        })
        .eq("id", job.id);

      await atualizarInvoiceParaError(
        job.invoice_id,
        "A emissão falhou após ficar travada em processamento."
      );

      await logJob({
        jobId: job.id,
        invoiceId: job.invoice_id,
        level: "error",
        message: "Job travado finalizado como erro por exceder tentativas.",
        meta: {
          attempts: job.attempts,
          max_attempts: job.max_attempts,
          locked_at: job.locked_at,
        },
      });

      continue;
    }

    await supabaseAdmin
      .from("invoice_jobs")
      .update({
        status: "queued",
        locked_at: null,
        started_at: null,
        error_message: "Job recuperado automaticamente após travar em processing.",
        updated_at: new Date().toISOString(),
      })
      .eq("id", job.id);

    await atualizarInvoiceParaPending(
      job.invoice_id,
      "Job recuperado automaticamente após travar em processamento."
    );

    await logJob({
      jobId: job.id,
      invoiceId: job.invoice_id,
      level: "warning",
      message: "Job travado recuperado automaticamente para a fila.",
      meta: {
        attempts: job.attempts,
        max_attempts: job.max_attempts,
        locked_at: job.locked_at,
      },
    });
  }

  return jobsTravados.length;
}

async function buscarProximoJobQueued() {
  const { data, error } = await supabaseAdmin
    .from("invoice_jobs")
    .select("*")
    .eq("status", "queued")
    .eq("job_type", "emit_nfse")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(error.message);
  }

  return data || null;
}

async function travarJob(jobId) {
  const agora = new Date().toISOString();

  const { data, error } = await supabaseAdmin
    .from("invoice_jobs")
    .update({
      status: "processing",
      locked_at: agora,
      started_at: agora,
      updated_at: agora,
    })
    .eq("id", jobId)
    .eq("status", "queued")
    .select()
    .maybeSingle();

  if (error) {
    throw new Error(error.message);
  }

  return data || null;
}

async function incrementarTentativa(job) {
  const proximaTentativa = Number(job.attempts || 0) + 1;

  const { error } = await supabaseAdmin
    .from("invoice_jobs")
    .update({
      attempts: proximaTentativa,
      updated_at: new Date().toISOString(),
    })
    .eq("id", job.id);

  if (error) {
    throw new Error(error.message);
  }

  return proximaTentativa;
}

async function finalizarJobSucesso(jobId, result) {
  const agora = new Date().toISOString();

  const { error } = await supabaseAdmin
    .from("invoice_jobs")
    .update({
      status: "success",
      finished_at: agora,
      locked_at: null,
      result: result ?? null,
      error_message: null,
      updated_at: agora,
    })
    .eq("id", jobId);

  if (error) {
    throw new Error(error.message);
  }
}

async function finalizarJobErro(job, mensagem, result = null) {
  const agora = new Date().toISOString();
  const excedeu = Number(job.attempts || 0) >= Number(job.max_attempts || 3);
  const novoStatus = excedeu ? "error" : "queued";

  const { error } = await supabaseAdmin
    .from("invoice_jobs")
    .update({
      status: novoStatus,
      finished_at: excedeu ? agora : null,
      locked_at: null,
      error_message: mensagem,
      result,
      updated_at: agora,
    })
    .eq("id", job.id);

  if (error) {
    throw new Error(error.message);
  }
}

async function finalizarJobCancelado(
  jobId,
  mensagem = "Emissão cancelada pelo usuário."
) {
  const agora = new Date().toISOString();

  const { error } = await supabaseAdmin
    .from("invoice_jobs")
    .update({
      status: "canceled",
      finished_at: agora,
      locked_at: null,
      error_message: mensagem,
      result: { canceled: true, message: mensagem },
      updated_at: agora,
    })
    .eq("id", jobId);

  if (error) {
    throw new Error(error.message);
  }
}

async function processarJobFila(job) {
  await logJob({
    jobId: job.id,
    invoiceId: job.invoice_id,
    level: "info",
    message: "Iniciando processamento automático do job no worker.",
    meta: {
      attempts: job.attempts,
      max_attempts: job.max_attempts,
    },
  });

  if (job.cancel_requested) {
    await atualizarInvoiceParaCanceled(job.invoice_id);
    await finalizarJobCancelado(job.id);
    return {
      success: false,
      canceled: true,
      message: "Job cancelado antes do processamento.",
    };
  }

  const cliente = await buscarCliente(job.client_id);
  const senhaEmissor = getSenhaEmissor(cliente);

  if (!cliente.is_active) {
    throw new Error("Cliente inativo.");
  }

  if (!onlyDigits(cliente.cnpj) || !senhaEmissor) {
    throw new Error("Cliente sem dados fiscais completos para emissão.");
  }

  await atualizarInvoiceParaProcessing(job.invoice_id);

  const payload = job.payload || {};

  const resultado = await emitirNfseViaAutomacao({
    cnpjEmpresa: cliente.cnpj,
    senhaEmpresa: senhaEmissor,
    competencyDate: payload.competencyDate,
    tomadorDocumento: payload.tomadorDocumento,
    taxCode: payload.taxCode,
    serviceCity: payload.serviceCity,
    serviceValue: payload.serviceValue,
    serviceDescription: payload.serviceDescription,
    cancelKey: payload.cancelKey || String(job.invoice_id),
  });

  if (!resultado?.success) {
    const mensagem = resultado?.message || "Falha na automação.";

    if (
      mensagem.includes("EMISSAO_CANCELADA_USUARIO") ||
      mensagem.includes("Emissão cancelada pelo usuário.")
    ) {
      throw new Error("EMISSAO_CANCELADA_USUARIO");
    }

    throw new Error(mensagem);
  }

  const nfseKey = String(resultado?.nfseKey || "").trim();

  if (!nfseKey) {
    throw new Error("Nota emitida sem chave de acesso.");
  }

  await atualizarInvoiceParaSuccess(job.invoice_id, resultado);

  await logJob({
    jobId: job.id,
    invoiceId: job.invoice_id,
    level: "info",
    message: "Job processado com sucesso no worker.",
    meta: {
      nfseKey: resultado?.nfseKey || null,
      pdfUrl: resultado?.pdfUrl || null,
      xmlUrl: resultado?.xmlUrl || null,
      pdfPath: resultado?.pdfPath || null,
      xmlPath: resultado?.xmlPath || null,
    },
  });

  await finalizarJobSucesso(job.id, {
    success: true,
    invoiceId: job.invoice_id,
    nfseKey: resultado?.nfseKey || null,
    pdfUrl: resultado?.pdfUrl || null,
    xmlUrl: resultado?.xmlUrl || null,
    pdfPath: resultado?.pdfPath || null,
    xmlPath: resultado?.xmlPath || null,
  });

  return {
    success: true,
    invoiceId: job.invoice_id,
    nfseKey: resultado?.nfseKey || null,
  };
}

async function processarFilaAutomatica() {
  if (PROCESSANDO_EM_LOOP.ativo) {
    return;
  }

  PROCESSANDO_EM_LOOP.ativo = true;

  try {
    console.log("🔄 Iniciando varredura automática da fila...");

    await liberarJobsTravados();

    while (emExecucao < MAX_CONCORRENCIA) {
      const jobQueued = await buscarProximoJobQueued();

      if (!jobQueued) {
        console.log("Nenhum job queued encontrado no momento.");
        break;
      }

      console.log(
        `Job queued encontrado: ${jobQueued.id} | invoice ${jobQueued.invoice_id}`
      );

      const jobTravado = await travarJob(jobQueued.id);

      if (!jobTravado) {
        console.log(`Não foi possível travar o job ${jobQueued.id}.`);
        continue;
      }

      const attemptsAtual = await incrementarTentativa(jobTravado);
      const jobComTentativa = {
        ...jobTravado,
        attempts: attemptsAtual,
      };

      emExecucao += 1;

      (async () => {
        try {
          console.log(
            `Processando job automático ${jobComTentativa.id} da invoice ${jobComTentativa.invoice_id}`
          );

          await processarJobFila(jobComTentativa);
        } catch (error) {
          const mensagemErro = String(
            error?.message || "Erro ao processar job automático."
          );

          console.error(
            `Erro no job ${jobComTentativa.id} / invoice ${jobComTentativa.invoice_id}:`,
            mensagemErro
          );

          await logJob({
            jobId: jobComTentativa.id,
            invoiceId: jobComTentativa.invoice_id,
            level: "error",
            message: "Erro ao processar job automático.",
            meta: {
              error: mensagemErro,
              attempt: jobComTentativa.attempts,
            },
          });

          if (
            mensagemErro.includes("EMISSAO_CANCELADA_USUARIO") ||
            mensagemErro.includes("cancelada pelo usuário")
          ) {
            await atualizarInvoiceParaCanceled(
              jobComTentativa.invoice_id,
              "Emissão cancelada pelo usuário."
            );
            await finalizarJobCancelado(
              jobComTentativa.id,
              "Emissão cancelada pelo usuário."
            );
          } else {
            const excedeu =
              Number(jobComTentativa.attempts || 0) >=
              Number(jobComTentativa.max_attempts || 3);

            if (excedeu) {
              await atualizarInvoiceParaError(
                jobComTentativa.invoice_id,
                mensagemErro
              );
            } else {
              await atualizarInvoiceParaPending(
                jobComTentativa.invoice_id,
                "Tentando novamente a emissão após falha temporária."
              );
            }

            await finalizarJobErro(jobComTentativa, mensagemErro, {
              lastError: mensagemErro,
              attempt: jobComTentativa.attempts,
            });
          }
        } finally {
          emExecucao -= 1;
        }
      })();
    }
  } catch (error) {
    console.error("Erro geral no loop automático da fila:", error);
  } finally {
    PROCESSANDO_EM_LOOP.ativo = false;
  }
}

setInterval(() => {
  processarFilaAutomatica().catch((error) => {
    console.error("Erro no setInterval da fila automática:", error);
  });
}, INTERVALO_FILA_LOCAL_MS);

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    message: "NFSe Worker online",
    concorrenciaMaxima: MAX_CONCORRENCIA,
    intervaloFilaMs: INTERVALO_FILA_LOCAL_MS,
    emExecucao,
    filaMemoria: fila.length,
    supabaseUrl: SUPABASE_URL || null,
    supabaseServiceRolePresent: Boolean(SUPABASE_SERVICE_ROLE_KEY),
    supabaseServiceRoleLength: SUPABASE_SERVICE_ROLE_KEY.length,
  });
});

app.get("/health", async (_req, res) => {
  const status = {
    ok: true,
    worker: "online",
    emExecucao,
    filaMemoria: fila.length,
    concorrenciaMaxima: MAX_CONCORRENCIA,
    intervaloFilaMs: INTERVALO_FILA_LOCAL_MS,
    loopAtivo: PROCESSANDO_EM_LOOP.ativo,
    supabaseUrl: SUPABASE_URL || null,
    supabaseServiceRolePresent: Boolean(SUPABASE_SERVICE_ROLE_KEY),
    supabaseServiceRoleLength: SUPABASE_SERVICE_ROLE_KEY.length,
    supabaseConnection: "unknown",
    supabaseError: null,
  };

  try {
    const { error } = await supabaseAdmin
      .from("invoice_jobs")
      .select("id")
      .limit(1);

    if (error) {
      status.ok = false;
      status.supabaseConnection = "error";
      status.supabaseError = error.message;
      return res.status(500).json(status);
    }

    status.supabaseConnection = "ok";
    return res.json(status);
  } catch (error) {
    status.ok = false;
    status.supabaseConnection = "error";
    status.supabaseError = descreverErro(error);
    return res.status(500).json(status);
  }
});

app.post("/emitir", async (req, res) => {
  const input = req.body;

  return new Promise((resolveResposta) => {
    fila.push({
      input,
      resolve: (resultado) => {
        res.json(resultado);
        resolveResposta();
      },
      reject: (mensagemErro) => {
        res.status(500).json({
          success: false,
          message: mensagemErro,
        });
        resolveResposta();
      },
    });

    processarFilaMemoria();
  });
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, async () => {
  console.log(`SUPABASE URL: ${SUPABASE_URL || "[VAZIO]"}`);
  console.log(
    `SUPABASE SERVICE ROLE PRESENTE: ${Boolean(SUPABASE_SERVICE_ROLE_KEY)}`
  );
  console.log(
    `SUPABASE SERVICE ROLE MASK: ${mascararValor(SUPABASE_SERVICE_ROLE_KEY)}`
  );
  console.log(
    `SUPABASE SERVICE ROLE LENGTH: ${SUPABASE_SERVICE_ROLE_KEY.length}`
  );

  console.log(`Worker rodando na porta ${PORT}`);
  console.log(`MAX_CONCORRENCIA: ${MAX_CONCORRENCIA}`);
  console.log(`PROCESSAR_FILA_INTERVALO_MS: ${INTERVALO_FILA_LOCAL_MS}`);

  const conexaoOk = await testarConexaoSupabase();

  if (!conexaoOk) {
    console.error("❌ O worker iniciou, mas a conexão com o Supabase falhou.");
  }

  try {
    await processarFilaAutomatica();
  } catch (error) {
    console.error("Erro ao iniciar processamento automático:", error);
  }
});