import express from "express";
import { emitirNfseViaAutomacao } from "./nfseAutomation.js";

const app = express();

app.use(express.json({ limit: "10mb" }));

const fila = [];
let emExecucao = 0;
const MAX_CONCORRENCIA = 3;

function processarFila() {
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
        console.error("Erro no worker:", error);

        job.reject(
          error instanceof Error ? error.message : "Erro interno no worker."
        );
      } finally {
        emExecucao -= 1;
        processarFila();
      }
    })();
  }
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    message: "NFSe Worker online",
  });
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

    processarFila();
  });
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`Worker rodando na porta ${PORT}`);
});