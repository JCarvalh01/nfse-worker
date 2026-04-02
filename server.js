import express from "express";
import { emitirNfseViaAutomacao } from "./nfseAutomation.js";

const app = express();

app.use(express.json({ limit: "10mb" }));

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    message: "NFSe Worker online"
  });
});

app.post("/emitir", async (req, res) => {
  try {
    const input = req.body;

    console.log("Nova requisição recebida no worker.");
    console.log(input);

    const resultado = await emitirNfseViaAutomacao(input);

    return res.json(resultado);
  } catch (error) {
    console.error("Erro no worker:", error);

    return res.status(500).json({
      success: false,
      message:
        error instanceof Error
          ? error.message
          : "Erro interno no worker."
    });
  }
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`Worker rodando na porta ${PORT}`);
});