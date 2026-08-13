import "dotenv/config";
import WebSocket from "ws";
import fetch from "node-fetch";
import bs58 from "bs58";
import {
  Connection,
  Keypair,
  VersionedTransaction,
} from "@solana/web3.js";

// ---------- Config ----------
const CFG = {
  privateKey: process.env.SOLANA_PRIVATE_KEY || "",
  rpcUrl: process.env.RPC_URL || "https://api.mainnet-beta.solana.com",
  buyAmountSol: parseFloat(process.env.BUY_AMOUNT_SOL || "0.01"),
  maxMarketCapSol: parseFloat(process.env.MAX_MARKET_CAP_SOL || "30"),
  blacklist: (process.env.BLACKLIST_KEYWORDS || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean),
  takeProfitPct: parseFloat(process.env.TAKE_PROFIT_PCT || "50"),
  stopLossPct: parseFloat(process.env.STOP_LOSS_PCT || "25"),
  maxHoldMinutes: parseFloat(process.env.MAX_HOLD_MINUTES || "30"),
  slippagePct: parseFloat(process.env.SLIPPAGE_PCT || "15"),
  priorityFeeSol: parseFloat(process.env.PRIORITY_FEE_SOL || "0.0001"),
  dryRun: (process.env.DRY_RUN || "true").toLowerCase() !== "false",
  maxOpenPositions: parseInt(process.env.MAX_OPEN_POSITIONS || "1", 10),
};

const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";
const PUMPPORTAL_TRADE_LOCAL = "https://pumpportal.fun/api/trade-local";

// ---------- Wallet / connection ----------
let keypair = null;
if (CFG.privateKey) {
  keypair = Keypair.fromSecretKey(bs58.decode(CFG.privateKey));
}
const connection = new Connection(CFG.rpcUrl, "confirmed");

// ---------- State ----------
// positions ouvertes: mint -> { entryPriceSol, amountTokens, buyTimestamp }
const positions = new Map();

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

// ---------- Filtre stratégie snipe ----------
function passesFilter(tokenEvent) {
  const name = (tokenEvent.name || "").toLowerCase();
  const symbol = (tokenEvent.symbol || "").toLowerCase();

  if (CFG.blacklist.some((kw) => name.includes(kw) || symbol.includes(kw))) {
    return false;
  }
  const marketCapSol = tokenEvent.marketCapSol ?? tokenEvent.vSolInBondingCurve;
  if (marketCapSol !== undefined && marketCapSol > CFG.maxMarketCapSol) {
    return false;
  }
  if (positions.size >= CFG.maxOpenPositions) {
    return false;
  }
  return true;
}

// ---------- Construction + envoi transaction via Local Trading API ----------
async function trade(action, mint, amount, denominatedInSol) {
  if (!keypair) {
    log("ERREUR: SOLANA_PRIVATE_KEY manquante, impossible de trader.");
    return null;
  }

  const body = {
    publicKey: keypair.publicKey.toBase58(),
    action, // "buy" | "sell"
    mint,
    amount,
    denominatedInSol: denominatedInSol ? "true" : "false",
    slippage: CFG.slippagePct,
    priorityFee: CFG.priorityFeeSol,
    pool: "pump",
  };

  if (CFG.dryRun) {
    log(`[DRY_RUN] ${action.toUpperCase()} ${mint} amount=${amount} sol=${denominatedInSol}`);
    return { simulated: true };
  }

  try {
    const res = await fetch(PUMPPORTAL_TRADE_LOCAL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (res.status !== 200) {
      log(`Erreur API trade-local (${res.status}):`, await res.text());
      return null;
    }

    const txData = new Uint8Array(await res.arrayBuffer());
    const tx = VersionedTransaction.deserialize(txData);
    tx.sign([keypair]);

    const sig = await connection.sendRawTransaction(tx.serialize(), {
      skipPreflight: false,
      maxRetries: 3,
    });

    log(`TX envoyée: https://solscan.io/tx/${sig}`);
    return { signature: sig };
  } catch (err) {
    log("Erreur execution trade:", err.message);
    return null;
  }
}

// ---------- Achat ----------
async function buyToken(tokenEvent) {
  const mint = tokenEvent.mint;
  log(`ACHAT -> ${tokenEvent.name} (${tokenEvent.symbol}) mint=${mint}`);

  const result = await trade("buy", mint, CFG.buyAmountSol, true);
  if (!result) return;

  positions.set(mint, {
    entryPriceSol: tokenEvent.marketCapSol ?? tokenEvent.vSolInBondingCurve ?? 0,
    buyTimestamp: Date.now(),
    name: tokenEvent.name,
    symbol: tokenEvent.symbol,
  });

  subscribeTokenTrades(mint);
}

// ---------- Vente ----------
async function sellToken(mint, reason) {
  const pos = positions.get(mint);
  if (!pos) return;

  log(`VENTE -> ${pos.name} (${pos.symbol}) mint=${mint} raison=${reason}`);
  // amount=100%, denominatedInSol=false -> vend 100% du solde de tokens
  await trade("sell", mint, "100%", false);
  positions.delete(mint);
}

// ---------- Suivi prix position ouverte (via trades live du mint) ----------
function evaluatePosition(mint, currentMarketCapSol) {
  const pos = positions.get(mint);
  if (!pos || !pos.entryPriceSol) return;

  const changePct =
    ((currentMarketCapSol - pos.entryPriceSol) / pos.entryPriceSol) * 100;

  if (changePct >= CFG.takeProfitPct) {
    sellToken(mint, `take-profit (+${changePct.toFixed(1)}%)`);
    return;
  }
  if (changePct <= -CFG.stopLossPct) {
    sellToken(mint, `stop-loss (${changePct.toFixed(1)}%)`);
    return;
  }
  if (CFG.maxHoldMinutes > 0) {
    const heldMin = (Date.now() - pos.buyTimestamp) / 60000;
    if (heldMin >= CFG.maxHoldMinutes) {
      sellToken(mint, `max-hold-time (${heldMin.toFixed(1)}min)`);
    }
  }
}

// ---------- WebSocket principal (nouveaux tokens) ----------
function connectMainSocket() {
  const ws = new WebSocket(PUMPPORTAL_WS);

  ws.on("open", () => {
    log("Connecté à PumpPortal, subscribe nouveaux tokens...");
    ws.send(JSON.stringify({ method: "subscribeNewToken" }));
  });

  ws.on("message", async (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!data.mint) return; // ignore messages non pertinents

    if (passesFilter(data)) {
      await buyToken(data);
    }
  });

  ws.on("close", () => {
    log("Socket principal fermé, reconnexion dans 5s...");
    setTimeout(connectMainSocket, 5000);
  });

  ws.on("error", (err) => log("Erreur socket principal:", err.message));
}

// ---------- WebSocket par position (suivi prix live) ----------
function subscribeTokenTrades(mint) {
  const ws = new WebSocket(PUMPPORTAL_WS);

  ws.on("open", () => {
    ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  });

  ws.on("message", (raw) => {
    if (!positions.has(mint)) {
      ws.close();
      return;
    }
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const mc = data.marketCapSol ?? data.vSolInBondingCurve;
    if (mc !== undefined) evaluatePosition(mint, mc);
  });

  ws.on("close", () => {
    if (positions.has(mint)) {
      // reconnecte tant que la position est ouverte
      setTimeout(() => subscribeTokenTrades(mint), 3000);
    }
  });

  ws.on("error", () => {});
}

// ---------- Boot ----------
log("=== Bot pump.fun démarré ===");
log(`DRY_RUN=${CFG.dryRun} | buy=${CFG.buyAmountSol} SOL | TP=${CFG.takeProfitPct}% | SL=${CFG.stopLossPct}%`);
if (!keypair && !CFG.dryRun) {
  log("ATTENTION: aucune clé privée fournie et DRY_RUN=false -> le bot ne pourra pas trader.");
}
connectMainSocket();

// keepalive simple pour Railway (évite exit process)
setInterval(() => {}, 1 << 30);
