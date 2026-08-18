import "dotenv/config";
import http from "http";
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
  takeProfitPct: parseFloat(process.env.TAKE_PROFIT_PCT || "80"),
  stopLossPct: parseFloat(process.env.STOP_LOSS_PCT || "20"),
  maxHoldMinutes: parseFloat(process.env.MAX_HOLD_MINUTES || "15"),
  deadTokenTimeoutMinutes: parseFloat(process.env.DEAD_TOKEN_TIMEOUT_MINUTES || "2"),
  candidateWindowSeconds: parseFloat(process.env.CANDIDATE_WINDOW_SECONDS || "8"),
  minBuysToQualify: parseInt(process.env.MIN_BUYS_TO_QUALIFY || "2", 10),
  slippagePct: parseFloat(process.env.SLIPPAGE_PCT || "15"),
  priorityFeeSol: parseFloat(process.env.PRIORITY_FEE_SOL || "0.0001"),
  dryRun: (process.env.DRY_RUN || "true").toLowerCase() !== "false",
  maxOpenPositions: parseInt(process.env.MAX_OPEN_POSITIONS || "1", 10),
  paperStartingBalanceSol: parseFloat(process.env.PAPER_STARTING_BALANCE_SOL || "1"),
  pumpFunFeePct: 1, // fee natif pump.fun, incompressible, appliqué même en paper trading
  ntfyTopic: process.env.NTFY_TOPIC || "pump_fun_bot",
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

// candidats en cours d'observation: mint -> { name, symbol, createdAt, buyCount, marketCapSol }
const candidates = new Map();
let scoreSocket = null;

// ---------- Paper trading (solde fictif, données de marché réelles) ----------
const paper = {
  balance: CFG.paperStartingBalanceSol,
  startingBalance: CFG.paperStartingBalanceSol,
  closedTrades: [], // { pnlSol, pnlPct, reason, name }
};

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

// ---------- Notification push (ntfy.sh, gratuit, sans compte) ----------
async function notify(title, message) {
  if (!CFG.ntfyTopic) return;
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    const res = await fetch("https://ntfy.sh/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        topic: CFG.ntfyTopic,
        title,
        message,
        priority: 3,
      }),
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) {
      log(`Notification refusée par ntfy.sh (status ${res.status})`);
    }
  } catch (err) {
    const detail = err.cause?.code || err.cause?.message || err.message || String(err);
    log("Erreur envoi notification:", detail);
  }
}

function logPaperSummary() {
  const n = paper.closedTrades.length;
  if (n === 0) {
    log(`[PAPER] Solde=${paper.balance.toFixed(4)} SOL | positions ouvertes=${positions.size} | 0 trade clôturé`);
    return;
  }
  const wins = paper.closedTrades.filter((t) => t.pnlSol > 0).length;
  const totalPnl = paper.balance - paper.startingBalance;
  const totalPnlPct = (totalPnl / paper.startingBalance) * 100;
  log(
    `[PAPER] Solde=${paper.balance.toFixed(4)} SOL (${totalPnl >= 0 ? "+" : ""}${totalPnl.toFixed(4)} / ${totalPnlPct.toFixed(1)}%) | ` +
      `trades=${n} | winrate=${((wins / n) * 100).toFixed(0)}% | positions ouvertes=${positions.size}`
  );
}

// ---------- Filtre initial (avant même de devenir candidat) ----------
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
  if (CFG.dryRun) {
    log(`[DRY_RUN] ${action.toUpperCase()} ${mint} amount=${amount} sol=${denominatedInSol}`);
    return { simulated: true };
  }

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

  if (CFG.dryRun && paper.balance < CFG.buyAmountSol) {
    log(`[PAPER] Solde fictif insuffisant (${paper.balance.toFixed(4)} SOL), achat ignoré: ${tokenEvent.name}`);
    return;
  }

  log(`ACHAT -> ${tokenEvent.name} (${tokenEvent.symbol}) mint=${mint}`);
  notify(
    `🟢 Achat: ${tokenEvent.symbol}`,
    `${tokenEvent.name} | ${CFG.buyAmountSol} SOL | mint=${mint}`
  );

  const result = await trade("buy", mint, CFG.buyAmountSol, true);
  if (!result) return;

  if (CFG.dryRun) {
    paper.balance -= CFG.buyAmountSol; // immobilisé dans la position fictive
  }

  positions.set(mint, {
    entryPriceSol: tokenEvent.marketCapSol ?? tokenEvent.vSolInBondingCurve ?? 0,
    lastKnownMarketCapSol: tokenEvent.marketCapSol ?? tokenEvent.vSolInBondingCurve ?? 0,
    buyTimestamp: Date.now(),
    lastActivityTimestamp: Date.now(), // mis à jour à chaque trade reçu sur ce mint
    name: tokenEvent.name,
    symbol: tokenEvent.symbol,
  });

  if (scoreSocket && scoreSocket.readyState === WebSocket.OPEN) {
    scoreSocket.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  }
}

// ---------- Vente ----------
async function sellToken(mint, reason, exitMarketCapSol) {
  const pos = positions.get(mint);
  if (!pos) return;

  log(`VENTE -> ${pos.name} (${pos.symbol}) mint=${mint} raison=${reason}`);
  // amount=100%, denominatedInSol=false -> vend 100% du solde de tokens
  await trade("sell", mint, "100%", false);
  positions.delete(mint);

  if (CFG.dryRun) {
    const changePct =
      pos.entryPriceSol && exitMarketCapSol !== undefined
        ? ((exitMarketCapSol - pos.entryPriceSol) / pos.entryPriceSol) * 100
        : 0;
    // PnL brut sur le montant investi, moins fee pump.fun 1% (aller+retour) et slippage estimé
    const grossReturn = CFG.buyAmountSol * (1 + changePct / 100);
    const feesEstimate = CFG.buyAmountSol * ((CFG.pumpFunFeePct * 2) / 100);
    const netReturn = Math.max(0, grossReturn - feesEstimate);
    const pnlSol = netReturn - CFG.buyAmountSol;
    const pnlPct = (pnlSol / CFG.buyAmountSol) * 100;

    paper.balance += netReturn;
    paper.closedTrades.push({ pnlSol, pnlPct, reason, name: pos.name });

    log(
      `[PAPER] Clôture ${pos.symbol}: ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL (${pnlPct.toFixed(1)}%) | ` +
        `nouveau solde=${paper.balance.toFixed(4)} SOL`
    );
    notify(
      `${pnlSol >= 0 ? "🟢" : "🔴"} Vente: ${pos.symbol} ${pnlSol >= 0 ? "+" : ""}${pnlPct.toFixed(1)}%`,
      `${reason} | PnL: ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL | Solde: ${paper.balance.toFixed(4)} SOL`
    );
  } else {
    notify(`💰 Vente: ${pos.symbol}`, `${reason} | mint=${mint}`);
  }
}

// ---------- Suivi prix position ouverte (via trades live du mint) ----------
function evaluatePosition(mint, currentMarketCapSol) {
  const pos = positions.get(mint);
  if (!pos || !pos.entryPriceSol) return;

  pos.lastKnownMarketCapSol = currentMarketCapSol; // toujours garder le dernier prix connu
  pos.lastActivityTimestamp = Date.now(); // un trade est arrivé -> token pas mort

  const changePct =
    ((currentMarketCapSol - pos.entryPriceSol) / pos.entryPriceSol) * 100;

  if (changePct >= CFG.takeProfitPct) {
    sellToken(mint, `take-profit (+${changePct.toFixed(1)}%)`, currentMarketCapSol);
    return;
  }
  if (changePct <= -CFG.stopLossPct) {
    sellToken(mint, `stop-loss (${changePct.toFixed(1)}%)`, currentMarketCapSol);
    return;
  }
}

// vérifie le temps de hold indépendamment des messages websocket reçus
// (un token peu liquide peut ne plus émettre aucun trade après l'achat -> sans ce check
// séparé, max-hold-time ne se déclencherait jamais)
function checkMaxHoldTime() {
  const now = Date.now();
  for (const [mint, pos] of positions.entries()) {
    const heldMin = (now - pos.buyTimestamp) / 60000;
    const inactiveMin = (now - pos.lastActivityTimestamp) / 60000;

    // token mort: aucun trade reçu depuis DEAD_TOKEN_TIMEOUT_MINUTES -> sortie anticipée
    if (CFG.deadTokenTimeoutMinutes > 0 && inactiveMin >= CFG.deadTokenTimeoutMinutes) {
      sellToken(mint, `token inactif (${inactiveMin.toFixed(1)}min sans trade)`, pos.lastKnownMarketCapSol);
      continue;
    }

    if (CFG.maxHoldMinutes > 0 && heldMin >= CFG.maxHoldMinutes) {
      sellToken(mint, `max-hold-time (${heldMin.toFixed(1)}min)`, pos.lastKnownMarketCapSol);
    }
  }
}

// ---------- WebSocket principal (nouveaux tokens -> deviennent candidats) ----------
function connectMainSocket() {
  const ws = new WebSocket(PUMPPORTAL_WS);

  ws.on("open", () => {
    log("Connecté à PumpPortal, subscribe nouveaux tokens...");
    ws.send(JSON.stringify({ method: "subscribeNewToken" }));
  });

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!data.mint) return; // ignore messages non pertinents
    if (candidates.has(data.mint) || positions.has(data.mint)) return;

    if (passesFilter(data)) {
      candidates.set(data.mint, {
        name: data.name,
        symbol: data.symbol,
        createdAt: Date.now(),
        buyCount: 0,
        volumeSol: 0,
        marketCapSol: data.marketCapSol ?? data.vSolInBondingCurve ?? 0,
      });
      if (scoreSocket && scoreSocket.readyState === WebSocket.OPEN) {
        scoreSocket.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [data.mint] }));
      }
    }
  });

  ws.on("close", () => {
    log("Socket principal fermé, reconnexion dans 5s...");
    setTimeout(connectMainSocket, 5000);
  });

  ws.on("error", (err) => log("Erreur socket principal:", err.message));
}

// ---------- Scoring des candidats (comptage achats live) ----------
function connectScoreSocket() {
  scoreSocket = new WebSocket(PUMPPORTAL_WS);

  scoreSocket.on("open", () => {
    // resouscrit tout ce qui est actuellement suivi (couvre le 1er connect ET les reconnexions)
    const mints = [...candidates.keys(), ...positions.keys()];
    if (mints.length > 0) {
      scoreSocket.send(JSON.stringify({ method: "subscribeTokenTrade", keys: mints }));
      log(`Score socket (re)connecté, ${mints.length} mint(s) resouscrit(s).`);
    } else {
      log("Score socket connecté.");
    }
  });

  scoreSocket.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!data.mint) return;
    const mc = data.marketCapSol ?? data.vSolInBondingCurve;

    // position ouverte sur ce mint -> évalue TP/SL
    if (positions.has(data.mint)) {
      if (mc !== undefined) evaluatePosition(data.mint, mc);
      return;
    }

    // sinon, candidat en observation -> scoring trending
    const cand = candidates.get(data.mint);
    if (!cand) return;
    const isBuy = data.txType === "buy" || data.is_buy === true;
    if (isBuy) {
      cand.buyCount += 1;
      cand.volumeSol += data.solAmount ?? 0;
    }
    if (mc !== undefined) cand.marketCapSol = mc;
  });

  scoreSocket.on("close", () => {
    log("Score socket fermé, reconnexion dans 3s...");
    setTimeout(connectScoreSocket, 3000);
  });

  scoreSocket.on("error", () => {});
}

// évalue les candidats dont la fenêtre d'observation est close, achète le plus trending
function resolveCandidates() {
  const now = Date.now();
  const expired = [];

  for (const [mint, cand] of candidates.entries()) {
    const ageSec = (now - cand.createdAt) / 1000;
    if (ageSec >= CFG.candidateWindowSeconds) expired.push([mint, cand]);
  }
  if (expired.length === 0) return;

  // retire tous les candidats expirés de la liste d'observation
  for (const [mint] of expired) {
    candidates.delete(mint);
    if (scoreSocket && scoreSocket.readyState === WebSocket.OPEN) {
      scoreSocket.send(JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] }));
    }
  }

  if (positions.size >= CFG.maxOpenPositions) return;

  // le plus trending = le plus d'achats reçus pendant la fenêtre, tie-break sur volume
  expired.sort((a, b) => b[1].buyCount - a[1].buyCount || b[1].volumeSol - a[1].volumeSol);
  const [winnerMint, winner] = expired[0];

  if (winner.buyCount < CFG.minBuysToQualify) {
    log(`Aucun candidat assez trending (meilleur: ${winner.symbol} avec ${winner.buyCount} achats), skip.`);
    return;
  }

  log(`Trending détecté: ${winner.symbol} (${winner.buyCount} achats en ${CFG.candidateWindowSeconds}s)`);
  buyToken({
    mint: winnerMint,
    name: winner.name,
    symbol: winner.symbol,
    marketCapSol: winner.marketCapSol,
  });
}

// ---------- Boot ----------
log("=== Bot pump.fun démarré ===");
log(`DRY_RUN=${CFG.dryRun} | buy=${CFG.buyAmountSol} SOL | TP=${CFG.takeProfitPct}% | SL=${CFG.stopLossPct}%`);
if (CFG.dryRun) {
  log(`[PAPER] Mode paper trading actif. Solde fictif de départ = ${paper.balance.toFixed(4)} SOL`);
}
log(`Notifications: topic ntfy.sh = "${CFG.ntfyTopic}"`);
notify("🤖 Bot démarré", `pump.fun bot en ligne | DRY_RUN=${CFG.dryRun}`);
if (!keypair && !CFG.dryRun) {
  log("ATTENTION: aucune clé privée fournie et DRY_RUN=false -> le bot ne pourra pas trader.");
}
connectMainSocket();
connectScoreSocket();

// check indépendant du max-hold-time, pas seulement quand un trade websocket arrive
setInterval(checkMaxHoldTime, 30 * 1000);

// résout les candidats en fin de fenêtre d'observation (achète le plus trending)
setInterval(resolveCandidates, 1000);

// serveur HTTP minimal - juste pour répondre au healthcheck Railway (le bot n'a pas besoin de port)
const port = process.env.PORT || 3000;
http
  .createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pumpfun-bot OK\n");
  })
  .listen(port, () => log(`Healthcheck HTTP en écoute sur le port ${port}`));

// ---------- Heartbeat (notif toutes les 5min - confirme que le bot est vivant) ----------
const bootTime = Date.now();
setInterval(() => {
  const uptimeMin = Math.round((Date.now() - bootTime) / 60000);
  const balanceInfo = CFG.dryRun ? ` | Solde: ${paper.balance.toFixed(4)} SOL` : "";
  notify(
    "Bot actif",
    `Uptime: ${uptimeMin}min | Positions ouvertes: ${positions.size}${balanceInfo}`
  );
}, 5 * 60 * 1000);

// ---------- Arrêt propre + robustesse ----------
// évite un crash bruyant (npm error) sur un SIGTERM normal de Railway (redeploy, scaling)
function gracefulShutdown(signal) {
  log(`Signal ${signal} reçu, arrêt propre du bot...`);
  logPaperSummary();
  process.exit(0);
}
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// une erreur async oubliée quelque part (fetch, websocket) ne doit pas tuer tout le process
process.on("unhandledRejection", (err) => {
  log("Erreur non gérée (promise):", err?.message || err);
});
process.on("uncaughtException", (err) => {
  log("Erreur non gérée (exception):", err?.message || err);
});
