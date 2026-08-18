import "dotenv/config";
import http from "http";
import WebSocket from "ws";
import fetch from "node-fetch";
import bs58 from "bs58";
import { Connection, Keypair, VersionedTransaction } from "@solana/web3.js";

// ─── Config ───────────────────────────────────────────────────────────────────
const CFG = {
  privateKey:            process.env.SOLANA_PRIVATE_KEY || "",
  rpcUrl:                process.env.RPC_URL || "https://api.mainnet-beta.solana.com",
  buyAmountSol:          parseFloat(process.env.BUY_AMOUNT_SOL          || "0.01"),
  takeProfitPct:         parseFloat(process.env.TAKE_PROFIT_PCT          || "80"),
  stopLossPct:           parseFloat(process.env.STOP_LOSS_PCT            || "20"),
  maxHoldMinutes:        parseFloat(process.env.MAX_HOLD_MINUTES         || "15"),
  deadTokenMinutes:      parseFloat(process.env.DEAD_TOKEN_TIMEOUT_MINUTES || "2"),
  slippagePct:           parseFloat(process.env.SLIPPAGE_PCT             || "15"),
  priorityFeeSol:        parseFloat(process.env.PRIORITY_FEE_SOL        || "0.0001"),
  dryRun:               (process.env.DRY_RUN || "true").toLowerCase() !== "false",
  maxOpenPositions:      parseInt  (process.env.MAX_OPEN_POSITIONS       || "1", 10),
  paperBalanceSol:       parseFloat(process.env.PAPER_STARTING_BALANCE_SOL || "1"),
  ntfyTopic:             process.env.NTFY_TOPIC || "pump_fun_bot",
  pollIntervalSec:       parseFloat(process.env.POLL_INTERVAL_SECONDS    || "10"),
  minMarketCapUsd:       parseFloat(process.env.MIN_MARKET_CAP_USD       || "5000"),
  maxMarketCapUsd:       parseFloat(process.env.MAX_MARKET_CAP_USD       || "200000"),
  pumpFunFeePct: 1, // fee natif pump.fun incompressible
};

// ─── Wallet / RPC ─────────────────────────────────────────────────────────────
let keypair = null;
if (CFG.privateKey) {
  try {
    keypair = Keypair.fromSecretKey(bs58.decode(CFG.privateKey));
    log(`Wallet: ${keypair.publicKey.toBase58()}`);
  } catch {
    log("SOLANA_PRIVATE_KEY invalide, DRY_RUN obligatoire.");
  }
}
const connection = new Connection(CFG.rpcUrl, "confirmed");

// ─── State ────────────────────────────────────────────────────────────────────
// positions: mint → { name, symbol, entryMcapUsd, lastMcapUsd, lastActivityAt, buyTimestamp }
const positions = new Map();
const boughtMints = new Set(); // évite de re-acheter un token déjà passé

// paper trading
const paper = {
  balance: CFG.paperBalanceSol,
  start:   CFG.paperBalanceSol,
  trades:  [],
};

// ─── Logging ──────────────────────────────────────────────────────────────────
function log(...a) {
  console.log(`[${new Date().toISOString()}]`, ...a);
}

// ─── Notifications (ntfy.sh) ──────────────────────────────────────────────────
async function notify(title, message) {
  if (!CFG.ntfyTopic) return;
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 8000);
    const res = await fetch("https://ntfy.sh/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic: CFG.ntfyTopic, title, message, priority: 3 }),
      signal: ctrl.signal,
    });
    clearTimeout(t);
    if (!res.ok) log(`ntfy erreur status=${res.status}`);
  } catch (err) {
    const d = err.cause?.code || err.cause?.message || err.message || String(err);
    log("ntfy erreur:", d);
  }
}

// ─── Trade (achat / vente via PumpPortal Local Trading API) ──────────────────
async function trade(action, mint, amount, denominatedInSol) {
  if (CFG.dryRun) {
    log(`[DRY_RUN] ${action.toUpperCase()} ${mint} amount=${amount} sol=${denominatedInSol}`);
    return { simulated: true };
  }
  if (!keypair) { log("Pas de clé privée, impossible de trader."); return null; }

  try {
    const res = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        publicKey: keypair.publicKey.toBase58(),
        action,
        mint,
        amount,
        denominatedInSol: denominatedInSol ? "true" : "false",
        slippage: CFG.slippagePct,
        priorityFee: CFG.priorityFeeSol,
        pool: "pump",
      }),
    });
    if (res.status !== 200) { log(`API trade-local erreur (${res.status})`); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await res.arrayBuffer()));
    tx.sign([keypair]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, maxRetries: 3 });
    log(`TX: https://solscan.io/tx/${sig}`);
    return { signature: sig };
  } catch (err) {
    log("Erreur trade:", err.message);
    return null;
  }
}

// ─── Achat ────────────────────────────────────────────────────────────────────
async function buyToken(token) {
  if (positions.size >= CFG.maxOpenPositions) return;
  if (boughtMints.has(token.mint)) return;

  if (CFG.dryRun && paper.balance < CFG.buyAmountSol) {
    log(`[PAPER] Solde insuffisant (${paper.balance.toFixed(4)} SOL), achat ignoré.`);
    return;
  }

  log(`ACHAT → ${token.name} (${token.symbol}) mcap=$${Math.round(token.usd_market_cap).toLocaleString()} mint=${token.mint}`);
  notify(`🟢 Achat: ${token.symbol}`, `${token.name} | $${Math.round(token.usd_market_cap).toLocaleString()} mcap | ${CFG.buyAmountSol} SOL`);

  const result = await trade("buy", token.mint, CFG.buyAmountSol, true);
  if (!result) return;

  boughtMints.add(token.mint);
  if (CFG.dryRun) paper.balance -= CFG.buyAmountSol;

  positions.set(token.mint, {
    name:           token.name,
    symbol:         token.symbol,
    entryMcapUsd:   token.usd_market_cap,
    lastMcapUsd:    token.usd_market_cap,
    buyTimestamp:   Date.now(),
    lastActivityAt: Date.now(),
  });

  subscribePosition(token.mint);
}

// ─── Vente ────────────────────────────────────────────────────────────────────
async function sellToken(mint, reason, changePct = 0) {
  const pos = positions.get(mint);
  if (!pos) return;
  positions.delete(mint); // retire en premier pour éviter double-sell

  log(`VENTE → ${pos.symbol} raison=${reason} pnl=${changePct >= 0 ? "+" : ""}${changePct.toFixed(1)}%`);

  await trade("sell", mint, "100%", false);

  if (CFG.dryRun) {
    const gross  = CFG.buyAmountSol * (1 + changePct / 100);
    const fees   = CFG.buyAmountSol * (CFG.pumpFunFeePct * 2 / 100);
    const net    = Math.max(0, gross - fees);
    const pnlSol = net - CFG.buyAmountSol;
    const pnlPct = (pnlSol / CFG.buyAmountSol) * 100;
    paper.balance += net;
    paper.trades.push({ pnlSol, pnlPct, reason });
    log(`[PAPER] Clôture ${pos.symbol}: ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL (${pnlPct.toFixed(1)}%) | solde=${paper.balance.toFixed(4)} SOL`);
    notify(
      `${pnlSol >= 0 ? "🟢" : "🔴"} Vente: ${pos.symbol} ${pnlSol >= 0 ? "+" : ""}${pnlPct.toFixed(1)}%`,
      `${reason} | PnL: ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL | Solde: ${paper.balance.toFixed(4)} SOL`
    );
  } else {
    notify(`💰 Vente: ${pos.symbol}`, `${reason} | pnl≈${changePct >= 0 ? "+" : ""}${changePct.toFixed(1)}%`);
  }
}

// ─── Check temps de hold / token mort (boucle indépendante du WS) ─────────────
function checkHoldTimes() {
  const now = Date.now();
  for (const [mint, pos] of positions.entries()) {
    const heldMin     = (now - pos.buyTimestamp)   / 60000;
    const inactiveMin = (now - pos.lastActivityAt) / 60000;
    const mcapRef     = pos.lastMcapSol ?? 0;
    const changePct   = pos.entryMcapSol
      ? ((mcapRef - pos.entryMcapSol) / pos.entryMcapSol) * 100
      : 0;

    if (CFG.deadTokenMinutes > 0 && inactiveMin >= CFG.deadTokenMinutes) {
      sellToken(mint, `inactif ${inactiveMin.toFixed(1)}min`, changePct);
    } else if (CFG.maxHoldMinutes > 0 && heldMin >= CFG.maxHoldMinutes) {
      sellToken(mint, `max-hold ${heldMin.toFixed(1)}min`, changePct);
    }
  }
}

// ─── Suivi prix d'une position ouverte (WS PumpPortal) ───────────────────────
function subscribePosition(mint) {
  const ws = new WebSocket("wss://pumpportal.fun/api/data");

  ws.on("open", () => {
    ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  });

  ws.on("message", (raw) => {
    if (!positions.has(mint)) { ws.close(); return; }
    let d;
    try { d = JSON.parse(raw.toString()); } catch { return; }

    // PumpPortal WS expose le mcap en SOL via vSolInBondingCurve ou marketCapSol
    const mcapSol = d.vSolInBondingCurve ?? d.marketCapSol;
    if (mcapSol === undefined || mcapSol <= 0) return;

    const pos = positions.get(mint);
    if (!pos) return;

    // Calibre la référence SOL au premier message reçu (entrée = vrai prix exécuté)
    if (!pos.entryMcapSol) {
      pos.entryMcapSol = mcapSol;
      log(`[WS] Référence SOL calibrée pour ${pos.symbol}: ${mcapSol.toFixed(4)} SOL`);
    }

    pos.lastMcapSol    = mcapSol;
    pos.lastActivityAt = Date.now();

    const changePct = ((mcapSol - pos.entryMcapSol) / pos.entryMcapSol) * 100;

    if (changePct >= CFG.takeProfitPct) {
      sellToken(mint, `TP +${changePct.toFixed(1)}%`, changePct);
    } else if (changePct <= -CFG.stopLossPct) {
      sellToken(mint, `SL ${changePct.toFixed(1)}%`, changePct);
    }
  });

  ws.on("close", () => {
    if (positions.has(mint)) setTimeout(() => subscribePosition(mint), 3000);
  });

  ws.on("error", () => {});
}

// ─── Récupération trending via pump.fun API ───────────────────────────────────
// Trie par last_trade_unix_time DESC = les tokens avec le trade le plus récent en premier.
// C'est ce que fait pump.fun "trending" dans l'app (activité la plus fraîche).
async function fetchTrending() {
  try {
    const url = "https://frontend-api.pump.fun/coins" +
      "?limit=20&sort=last_trade_timestamp&order=DESC&offset=0&includeNsfw=false";
    const res = await fetch(url, {
      headers: { "Accept": "application/json", "User-Agent": "Mozilla/5.0" },
    });
    if (!res.ok) { log(`pump.fun API erreur status=${res.status}`); return null; }
    const coins = await res.json();
    return Array.isArray(coins) ? coins : null;
  } catch (err) {
    log("Erreur fetchTrending:", err.message);
    return null;
  }
}

// ─── Boucle principale : polling trending + achat si opportunité ───────────────
async function runTrendingLoop() {
  if (positions.size >= CFG.maxOpenPositions) return; // position déjà ouverte, on attend

  const coins = await fetchTrending();
  if (!coins || coins.length === 0) return;

  // Filtre: market cap dans la plage, pas déjà acheté/en position, pas rugged (still_on_pump)
  const candidates = coins.filter(c =>
    c.usd_market_cap >= CFG.minMarketCapUsd &&
    c.usd_market_cap <= CFG.maxMarketCapUsd &&
    !boughtMints.has(c.mint) &&
    !positions.has(c.mint) &&
    c.complete === false // token pas encore gradué (toujours en bonding curve pump.fun)
  );

  if (candidates.length === 0) {
    log(`Trending: aucun candidat dans la plage $${CFG.minMarketCapUsd.toLocaleString()}–$${CFG.maxMarketCapUsd.toLocaleString()}`);
    return;
  }

  // Meilleur candidat = le plus haut market cap dans la plage (= le plus chaud du moment)
  // On pourrait aussi trier par last_trade_unix_time mais l'API le fait déjà,
  // donc le premier qui passe le filtre est déjà le plus récemment actif
  const best = candidates[0];
  log(`Meilleur trending: ${best.symbol} mcap=$${Math.round(best.usd_market_cap).toLocaleString()} mint=${best.mint}`);

  await buyToken({
    mint:          best.mint,
    name:          best.name  || best.symbol,
    symbol:        best.symbol,
    usd_market_cap: best.usd_market_cap,
  });
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
function logPaperStats() {
  const n = paper.trades.length;
  const wins = paper.trades.filter(t => t.pnlSol > 0).length;
  const totalPnl = paper.balance - paper.start;
  log(`[PAPER] solde=${paper.balance.toFixed(4)} SOL (${totalPnl >= 0 ? "+" : ""}${totalPnl.toFixed(4)}) | trades=${n} | winrate=${n ? ((wins/n)*100).toFixed(0) : 0}%`);
}

log("=== Bot pump.fun démarré (stratégie: trending API) ===");
log(`DRY_RUN=${CFG.dryRun} | buy=${CFG.buyAmountSol} SOL | TP=${CFG.takeProfitPct}% | SL=${CFG.stopLossPct}% | mcap=$${CFG.minMarketCapUsd}–$${CFG.maxMarketCapUsd}`);
if (CFG.dryRun) log(`[PAPER] Solde fictif de départ = ${paper.balance.toFixed(4)} SOL`);
notify("🤖 Bot démarré", `Stratégie trending | DRY_RUN=${CFG.dryRun} | TP=${CFG.takeProfitPct}% SL=${CFG.stopLossPct}%`);

// boucle principale: poll API trending toutes les POLL_INTERVAL_SECONDS
setInterval(runTrendingLoop, CFG.pollIntervalSec * 1000);
runTrendingLoop(); // premier appel immédiat

// boucle de sécurité: check hold-time / token mort toutes les 30s
setInterval(checkHoldTimes, 30 * 1000);

// heartbeat toutes les 5min
const bootTime = Date.now();
setInterval(() => {
  const uptimeMin = Math.round((Date.now() - bootTime) / 60000);
  const bal = CFG.dryRun ? ` | solde=${paper.balance.toFixed(4)} SOL` : "";
  notify("⏱️ Bot actif", `Uptime: ${uptimeMin}min | positions=${positions.size}${bal}`);
  if (CFG.dryRun) logPaperStats();
}, 5 * 60 * 1000);

// serveur HTTP pour Railway healthcheck
const port = process.env.PORT || 3000;
http.createServer((req, res) => {
  res.writeHead(200);
  res.end(`pumpfun-bot OK | uptime=${Math.round((Date.now()-bootTime)/60000)}min | positions=${positions.size}`);
}).listen(port, () => log(`Healthcheck HTTP port ${port}`));

// arrêt propre sur signal Railway
function shutdown(sig) {
  log(`Signal ${sig}, arrêt propre.`);
  if (CFG.dryRun) logPaperStats();
  notify("🛑 Bot arrêté", `Signal ${sig}`);
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("unhandledRejection", err => log("UnhandledRejection:", err?.message || err));
process.on("uncaughtException",  err => log("UncaughtException:",  err?.message || err));
