import "dotenv/config";
import http from "http";
import WebSocket from "ws";
import fetch from "node-fetch";
import bs58 from "bs58";
import { Connection, Keypair, VersionedTransaction } from "@solana/web3.js";

// ─── Config ───────────────────────────────────────────────────────────────────
const CFG = {
  privateKey:       process.env.SOLANA_PRIVATE_KEY || "",
  rpcUrl:           process.env.RPC_URL || "https://api.mainnet-beta.solana.com",
  buyAmountSol:     parseFloat(process.env.BUY_AMOUNT_SOL          || "0.01"),
  takeProfitPct:    parseFloat(process.env.TAKE_PROFIT_PCT          || "80"),
  stopLossPct:      parseFloat(process.env.STOP_LOSS_PCT            || "20"),
  maxHoldMinutes:   parseFloat(process.env.MAX_HOLD_MINUTES         || "15"),
  deadTokenMinutes: parseFloat(process.env.DEAD_TOKEN_TIMEOUT_MINUTES || "2"),
  slippagePct:      parseFloat(process.env.SLIPPAGE_PCT             || "15"),
  priorityFeeSol:   parseFloat(process.env.PRIORITY_FEE_SOL        || "0.0001"),
  dryRun:          (process.env.DRY_RUN || "true").toLowerCase() !== "false",
  maxOpenPositions: parseInt(process.env.MAX_OPEN_POSITIONS         || "1", 10),
  paperBalance:     parseFloat(process.env.PAPER_STARTING_BALANCE_SOL || "1"),
  ntfyTopic:        process.env.NTFY_TOPIC || "pump_fun_bot",
  // fenêtre d'observation trending (secondes) avant de choisir le meilleur token
  windowSec:        parseFloat(process.env.CANDIDATE_WINDOW_SECONDS || "20"),
  // seuil minimum de SOL dans la bonding curve pour qu'un token soit éligible
  minSolInCurve:    parseFloat(process.env.MIN_SOL_IN_CURVE         || "5"),
  pumpFeeRate: 0.01, // 1% fee pump.fun par sens
};

// ─── Wallet ───────────────────────────────────────────────────────────────────
let keypair = null;
if (CFG.privateKey) {
  try { keypair = Keypair.fromSecretKey(bs58.decode(CFG.privateKey)); }
  catch { log("SOLANA_PRIVATE_KEY invalide."); }
}
const connection = new Connection(CFG.rpcUrl, "confirmed");

// ─── State ────────────────────────────────────────────────────────────────────
// positions: mint → { name, symbol, entryMcapSol, lastMcapSol, buyTimestamp, lastActivityAt }
const positions  = new Map();
const boughtMints = new Set();
// candidats en observation: mint → { name, symbol, solInCurve, buyCount, seenAt }
const candidates = new Map();

// paper
const paper = { balance: CFG.paperBalance, start: CFG.paperBalance, trades: [] };

// ─── Log ─────────────────────────────────────────────────────────────────────
function log(...a) { console.log(`[${new Date().toISOString()}]`, ...a); }

// ─── Notify ───────────────────────────────────────────────────────────────────
async function notify(title, msg) {
  if (!CFG.ntfyTopic) return;
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 8000);
    const res = await fetch("https://ntfy.sh/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic: CFG.ntfyTopic, title, message: msg, priority: 3 }),
      signal: ctrl.signal,
    });
    clearTimeout(t);
    if (!res.ok) log(`ntfy status=${res.status}`);
  } catch (err) {
    log("ntfy err:", err.cause?.code || err.message);
  }
}

// ─── Trade ────────────────────────────────────────────────────────────────────
async function trade(action, mint, amount, denominatedInSol) {
  if (CFG.dryRun) {
    log(`[DRY] ${action.toUpperCase()} ${mint} amount=${amount}`);
    return { ok: true };
  }
  if (!keypair) { log("Pas de clé."); return null; }
  try {
    const res = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        publicKey: keypair.publicKey.toBase58(),
        action, mint, amount,
        denominatedInSol: denominatedInSol ? "true" : "false",
        slippage: CFG.slippagePct,
        priorityFee: CFG.priorityFeeSol,
        pool: "pump",
      }),
    });
    if (res.status !== 200) { log(`trade-local ${res.status}`); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await res.arrayBuffer()));
    tx.sign([keypair]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, maxRetries: 3 });
    log(`TX: https://solscan.io/tx/${sig}`);
    return { sig };
  } catch (err) { log("trade err:", err.message); return null; }
}

// ─── Buy ─────────────────────────────────────────────────────────────────────
async function buyToken(token) {
  if (positions.size >= CFG.maxOpenPositions) return;
  if (boughtMints.has(token.mint)) return;
  if (CFG.dryRun && paper.balance < CFG.buyAmountSol) {
    log(`[PAPER] Solde insuffisant (${paper.balance.toFixed(4)} SOL)`);
    return;
  }

  log(`ACHAT → ${token.name} (${token.symbol}) solInCurve=${token.solInCurve?.toFixed(2)} SOL | buyCount=${token.buyCount} | mint=${token.mint}`);
  notify(`🟢 Achat: ${token.symbol}`, `${token.name} | ${token.solInCurve?.toFixed(2)} SOL in curve | ${token.buyCount} achats en ${CFG.windowSec}s`);

  const result = await trade("buy", token.mint, CFG.buyAmountSol, true);
  if (!result) return;

  boughtMints.add(token.mint);
  if (CFG.dryRun) paper.balance -= CFG.buyAmountSol;

  positions.set(token.mint, {
    name:           token.name,
    symbol:         token.symbol,
    entryMcapSol:   null, // calibré au 1er message WS reçu
    lastMcapSol:    token.solInCurve || 0,
    buyTimestamp:   Date.now(),
    lastActivityAt: Date.now(),
  });

  subscribePosition(token.mint);
}

// ─── Sell ─────────────────────────────────────────────────────────────────────
async function sellToken(mint, reason, changePct = 0) {
  const pos = positions.get(mint);
  if (!pos) return;
  positions.delete(mint);

  log(`VENTE → ${pos.symbol} | ${reason} | pnl≈${changePct >= 0 ? "+" : ""}${changePct.toFixed(1)}%`);
  await trade("sell", mint, "100%", false);

  if (CFG.dryRun) {
    const gross  = CFG.buyAmountSol * (1 + changePct / 100);
    const fees   = CFG.buyAmountSol * CFG.pumpFeeRate * 2;
    const net    = Math.max(0, gross - fees);
    const pnlSol = net - CFG.buyAmountSol;
    const pnlPct = (pnlSol / CFG.buyAmountSol) * 100;
    paper.balance += net;
    paper.trades.push({ pnlSol, reason });
    log(`[PAPER] ${pos.symbol}: ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL (${pnlPct.toFixed(1)}%) | solde=${paper.balance.toFixed(4)}`);
    notify(
      `${pnlSol >= 0 ? "🟢" : "🔴"} Vente: ${pos.symbol} ${pnlSol >= 0 ? "+" : ""}${pnlPct.toFixed(1)}%`,
      `${reason} | ${pnlSol >= 0 ? "+" : ""}${pnlSol.toFixed(4)} SOL | Solde: ${paper.balance.toFixed(4)} SOL`
    );
  } else {
    notify(`💰 Vente: ${pos.symbol}`, `${reason} | ≈${changePct >= 0 ? "+" : ""}${changePct.toFixed(1)}%`);
  }
}

// ─── Suivi prix (WS par position) ────────────────────────────────────────────
function subscribePosition(mint) {
  const ws = new WebSocket("wss://pumpportal.fun/api/data");
  ws.on("open", () => ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] })));
  ws.on("message", (raw) => {
    if (!positions.has(mint)) { ws.close(); return; }
    let d; try { d = JSON.parse(raw.toString()); } catch { return; }
    const mcapSol = d.vSolInBondingCurve ?? d.marketCapSol;
    if (!mcapSol || mcapSol <= 0) return;
    const pos = positions.get(mint);
    if (!pos) return;
    if (!pos.entryMcapSol) {
      pos.entryMcapSol = mcapSol;
      log(`[WS] ${pos.symbol} ref calibrée: ${mcapSol.toFixed(4)} SOL`);
    }
    pos.lastMcapSol    = mcapSol;
    pos.lastActivityAt = Date.now();
    const pct = ((mcapSol - pos.entryMcapSol) / pos.entryMcapSol) * 100;
    if      (pct >= CFG.takeProfitPct)  sellToken(mint, `TP +${pct.toFixed(1)}%`, pct);
    else if (pct <= -CFG.stopLossPct)   sellToken(mint, `SL ${pct.toFixed(1)}%`,  pct);
  });
  ws.on("close", () => { if (positions.has(mint)) setTimeout(() => subscribePosition(mint), 3000); });
  ws.on("error", () => {});
}

// ─── Check hold-time (boucle indépendante du WS) ─────────────────────────────
function checkHoldTimes() {
  const now = Date.now();
  for (const [mint, pos] of positions.entries()) {
    const heldMin     = (now - pos.buyTimestamp)   / 60000;
    const inactiveMin = (now - pos.lastActivityAt) / 60000;
    const pct = pos.entryMcapSol
      ? ((pos.lastMcapSol - pos.entryMcapSol) / pos.entryMcapSol) * 100
      : 0;
    if (CFG.deadTokenMinutes > 0 && inactiveMin >= CFG.deadTokenMinutes)
      sellToken(mint, `inactif ${inactiveMin.toFixed(1)}min`, pct);
    else if (CFG.maxHoldMinutes > 0 && heldMin >= CFG.maxHoldMinutes)
      sellToken(mint, `max-hold ${heldMin.toFixed(1)}min`, pct);
  }
}

// ─── Résolution candidats (fenêtre écoulée → achète le meilleur) ──────────────
function resolveCandidates() {
  if (positions.size >= CFG.maxOpenPositions) { candidates.clear(); return; }
  const now = Date.now();
  let best = null;

  for (const [mint, cand] of candidates.entries()) {
    const ageSec = (now - cand.seenAt) / 1000;
    if (ageSec < CFG.windowSec) continue; // fenêtre pas encore écoulée

    // éligible si solInCurve >= seuil (= assez d'argent entré dans le token)
    if (cand.solInCurve >= CFG.minSolInCurve) {
      if (!best || cand.solInCurve > best.cand.solInCurve ||
         (cand.solInCurve === best.cand.solInCurve && cand.buyCount > best.cand.buyCount)) {
        best = { mint, cand };
      }
    }
    candidates.delete(mint); // retire qu'il soit élu ou non
  }

  if (!best) return;
  log(`Trending élu: ${best.cand.symbol} | ${best.cand.solInCurve.toFixed(2)} SOL | ${best.cand.buyCount} achats`);
  buyToken({ mint: best.mint, ...best.cand });
}

// ─── WebSocket PumpPortal (nouveaux tokens + scoring candidats) ───────────────
let scoreWs = null;

function connectMainSocket() {
  const ws = new WebSocket("wss://pumpportal.fun/api/data");

  ws.on("open", () => {
    log("PumpPortal WS connecté, subscribe nouveaux tokens...");
    ws.send(JSON.stringify({ method: "subscribeNewToken" }));
  });

  ws.on("message", (raw) => {
    let d; try { d = JSON.parse(raw.toString()); } catch { return; }
    if (!d.mint) return;
    if (candidates.has(d.mint) || positions.has(d.mint) || boughtMints.has(d.mint)) return;

    const solInCurve = d.vSolInBondingCurve ?? d.marketCapSol ?? 0;
    log(`Nouveau token: ${d.symbol} | ${solInCurve.toFixed(2)} SOL in curve`);

    candidates.set(d.mint, {
      name:       d.name   || d.symbol,
      symbol:     d.symbol || "?",
      solInCurve,
      buyCount:   0,
      seenAt:     Date.now(),
    });

    // subscribe au scoring via le WS partagé
    if (scoreWs?.readyState === WebSocket.OPEN)
      scoreWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [d.mint] }));
  });

  ws.on("close", () => { log("Main WS fermé, reconnexion 5s..."); setTimeout(connectMainSocket, 5000); });
  ws.on("error", (e) => log("Main WS err:", e.message));
}

function connectScoreSocket() {
  scoreWs = new WebSocket("wss://pumpportal.fun/api/data");

  scoreWs.on("open", () => {
    log("Score WS connecté.");
    // resouscrit tout (reconnexion)
    const keys = [...candidates.keys(), ...positions.keys()];
    if (keys.length) scoreWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys }));
  });

  scoreWs.on("message", (raw) => {
    let d; try { d = JSON.parse(raw.toString()); } catch { return; }
    if (!d.mint) return;

    // MAJ position ouverte → géré par subscribePosition (WS dédié), rien à faire ici

    // MAJ candidat en cours d'observation
    const cand = candidates.get(d.mint);
    if (!cand) return;

    // ✅ champ correct PumpPortal: isBuy (boolean)
    if (d.isBuy === true) {
      cand.buyCount  += 1;
      cand.solInCurve = d.vSolInBondingCurve ?? cand.solInCurve;
    } else if (d.isBuy === false) {
      cand.solInCurve = d.vSolInBondingCurve ?? cand.solInCurve;
    }
  });

  scoreWs.on("close", () => { log("Score WS fermé, reconnexion 3s..."); setTimeout(connectScoreSocket, 3000); });
  scoreWs.on("error", () => {});
}

// ─── Paper stats ──────────────────────────────────────────────────────────────
function paperStats() {
  const n = paper.trades.length;
  const wins = paper.trades.filter(t => t.pnlSol > 0).length;
  const pnl  = paper.balance - paper.start;
  log(`[PAPER] solde=${paper.balance.toFixed(4)} SOL (${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)}) | trades=${n} | winrate=${n ? Math.round(wins/n*100) : 0}%`);
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
log("=== Bot pump.fun démarré ===");
log(`DRY_RUN=${CFG.dryRun} | buy=${CFG.buyAmountSol} SOL | TP=${CFG.takeProfitPct}% | SL=${CFG.stopLossPct}% | window=${CFG.windowSec}s | minSol=${CFG.minSolInCurve}`);
if (CFG.dryRun) log(`[PAPER] Solde fictif = ${paper.balance.toFixed(4)} SOL`);
notify("🤖 Bot démarré", `DRY_RUN=${CFG.dryRun} | TP=${CFG.takeProfitPct}% SL=${CFG.stopLossPct}%`);

connectMainSocket();
connectScoreSocket();

setInterval(checkHoldTimes,   30_000);
setInterval(resolveCandidates, 1_000);

const bootTime = Date.now();
setInterval(() => {
  const min = Math.round((Date.now() - bootTime) / 60000);
  const bal = CFG.dryRun ? ` | solde=${paper.balance.toFixed(4)} SOL` : "";
  notify("⏱️ Bot actif", `Uptime: ${min}min | positions=${positions.size} | candidats=${candidates.size}${bal}`);
  if (CFG.dryRun) paperStats();
}, 5 * 60_000);

http.createServer((_, res) => {
  res.writeHead(200);
  res.end(`OK | uptime=${Math.round((Date.now()-bootTime)/60000)}min | pos=${positions.size} | cands=${candidates.size}`);
}).listen(process.env.PORT || 3000, () => log(`HTTP port ${process.env.PORT || 3000}`));

process.on("SIGTERM", () => { if (CFG.dryRun) paperStats(); notify("🛑 Arrêt", "SIGTERM"); process.exit(0); });
process.on("SIGINT",  () => { if (CFG.dryRun) paperStats(); process.exit(0); });
process.on("unhandledRejection", e => log("UnhandledRejection:", e?.message || e));
process.on("uncaughtException",  e => log("UncaughtException:",  e?.message || e));
