# Bot trading auto pump.fun (Railway)

Snipe nouveaux tokens pump.fun + vente auto (take-profit / stop-loss / max-hold-time).
API utilisée: **PumpPortal** (WebSocket data gratuit + Local Trading API gratuite — tu paies juste les frais réseau Solana + le fee natif 1% de pump.fun, incompressible quel que soit l'outil).

## ⚠️ Avant tout

- Ceci n'est **pas un conseil financier**. Trading memecoins pump.fun = risque de perte totale quasi systématique (>90% des tokens tombent à zéro). Ce bot ne garantit aucun profit.
- Teste D'ABORD en `DRY_RUN=true` (défaut) pendant plusieurs jours pour observer le comportement sans risquer d'argent réel.
- Ne mets dans le wallet que ce que tu es prêt à perdre entièrement.
- La clé privée dans les variables d'env Railway a accès total au wallet. Utilise un wallet dédié, jamais ton wallet principal.

## Installation locale

```bash
npm install
cp .env.example .env
# remplir .env (clé privée, params stratégie)
npm start
```

## Déploiement Railway

1. Crée un repo Git avec ces fichiers, push sur GitHub.
2. Sur [railway.app](https://railway.app) → New Project → Deploy from GitHub repo.
3. Railway détecte Node.js automatiquement (Nixpacks) et lance `npm start`.
4. Dans l'onglet **Variables**, ajoute toutes les clés de `.env.example` (au minimum `SOLANA_PRIVATE_KEY` et `DRY_RUN`).
5. Coût: le plan gratuit Railway (Trial/Hobby avec crédit offert) suffit pour un petit bot léger. Au-delà du crédit gratuit, Railway facture à l'usage — vérifie leur pricing actuel si tu veux du 100% 0€ dans la durée.

## Paramètres clés (.env)

| Variable | Rôle |
|---|---|
| `BUY_AMOUNT_SOL` | Montant SOL misé par snipe |
| `MAX_MARKET_CAP_SOL` | N'achète que si market cap du token ≤ ce seuil au lancement |
| `TAKE_PROFIT_PCT` / `STOP_LOSS_PCT` | Seuils de sortie auto |
| `MAX_HOLD_MINUTES` | Vente forcée après X minutes si ni TP ni SL touché |
| `SLIPPAGE_PCT` | Tolérance slippage sur les tx |
| `DRY_RUN` | `true` = simulation, aucune tx réelle envoyée |
| `MAX_OPEN_POSITIONS` | Nombre de positions simultanées max |

## Partie "IA"

Le filtre de snipe actuel est basé sur des règles (market cap, blacklist mots-clés). Pas de modèle IA branché par défaut — les API "IA gratuites" fiables pour scorer un token en temps réel n'existent pas vraiment (et aucune ne prédit le prix). Si tu veux enrichir la logique, le point d'extension est la fonction `passesFilter()` dans `index.js` : tu peux y appeler n'importe quelle API externe avant de décider d'acheter.

## Limites connues

- RPC public Solana = rate-limité, peut louper des slots en période de forte charge. Pour un bot sérieux, prends un RPC dédié (Helius/QuickNode ont un free tier).
- Pas de gestion de reconnexion Solana avancée / retry sophistiqué au-delà du basique inclus.
- Aucune protection anti-rug avancée (dev wallet dump, honeypot) — le filtre reste simple.
