# Curve

- **Event**: `Curve`
- **Checkpoint**: `curve`
- **Deployment address**: `BONDING_CURVE`
- **Dependency**: Price

> Implementation provenance: the active Curve stream uses `abi/v2/BondingCurve.json`.

Curve events are sorted by block number, transaction index, and log index, grouped by token, and written in batches.

## Create

Create announces a new token and its bonding-curve market.

| Field | Meaning |
| --- | --- |
| `creator` | token creator |
| `token` | token address |
| `pair` | pool used after graduation |
| `quoteToken` | quote-token address |
| `name`, `symbol`, `tokenURI` | public metadata |
| `virtualQuoteReserve`, `virtualTokenReserve` | initial pricing reserves |
| `minTokenReserve` | graduation threshold reserve |

Stream processing validates the configured vanity suffix, resolves the transaction actor, and registers the token, pool, creator, and quote-token cache entries. Receive processing calculates the initial quote price and writes token, market, chart, point, and fee-history rows. Create and graduate point values use `CREATE_FEE_AMOUNT` and `GRADUATE_FEE_AMOUNT`, respectively.

## Buy and Sell

Buy contains `token`, `buyer`, `quoteIn`, and `tokenOut`. Sell contains `token`, `seller`, `tokenIn`, and `quoteOut`.

The stream resolves the effective actor. Receive processing associates the nearest earlier Sync reserve in the same transaction, converts quote value to USD, and writes swap, chart, point, and fee-history data. The token-specific cached fee configuration is preferred; `BONDING_CURVE_FEE_RATE` is the fallback. Fee history uses `curve_buy` for Buy and `curve_sell` for Sell.

## Sync

Sync carries real and virtual quote/token reserves. The virtual reserve ratio determines the quote price. Receive processing updates market reserves, price, all-time-high values, and chart history; the same reserve snapshot is used by nearby Buy and Sell events.

## Graduate

Graduate carries `token` and `pair`. It registers the pool mapping, changes the market value to `DEX`, creates the pool row, and grants the creator's graduate point when the creator is known.

## SnipingPenalty

SnipingPenalty carries `token`, `buyer`, `snipingFee`, and `penaltyBps`. Receive processing writes the sniping-penalty history.
