# Dex

- **Event**: `Dex`
- **Checkpoint**: `dex`
- **Deployment addresses**: `DEX_FACTORY`, `DEX_ROUTER`
- **Dependency**: Curve

Dex processes known canonical Uniswap V3 token pools and accepts router events only from the configured GiwaRouter address. Events are ordered by block number, transaction index, and log index, grouped by token where applicable, and then written in batches.

## Swap

Canonical Uniswap V3 pool Swap provides `sender`, signed token deltas, `sqrtPriceX96`, liquidity, and tick. Stream processing:

1. rejects pools outside the token-pool whitelist;
2. loads the cached token pair;
3. derives quote/token price and virtual reserves from `sqrtPriceX96` and liquidity;
4. creates a synthetic Sync immediately before the swap for reserve matching;
5. classifies the trade as buy or sell and resolves the effective actor.

Receive processing writes swap, market, chart, point, and fee-history data. The persisted market value is `DEX`.

## Mint and Burn

Mint and Burn carry the owner, liquidity delta, and token amounts. The stream reads the pool state at the event block, calculates quote/token reserves, and persists the resulting liquidity history to the corresponding table.

## Router buy and sell

GiwaRouter Buy/Sell events are accepted only from `DEX_ROUTER`. Buy identifies the user as `buyer`; Sell identifies the user as `seller`. Both provide the token, input and output amounts, and a `graduated` flag. The stream processes only `graduated=true`; `graduated=false` represents bonding-curve trades already indexed by the Curve handler and is skipped to prevent duplicate storage. Receive processing writes Dex points and fee history; point and fee calculations use `DEX_ROUTER_FEE_RATE`.
