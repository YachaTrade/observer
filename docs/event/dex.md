# Dex

- **Event**: `Dex`
- **Checkpoint**: `dex`
- **Deployment addresses**: `DEX_FACTORY`, `DEX_ROUTER`
- **Dependency**: Curve

> Implementation provenance: the active Dex stream uses the v1 Capricorn concentrated-liquidity pool and router ABIs.

Dex processes known token pools. Events are ordered by block number, transaction index, and log index, grouped by token where applicable, and then written in batches.

## Swap

Pool Swap provides `sender`, signed token deltas, `sqrtPriceX96`, liquidity, and tick. Stream processing:

1. rejects pools outside the token-pool whitelist;
2. loads the cached token pair;
3. derives quote/token price and virtual reserves from `sqrtPriceX96` and liquidity;
4. creates a synthetic Sync immediately before the swap for reserve matching;
5. classifies the trade as buy or sell and resolves the effective actor.

Receive processing writes swap, market, chart, point, and fee-history data. The persisted market value is `DEX`.

## Mint and Burn

Mint and Burn carry the owner, liquidity delta, and token amounts. The stream reads the pool state at the event block, calculates quote/token reserves, and persists the resulting liquidity history to the corresponding table.

## SetFeeProtocol

SetFeeProtocol records the old and new protocol fee values for each side of a whitelisted pool. These events are batched separately because they are pool-scoped rather than token-scoped.

## Router buy and sell

Router events are accepted only from `DEX_ROUTER`. They provide the user, token, input, and output amounts. Receive processing writes Dex points and fee history; point and fee calculations use `DEX_ROUTER_FEE_RATE`.
