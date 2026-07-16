# Event indexing guide

Observer indexes six public GIWA event streams. Events within a batch are ordered by `(block_number, transaction_index, log_index)` before receive-side processing.

## Active streams

| Event | Contract implementation | Checkpoint |
| --- | --- | --- |
| Curve | v2 BondingCurve ABI | `curve` |
| Dex | v1 Capricorn DEX ABI | `dex` |
| LpManager | v1 LPManager ABI | `lp_manager` |
| Token | common ERC-20 stream | `token` |
| Price | common quote-price stream | `price` |
| PriceUsd | common token-USD stream | `price_usd` |

Versioned handler names are implementation details. Runtime coordination, checkpoints, and operational metrics use the generic Event and Checkpoint values above.

## Stream and receive ordering

```text
Price --------> Curve --------> Dex
                    |----------> LpManager
                    |----------> Token (strict wait)

PriceUsd (independent)
```

- Price and PriceUsd do not wait for another event stream.
- Curve waits for Price with a one-block dependency offset.
- Dex and LpManager wait for Curve with a one-block dependency offset.
- Token strictly waits for Curve and remains behind the Curve stream.

## Deployment variables

Only the active implementation selectors and fee values are documented as deployment variables:

```dotenv
BONDING_CURVE=0x...
DEX_FACTORY=0x...
DEX_ROUTER=0x...
LP_MANAGER=0x...
CREATE_FEE_AMOUNT=...
GRADUATE_FEE_AMOUNT=...
BONDING_CURVE_FEE_RATE=...
DEX_ROUTER_FEE_RATE=...
```

## Persistence contract

GIWA event processing writes `token.version='V2'` and `token.chain='GIWA'`. Market values are `CURVE` while a token trades on the bonding curve and `DEX` after graduation or for Dex trades. Curve fee history uses `curve_buy` and `curve_sell`.

Existing MON rows and existing versioned database values are intentionally unchanged. No historical row rewrite is part of this runtime selection.

## Event behavior

### Curve

Curve indexes Create, Buy, Sell, Sync, Graduate, and SnipingPenalty events from `BONDING_CURVE`. Create initializes token, market, chart, point, and fee-history data. Buy and Sell write swaps, chart volume, points, and fee history. Sync updates price and reserves. Graduate moves the market to `DEX` and registers the pool. SnipingPenalty records its penalty history.

See [Curve](event/curve.md) for fields and processing detail.

### Dex

Dex indexes concentrated-liquidity pool Swap, Mint, Burn, and SetFeeProtocol events plus router buy/sell events. Only known token pools are processed. Swap parsing synthesizes reserve/price state used by receive-side swap, market, chart, point, and fee-history writes.

See [Dex](event/dex.md) for fields and processing detail.

### LpManager

LpManager indexes allocation and collection events from `LP_MANAGER`. Collection processing reads treasury fee rates from the contract and persists the calculated creator, foundation, and community portions.

See [LpManager](event/lp-manager.md) for fields and processing detail.

### Token

Token consumes common ERC-20 transfer/burn activity for whitelisted tokens, excludes configured system addresses, and updates balance/position history after Curve has created the token state.

### Price

Price loads quote-token configuration from the database, obtains quote prices, and maintains block-addressable price data used by Curve and Dex USD calculations.

### PriceUsd

PriceUsd obtains token-USD prices through the common provider, groups them into deterministic block buckets, and persists the token price history independently of the contract event streams.
