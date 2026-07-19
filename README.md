# Nad.fun Observer

Nad.fun Observer indexes the active GIWA contract events and common price/token streams into PostgreSQL, with Redis-backed caches and Prometheus metrics.

Deployment status, required configuration, database constraints, and known baseline issues are summarized in [HANDOFF.md](HANDOFF.md).

## Runtime contract

The runtime starts exactly six generic event handlers. Checkpoint names are stable public identifiers; implementation versions describe the selected contract ABI, not separate runtime streams.

| Event | Contract implementation | Checkpoint |
| --- | --- | --- |
| Curve | v2 BondingCurve ABI | `curve` |
| Dex | v1 Capricorn DEX ABI | `dex` |
| LpManager | v1 LPManager ABI | `lp_manager` |
| Token | common ERC-20 stream | `token` |
| Price | common quote-price stream | `price` |
| PriceUsd | common token-USD stream | `price_usd` |

Each handler follows the same pipeline:

```text
RPC logs/provider data -> Stream -> Channel -> Receive -> PostgreSQL/Redis
```

Price and PriceUsd are independent. Curve waits for Price; Dex and LpManager wait for Curve; Token stays strictly behind Curve so token state cannot overtake token creation.

Detailed event behavior is documented in [docs/event-indexing.md](docs/event-indexing.md), with public module references for [Curve](docs/event/curve.md), [Dex](docs/event/dex.md), and [LpManager](docs/event/lp-manager.md).

## Deployment variables

The selected GIWA contract implementations and fee behavior use these deployment variables:

```dotenv
BONDING_CURVE=0x...
DEX_FACTORY=0x...
DEX_ROUTER=0x...
LP_MANAGER=0x...
WETH=0x4200000000000000000000000000000000000006
MAIN_RPC_URL=...
SUB_RPC_URL_1=...
SUB_RPC_URL_2=...
MODE=testnet
CREATE_FEE_AMOUNT=...
GRADUATE_FEE_AMOUNT=...
BONDING_CURVE_FEE_RATE=...
DEX_ROUTER_FEE_RATE=...
```

See [.env.example](.env.example) for the full variable list.

Address values are parsed and normalized at startup. Missing or invalid required addresses fail fast.

## Database write contract

New GIWA writes use:

- `token.version='V2'`
- `token.chain='GIWA'`
- market values `NADFUN` and `UNISWAPV3`
- Curve fee values `curve_buy` and `curve_sell`

Existing MON rows and existing versioned database values are intentionally unchanged. The feature does not rewrite historical data.

## Requirements and commands

- Rust 2024 edition toolchain
- PostgreSQL
- Redis
- GIWA-compatible RPC endpoints

Run the observer with:

```bash
cargo run --release
```

Run the focused library and runtime-contract checks with:

```bash
cargo test --lib
cargo test --test giwa_runtime_contract
```

Prometheus metrics are exposed at `/metrics` on the configured metrics server.

## License

This project is private software. All rights reserved.
