# GIWA Observer

GIWA Observer indexes active GIWA contract events and shared price/token streams into PostgreSQL, with Redis-backed caches and Prometheus metrics.

## Runtime contract

The runtime starts exactly seven event handlers. Their checkpoint names are stable public identifiers.

| Handler | Source | Checkpoint |
| --- | --- | --- |
| Curve | BondingCurve | `curve` |
| Dex | GIWA canonical Uniswap V3 pool + YachaRouter RouterBuy/RouterSell(graduated) | `dex` |
| LpManager | LPManager | `lp_manager` |
| Vault | BurnVault, LPVault, CreatorFeeVault, GiftVault, and DividendVault | `vault` |
| VaultRegistry | VaultRegistry | `vault_registry` |
| Token | ERC-20 transfers and balances | `token` |
| Price | quote-token prices | `price` |

PriceUsd has the stable `price_usd` checkpoint and an implementation module, but it is dormant: the runtime does not start a PriceUsd handler.

Each handler follows the same pipeline:

```text
RPC logs/provider data -> Stream -> Channel -> Receive -> PostgreSQL/Redis
```

Price and the admin-driven VaultRegistry stream are independent. Curve waits for Price; Dex, LpManager, and Vault wait for Curve with a one-block dependency offset. Token stays strictly behind Curve so token state cannot overtake token creation.

Detailed event behavior is documented in [docs/event-indexing.md](docs/event-indexing.md), with public module references for [Curve](docs/event/curve.md), [Dex](docs/event/dex.md), [LpManager](docs/event/lp-manager.md), [Vault](docs/event/vault.md), [Dividend](docs/event/dividend.md), and [VaultRegistry](docs/event/vault_registry.md).

## Deployment variables

The active GIWA handlers and fee behavior use these deployment variables:

```dotenv
BONDING_CURVE=0x...
DEX_FACTORY=0x...
YACHA_ROUTER=0x...
LP_MANAGER=0x...
WETH=0x4200000000000000000000000000000000000006
# Optional vault and registry contracts
BURN_VAULT=0x...
LP_VAULT=0x...
CREATOR_FEE_VAULT=0x...
GIFT_VAULT=0x...
DIVIDEND_VAULT=0x...
VAULT_REGISTRY=0x...
MAIN_RPC_URL=...
SUB_RPC_URL_1=...
SUB_RPC_URL_2=...
MODE=testnet
DEPLOY_FE_AMOUNT=...
GRADUATE_FEE_AMOUNT=...
BONDING_CURVE_FEE_RATE=...
DEX_ROUTER_FEE_RATE=...
```

See [.env.example](.env.example) for the full variable list.

Address values are parsed and normalized at startup. Missing or invalid required addresses fail fast. Vault and registry addresses are optional so deployments without those contracts still boot.

## Database write contract

GIWA writes use:

- market values `CURVE` and `DEX`
- Curve fee values `curve_buy` and `curve_sell`

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
