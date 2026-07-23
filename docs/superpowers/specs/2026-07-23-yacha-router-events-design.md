# YachaRouter and LPManager Event Alignment

## Goal

Align the GIWA observer with the deployed contracts in
`/Users/gyu/project/giwa/new_contract` at commit `8ee03dd`.

## Contract Sources

- `abis/YachaRouter.json`
  - `RouterBuy(address,address,uint256,uint256,bool)`
  - `RouterSell(address,address,uint256,uint256,bool)`
- `abis/LPManager.json`
  - `Allocate(address,address,uint256,uint256,uint256)`
  - `Collect(address,address,uint256,uint256,uint256)`

The observer copies these ABI arrays without editing their event definitions.

## Router Integration

- Rename the active ABI and generated Rust namespace from `GiwaRouter` to
  `YachaRouter`.
- Decode `RouterBuy` and `RouterSell`, retaining the existing
  `graduated=true` filter and downstream `DexRouterBuy`/`DexRouterSell`
  records.
- Rename the address config from `DEX_ROUTER_ADDRESS`/`DEX_ROUTER` to
  `YACHA_ROUTER_ADDRESS`/`YACHA_ROUTER` with no compatibility fallback.
- Keep `DEX_ROUTER_FEE_RATE`; it describes the DEX fee rather than the
  contract name.
- Use deployment address `0x733132B6f0FEbd58D062f61657F1b3dbb2aDEB5A`.

## LPManager Integration

- Replace the stale `ILpManager` ABI with the canonical `LPManager` ABI.
- Decode `Allocate` and `Collect` directly.
- Map contract `timestamp` to the existing domain `last_collect_time` field
  and continue using the canonical block timestamp as database `created_at`.
- Remove the obsolete on-chain `config()` read and creator/foundation/community
  split calculations. The current database still requires those historical
  columns, so persistence supplies numeric zero constants without exposing
  obsolete fields in the Rust event type.
- Preserve the `lp_manager` checkpoint and existing allocation/collection
  table identities.

## Compatibility and Safety

- No checkpoint rename or reset.
- No database migration or destructive schema operation.
- No fallback to old ABI event signatures or the old router env key.
- Update active documentation and runtime-contract tests; historical plan and
  handoff documents remain unchanged.
- Update ignored `.env` and `.env.testnet` mechanically without printing their
  contents.
