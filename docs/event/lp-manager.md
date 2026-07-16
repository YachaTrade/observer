# LpManager

- **Event**: `LpManager`
- **Checkpoint**: `lp_manager`
- **Deployment address**: `LP_MANAGER`
- **Dependency**: Curve

> Implementation provenance: the active LpManager stream uses the v1 LPManager ABI.

LpManager events are ordered by block number, transaction index, and log index, grouped by token, and persisted in allocation and collection batches.

## LpManagerAllocate

LpManagerAllocate is emitted when liquidity is assigned to a graduated token's pool.

| Field | Meaning |
| --- | --- |
| `token` | token address |
| `pool` | pool address |
| `monAmount` | allocated quote amount |
| `tokenAmount` | allocated token amount |
| `lastCollectTime` | latest collection timestamp |

Receive processing writes the allocation history through the LP controller.

## LpManagerCollect

LpManagerCollect reports quote and token amounts collected from the pool. The stream reads the manager's creator, foundation, and community treasury fee rates, calculates each quote-token share on the contract's 1,000,000 denominator, and includes those amounts in the collection batch.

Allocation and collection batches are submitted concurrently for each token.
