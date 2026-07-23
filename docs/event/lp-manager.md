# LpManager

- **Event**: `LpManager`
- **Checkpoint**: `lp_manager`
- **Deployment address**: `LP_MANAGER`
- **Dependency**: Curve

LpManager events are ordered by block number, transaction index, and log index, grouped by token, and persisted in allocation and collection batches.

## Allocate

Allocate is emitted when liquidity is assigned to a graduated token's pool.

| Field | Meaning |
| --- | --- |
| `token` | token address |
| `pool` | pool address |
| `quoteAmount` | allocated quote amount |
| `tokenAmount` | allocated token amount |
| `timestamp` | allocation timestamp |

Receive processing writes the allocation history through the LP controller.

## Collect

Collect reports the quote and token fees collected from the pool.

| Field | Meaning |
| --- | --- |
| `token` | token address |
| `pool` | pool address |
| `quoteAmount` | collected quote-token amount |
| `tokenAmount` | collected launch-token amount |
| `timestamp` | collection timestamp |

Allocation and collection batches are submitted concurrently for each token.
