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

Collect reports the total quote amount distributed after collecting the pool's
quote fee and swapping its collected launch-token fee into quote.

| Field | Meaning |
| --- | --- |
| `token` | token address |
| `pool` | pool address |
| `quoteAmount` | total distributed quote amount, including swapped launch-token fees |
| `timestamp` | collection timestamp |

Allocation and collection batches are submitted concurrently for each token.
Collection history stores only `quoteAmount` plus canonical transaction and log
coordinates; the event does not expose a separate launch-token amount or
treasury split amounts.
