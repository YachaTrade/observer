# GIWA Single-Version Indexing Design

## Context

The `giwa` observer reuses the existing database but indexes one GIWA
deployment. GIWA has no public V1/V2 split. It uses the contract behavior from
the existing v2 bonding-curve implementation, the existing v1 DEX
implementation, and the existing v1 LPManager implementation.

Versioned Rust module and ABI paths remain only where they document the source
contract interface. Runtime event names, deployment configuration, and new
market/fee data must not expose that implementation provenance.

The existing database contains MON history, including `V2_CURVE`, `V2_DEX`,
vault, dividend, reward, creator, and distributor data. This change must not
rewrite or delete any existing row or historical migration.

## Decisions

- The active runtime event set is exactly `Curve`, `Dex`, `LpManager`, `Token`,
  `Price`, and `PriceUsd`.
- `Curve` runs the existing v2 Curve implementation.
- `Dex` runs the existing v1 DEX implementation.
- `LpManager` runs the existing v1 LPManager implementation.
- Reward, Creator, Distributor, v2 DEX, v2 Fee, v2 LPManager, and every v2
  Vault/VaultRegistry/DividendVault stream are removed from the GIWA runtime.
- Inactive dedicated indexing stacks are removed from the branch instead of
  being retained behind flags. Active internal modules may keep `v1` or `v2`
  names to identify their ABI provenance.
- GIWA token rows explicitly store `version = 'V2'` and `chain = 'GIWA'`.
- GIWA market writes use only `CURVE` and `DEX`.
- GIWA Curve fee-history writes use `curve_buy` and `curve_sell`, not the
  version-prefixed values.
- `token_id` remains the sole primary key.
- Existing database rows and constraints are not normalized. In particular,
  existing `V2_CURVE`, `V2_DEX`, and `v2_*` fee-history values remain unchanged.
- Historical SQL migrations, tables, and seeds are preserved. The only new
  schema migration is the already approved `token.chain` migration.

## Runtime Architecture

### Active handlers

The runtime starts six event handlers:

| Runtime event | Implementation source | Public checkpoint name |
| --- | --- | --- |
| Curve | `event::v2::curve` | `curve` |
| Dex | `event::v1::dex` | `dex` |
| LpManager | `event::v1::lp_manager` | `lp_manager` |
| Token | `event::common::token` | `token` |
| Price | `event::common::price` | `price` |
| PriceUsd | `event::common::price_usd` | `price_usd` |

The Curve handler may retain internal types such as `V2CurveEvent`, but it is
started with `EventType::Curve`. No `V2Curve`, `V2Dex`, `V2Fee`, or
`V2LpManager` event type or checkpoint remains.

### Dependencies

- Curve waits for Price using the existing price-before-trade ordering.
- Dex waits for Curve.
- LpManager waits for Curve.
- Token waits for Curve and no longer waits for the removed v2 DEX checkpoint.
- Price and PriceUsd keep their existing common-stream behavior.

Startup price-cache warm-up uses the generic Dex checkpoint. No code waits for
or queries a removed checkpoint.

## Runtime and Source Removal

Remove runtime startup, event-type variants, synchronization branches,
configuration, dedicated event/type/controller code, ABIs, tests, and current
feature documentation for the inactive stacks:

- the old v1 Curve implementation
- the v2 DEX implementation
- the v2 Fee contract stream
- the v2 LPManager implementation
- v1 Reward, Creator, and Distributor implementations
- v2 Vault, VaultRegistry, and DividendVault implementations

Keep common code that is still consumed by an active implementation. Examples
include the common token/market/swap/fee-history controllers, the v2 Curve
sniping-penalty controller, and database structures required by the active v2
Curve implementation.

The existing v2 Factory source is outside the active runtime. It is removed
only if repository reference analysis proves that no active Curve path imports
it; no active dependency may be deleted merely because its name contains `v2`.

## Configuration

Active contract configuration uses unversioned environment variables:

- `BONDING_CURVE`
- `DEX_FACTORY`
- `DEX_ROUTER`
- `LP_MANAGER`

Active fee configuration also drops its version prefix:

- `CREATE_FEE_AMOUNT`
- `GRADUATE_FEE_AMOUNT`
- `BONDING_CURVE_FEE_RATE`
- `DEX_ROUTER_FEE_RATE`

`WMON` and unrelated common configuration remain unchanged. Removed streams do
not leave required or optional environment variables behind. Address parsing
and eager startup validation remain fail-fast.

The common Token stream builds its system-address filter only from active GIWA
addresses. It does not include addresses for removed reward, creator, v2 DEX,
v2 Fee, v2 LPManager, or vault contracts.

## Database Writes

### Token creation

The token batch CTE explicitly inserts:

```text
version = 'V2'
chain   = 'GIWA'
```

The shared database default remains `chain = 'MON'` for legacy writers and
existing rows. The observer does not rely on that default for GIWA writes.

### Market lifecycle

The active v2 Curve implementation writes:

- token creation and curve sync rows with `market_type = 'CURVE'`
- buy/sell rows on the curve with `market_type = 'CURVE'`
- graduation updates with `market_type = 'DEX'`

The active v1 DEX implementation continues to write `DEX`. The database CHECK
constraint is not tightened, and no existing `V2_CURVE` or `V2_DEX` row is
updated.

### Fee history

Curve creation keeps `fee_type = 'create'`. Curve trades use the existing
generic fee categories:

- buy: `curve_buy`
- sell: `curve_sell`

The active v1 DEX continues to use its existing generic swap/router fee
categories. The removed v2 Fee contract stream is unrelated to this common
trade `fee_history` and its removal does not remove the common controller.

## Vault and Historical Data Policy

The observer removes v2 Vault, VaultRegistry, and DividendVault code, ABIs,
backfill utility, tests, and current feature documentation. The database is
handled differently:

- Do not drop or truncate vault/dividend tables.
- Do not delete vault/dividend rows.
- Do not remove or rewrite `vault.sql`, `dividend.sql`, upgrade scripts, or
  vault metadata seeds.
- Do not add a cleanup migration for removed runtime features.

The same preservation rule applies to existing Reward, Creator, Distributor,
v2 DEX, v2 Fee, and v2 LPManager data and historical migrations.

## Data Flow

1. The shared database migration labels existing/legacy token rows as MON by
   default without changing their version or market values.
2. GIWA Curve reads the v2 bonding-curve contract under the generic `curve`
   checkpoint.
3. A create event inserts a token with `version = 'V2'`, `chain = 'GIWA'`, and
   a `CURVE` market.
4. Curve buy/sell events write generic market and fee-history categories.
5. Graduation changes the token's market to `DEX`.
6. GIWA Dex and LPManager continue through the v1-derived implementations under
   generic runtime names.
7. Common Token, Price, and PriceUsd streams advance independently of removed
   versioned checkpoints.

## Failure Handling

- Generic address variables remain required and are validated at startup.
- The token migration is transactional and idempotent.
- Explicit GIWA token values prevent accidental MON attribution.
- Removing an event type includes every match arm and dependency so the runtime
  cannot wait indefinitely for a checkpoint that never starts.
- Existing database data is never used as a cleanup target during this change.

## Verification

- Assert `EventType::all()` exposes exactly the six active generic events.
- Assert `main` maps the v2 Curve handler to `Curve`, v1 DEX to `Dex`, and v1
  LPManager to `LpManager`.
- Assert no runtime dependency references a removed event type.
- Verify generic environment-variable names initialize the active addresses and
  fee constants.
- Verify the production token CTE writes `version = 'V2'` and `chain = 'GIWA'`.
- Verify new Curve/graduate writes use `CURVE`/`DEX` and new Curve fee-history
  writes use `curve_buy`/`curve_sell`.
- Verify no migration updates existing market or fee-history values and no
  historical migration/seed is deleted.
- Verify inactive dedicated modules, ABIs, tests, and current docs are absent
  while active Curve/Dex/LPManager and common Token/Price functionality compile.
- Run focused integration tests, library tests, and runtime compilation. Record
  the pre-existing benchmark dependency and unrelated pool-reserve test failures
  separately rather than changing them in this feature.

## Out of Scope

- Updating existing MON rows from `V2_CURVE`/`V2_DEX` to generic values
- Dropping the `token.version` column or changing its allowed values
- Changing `token_id` or any foreign key to a composite chain key
- Adding `chain` to tables other than `token`
- Dropping historical tables or rewriting applied migrations
- Renaming active internal Rust event types or ABI directories solely to hide
  their contract provenance
- Fixing pre-existing benchmark dependencies, repository-wide formatting, or
  unrelated pool-reserve test failures
