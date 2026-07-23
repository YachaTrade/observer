//! Shared test fixtures for integration tests that need a real PostgreSQL
//! instance. Spawns an ephemeral container via testcontainers and applies
//! the baseline migrations.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

/// Owned test database handle. The `_container` field must be held for the
/// lifetime of the test — dropping it stops the Postgres container.
pub struct TestDb {
    pub pool: PgPool,
    pub _container: ContainerAsync<Postgres>,
}

/// Spin up a fresh Postgres container, connect to it, enable required
/// extensions, and apply the single canonical schema migration. Skips
/// destructive reset migrations.
pub async fn setup_test_db() -> Result<TestDb> {
    // Ensure observer's lazy-static address configs can initialize even in
    // tests that import them (e.g. `WNATIVE_ADDRESS`). Any valid hex address
    // works — these are not persisted anywhere and only need to parse
    // through `alloy::primitives::Address`. Keep idempotent so parallel
    // tests that all call this are safe.
    // SAFETY: Rust 2024 marks `set_var` unsafe because concurrent reads of
    // environment state from other threads are UB on some platforms. In the
    // test harness this runs during `setup_test_db` before any observer
    // code touches the env; both vars are idempotent (checked before set)
    // and are only consulted by this process. No other thread reads env
    // while we're setting it.
    unsafe {
        if std::env::var("WETH").is_err() {
            std::env::set_var("WETH", "0x4200000000000000000000000000000000000006");
        }
        if std::env::var("QUOTE_CONFIGS").is_err() {
            std::env::set_var(
                "QUOTE_CONFIGS",
                "0x760AfE15c6AB78f59cd24C2f5b9aeB8C82d95c5b:0xfeed:18",
            );
        }
        if std::env::var("DEFAULT_DELAY").is_err() {
            std::env::set_var("DEFAULT_DELAY", "10");
        }
    }

    // PG 17 matches production (`postgresql@17` via Homebrew) and supports
    // GENERATED ALWAYS AS ... STORED columns (introduced in PG 12). The crate
    // default tag is `11-alpine`, which does NOT support generated columns.
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .context("failed to start postgres container")?;

    let host = container
        .get_host()
        .await
        .context("failed to get container host")?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .context("failed to get container port")?;
    let url = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&url)
        .await
        .context("failed to connect to test postgres")?;

    // Extensions required by the migrations (pg_trgm for gin_trgm_ops,
    // btree_gist for exclusion constraints).
    sqlx::raw_sql("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        .execute(&pool)
        .await
        .context("failed to create pg_trgm extension")?;
    sqlx::raw_sql("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        .execute(&pool)
        .await
        .context("failed to create btree_gist extension")?;

    apply_baseline_migrations(&pool)
        .await
        .context("failed to apply baseline migrations")?;

    Ok(TestDb {
        pool,
        _container: container,
    })
}

/// Path to the GIWA deployment schema. `migrations/` is a submodule of the
/// YachaTrade/migrations repo, whose `0001_init.sql` is the single source of
/// truth for what a fresh GIWA database looks like. Tests apply exactly what
/// production applies, so a schema/code mismatch fails here instead of in
/// deployment.
const GIWA_SCHEMA: &str = "migrations/0001_init.sql";

/// Section markers inside `0001_init.sql`. The consolidated file records the
/// upstream file each block came from as `-- >>> <name>`, which lets a test
/// re-run one block in isolation — see [`read_schema_section`].
const SECTION_MARKER: &str = "-- >>> ";

/// Apply the GIWA deployment schema to a fresh database.
async fn apply_baseline_migrations(pool: &PgPool) -> Result<()> {
    let sql = std::fs::read_to_string(GIWA_SCHEMA).with_context(|| {
        format!("failed to read {GIWA_SCHEMA} — run `git submodule update --init`")
    })?;
    sqlx::raw_sql(&sql)
        .execute(pool)
        .await
        .with_context(|| format!("failed to apply {GIWA_SCHEMA}"))?;

    Ok(())
}

/// Return one `-- >>> <name>` block of the consolidated schema, for tests that
/// re-run a single idempotent block (e.g. a backfill) and assert it reproduces
/// what the triggers accumulated.
pub fn read_schema_section(name: &str) -> Result<String> {
    let sql = std::fs::read_to_string(GIWA_SCHEMA)
        .with_context(|| format!("failed to read {GIWA_SCHEMA}"))?;
    let header = format!("{SECTION_MARKER}{name}");
    let start = sql
        .find(&header)
        .with_context(|| format!("no `{header}` section in {GIWA_SCHEMA}"))?;
    let body = &sql[start + header.len()..];
    let end = body.find(SECTION_MARKER).unwrap_or(body.len());
    Ok(body[..end].to_string())
}

/// Insert a minimal `token` row with the given creator. Fills required
/// columns (name, symbol, image_uri, transaction_hash, total_supply,
/// created_at) with dummy values.
pub async fn insert_token(pool: &PgPool, token_id: &str, creator: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO token (
            token_id, name, symbol, image_uri, creator,
            transaction_hash, total_supply, created_at
        )
        VALUES ($1, 'Test Token', 'TT', 'uri', $2, 'tx_hash', 1000000::numeric, 0)
        "#,
    )
    .bind(token_id)
    .bind(creator)
    .execute(pool)
    .await
    .context("failed to insert test token row")?;
    Ok(())
}

// ============================================================================
// Balance / balance_history helpers (Group A: balance.rs tests)
// ============================================================================

/// Call `BATCH_SET_BALANCES_SQL` with a single balance tuple for test
/// ergonomics. Internally builds the `Vec<...>` arrays UNNEST expects.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_set_balances(
    pool: &PgPool,
    token_id: &str,
    account_id: &str,
    balance: &str,
    block_number: i64,
    transaction_hash: &str,
    log_index: i32,
    tx_index: i32,
    created_at: i64,
) -> Result<()> {
    use std::str::FromStr;
    let balance_num = bigdecimal::BigDecimal::from_str(balance)
        .context("failed to parse balance as BigDecimal")?;
    let token_ids = vec![token_id];
    let account_ids = vec![account_id];
    let balance_vals = vec![balance_num];
    let block_numbers = vec![block_number];
    let transaction_hashes = vec![transaction_hash];
    let log_indices = vec![log_index];
    let tx_indices = vec![tx_index];
    let created_ats = vec![created_at];

    sqlx::query(observer::db::postgres::controller::balance::BATCH_SET_BALANCES_SQL)
        .bind(&token_ids)
        .bind(&account_ids)
        .bind(&balance_vals)
        .bind(&block_numbers)
        .bind(&transaction_hashes)
        .bind(&log_indices)
        .bind(&tx_indices)
        .bind(&created_ats)
        .execute(pool)
        .await
        .context("failed to execute BATCH_SET_BALANCES_SQL")?;
    Ok(())
}

/// Read the `balance` table for a (token_id, account_id) pair. Returns None
/// if no row exists (e.g. deleted by `trigger_delete_zero_balance`).
pub async fn get_balance(
    pool: &PgPool,
    token_id: &str,
    account_id: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT balance::text FROM balance WHERE token_id = $1 AND account_id = $2")
            .bind(token_id)
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .context("failed to read balance row")?;
    Ok(row.map(|(b,)| b))
}

/// Count `balance_history` rows for a (token_id, account_id) pair.
pub async fn count_balance_history(pool: &PgPool, token_id: &str, account_id: &str) -> Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM balance_history WHERE token_id = $1 AND account_id = $2",
    )
    .bind(token_id)
    .bind(account_id)
    .fetch_one(pool)
    .await
    .context("failed to count balance_history rows")?;
    Ok(row.0)
}

/// Get token_holder_count from the `token` table.
pub async fn get_token_holder_count(pool: &PgPool, token_id: &str) -> Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT token_holder_count FROM token WHERE token_id = $1")
        .bind(token_id)
        .fetch_one(pool)
        .await
        .context("failed to read token_holder_count")?;
    Ok(row.0)
}

// ============================================================================
// Position / position_history helpers (Group A: position.rs tests)
// ============================================================================

/// Call `BATCH_INSERT_POSITION_HISTORY_SQL` with a single event. Returns
/// count of actually-inserted rows (0 if duplicate, 1 if new).
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_position_history(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
    quote_in: &str,
    quote_out: &str,
    usd_in: &str,
    usd_out: &str,
    token_in: &str,
    token_out: &str,
    transaction_hash: &str,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
    created_at: i64,
    transfer_type: &str,
    sender_address: Option<&str>,
) -> Result<usize> {
    use std::str::FromStr;
    let parse = |s: &str| {
        bigdecimal::BigDecimal::from_str(s).expect("failed to parse numeric string in test helper")
    };
    let account_ids = vec![account_id];
    let token_ids = vec![token_id];
    let quote_ins = vec![parse(quote_in)];
    let quote_outs = vec![parse(quote_out)];
    let usd_ins = vec![parse(usd_in)];
    let usd_outs = vec![parse(usd_out)];
    let token_ins = vec![parse(token_in)];
    let token_outs = vec![parse(token_out)];
    let transaction_hashes = vec![transaction_hash];
    let block_numbers = vec![block_number];
    let tx_indices = vec![tx_index];
    let log_indices = vec![log_index];
    let created_ats = vec![created_at];
    let transfer_types = vec![transfer_type];
    let counterparties: Vec<Option<&str>> = vec![sender_address];

    // position_history SQL uses RETURNING, so fetch_all instead of execute
    let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(
        observer::db::postgres::controller::position::BATCH_INSERT_POSITION_HISTORY_SQL,
    )
    .bind(&account_ids)
    .bind(&token_ids)
    .bind(&quote_ins)
    .bind(&quote_outs)
    .bind(&usd_ins)
    .bind(&usd_outs)
    .bind(&token_ins)
    .bind(&token_outs)
    .bind(&transaction_hashes)
    .bind(&block_numbers)
    .bind(&tx_indices)
    .bind(&log_indices)
    .bind(&created_ats)
    .bind(&transfer_types)
    .bind(&counterparties)
    .fetch_all(pool)
    .await
    .context("failed to execute BATCH_INSERT_POSITION_HISTORY_SQL")?;
    Ok(rows.len())
}

/// Count `position_history` rows for a composite PK.
pub async fn count_position_history(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
    transaction_hash: &str,
    tx_index: i32,
    log_index: i32,
) -> Result<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM position_history
        WHERE account_id = $1 AND token_id = $2 AND transaction_hash = $3
          AND tx_index = $4 AND log_index = $5
        "#,
    )
    .bind(account_id)
    .bind(token_id)
    .bind(transaction_hash)
    .bind(tx_index)
    .bind(log_index)
    .fetch_one(pool)
    .await
    .context("failed to count position_history rows")?;
    Ok(row.0)
}

/// Return (token_in, token_out) from the aggregated `position` table as
/// strings (for exact comparison), None if no row exists.
pub async fn get_position_token_flow(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT token_in::text, token_out::text
        FROM position
        WHERE account_id = $1 AND token_id = $2
        "#,
    )
    .bind(account_id)
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .context("failed to read position row")?;
    Ok(row)
}

// ============================================================================
// Swap helpers (Group A: swap.rs tests)
// ============================================================================

/// Insert a `market` row so the trigger on `swap` INSERT (which updates
/// market.volume) has a row to update. `market.token_id` is the primary
/// key so one row per token.
pub async fn insert_market(pool: &PgPool, token_id: &str, market_type: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO market (
            market_type, token_id, reserve_quote, reserve_token,
            volume, price, latest_trade_at, created_at
        )
        VALUES ($1, $2, 0::numeric, 0::numeric, 0::numeric, 1::numeric, 0, 0)
        ON CONFLICT (token_id) DO NOTHING
        "#,
    )
    .bind(market_type)
    .bind(token_id)
    .execute(pool)
    .await
    .context("failed to insert market row")?;
    Ok(())
}

/// Call `BATCH_INSERT_SWAPS_SQL` with a single swap tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_swaps(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
    is_buy: bool,
    quote_amount: &str,
    token_amount: &str,
    reserve_quote: &str,
    reserve_token: &str,
    value: &str,
    market_type: &str,
    created_at: i64,
    transaction_hash: &str,
    block_number: i64,
    log_index: i32,
    tx_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| {
        bigdecimal::BigDecimal::from_str(s).expect("failed to parse numeric string in test helper")
    };
    let account_ids = vec![account_id];
    let token_ids = vec![token_id];
    let is_buys = vec![is_buy];
    let quote_amounts = vec![parse(quote_amount)];
    let token_amounts = vec![parse(token_amount)];
    let reserve_quotes = vec![parse(reserve_quote)];
    let reserve_tokens = vec![parse(reserve_token)];
    let values = vec![parse(value)];
    let market_types = vec![market_type];
    let created_ats = vec![created_at];
    let transaction_hashes = vec![transaction_hash];
    let block_numbers = vec![block_number];
    let log_indexes = vec![log_index];
    let tx_indexes = vec![tx_index];

    sqlx::query(observer::db::postgres::controller::swap::BATCH_INSERT_SWAPS_SQL)
        .bind(&account_ids)
        .bind(&token_ids)
        .bind(&is_buys)
        .bind(&quote_amounts)
        .bind(&token_amounts)
        .bind(&reserve_quotes)
        .bind(&reserve_tokens)
        .bind(&values)
        .bind(&market_types)
        .bind(&created_ats)
        .bind(&transaction_hashes)
        .bind(&block_numbers)
        .bind(&log_indexes)
        .bind(&tx_indexes)
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_SWAPS_SQL")?;
    Ok(())
}

/// Get total swap_count (buy + sell) for a token. Returns None if no row.
pub async fn get_swap_count(pool: &PgPool, token_id: &str) -> Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT buy_count + sell_count FROM swap_count WHERE token_id = $1")
            .bind(token_id)
            .fetch_optional(pool)
            .await
            .context("failed to read swap_count row")?;
    Ok(row.map(|(c,)| c))
}

/// Get market.volume for a token as a string (for exact comparison).
pub async fn get_market_volume(pool: &PgPool, token_id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT volume::text FROM market WHERE token_id = $1")
            .bind(token_id)
            .fetch_optional(pool)
            .await
            .context("failed to read market.volume")?;
    Ok(row.map(|(v,)| v))
}

/// Seed the `price` table with a (block_number, price) row for the
/// WNATIVE quote. Used by the price-range / fallback SQL tests.
pub async fn insert_price(pool: &PgPool, block_number: i64, price: &str) -> Result<()> {
    use std::str::FromStr;
    let p =
        bigdecimal::BigDecimal::from_str(price).context("failed to parse price as BigDecimal")?;
    let quote_id: &str = &observer::config::WNATIVE_ADDRESS;
    sqlx::query(
        r#"
        INSERT INTO price (quote_id, block_number, price, created_at)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (quote_id, block_number) DO UPDATE SET price = EXCLUDED.price
        "#,
    )
    .bind(quote_id)
    .bind(block_number)
    .bind(&p)
    .execute(pool)
    .await
    .context("failed to insert price row")?;
    Ok(())
}

/// Execute `GET_PRICES_FOR_RANGE_SQL` directly and return the rows as
/// `(block_number, price::text)` for exact comparison.
pub async fn call_get_prices_for_range(
    pool: &PgPool,
    min_block: i64,
    max_block: i64,
) -> Result<Vec<(i64, String)>> {
    let quote_id: &str = &observer::config::WNATIVE_ADDRESS;
    // Wrap the base SQL so we can cast the numeric column to text in a
    // stable format (sqlx has no direct BigDecimal -> String for this
    // tuple, and we want to compare exact string representations).
    let rows: Vec<(i64, String)> = sqlx::query_as(
        r#"
        SELECT block_number, price::text
        FROM price
        WHERE quote_id = $1 AND block_number BETWEEN $2 AND $3
        ORDER BY block_number ASC
        "#,
    )
    .bind(quote_id)
    .bind(min_block)
    .bind(max_block)
    .fetch_all(pool)
    .await
    .context("failed to execute in-range price query")?;
    Ok(rows)
}

// ============================================================================
// Group B helpers: token, market, mint, burn, pool, lp
// ============================================================================

/// Insert a `token_metadata` row for metadata fetch/delete tests.
pub async fn insert_token_metadata(
    pool: &PgPool,
    metadata_url: &str,
    name: &str,
    symbol: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO token_metadata (metadata_url, name, symbol, image_url, is_nsfw)
        VALUES ($1, $2, $3, 'http://img', false)
        "#,
    )
    .bind(metadata_url)
    .bind(name)
    .bind(symbol)
    .execute(pool)
    .await
    .context("failed to insert token_metadata row")?;
    Ok(())
}

/// Count `token_metadata` rows for a given metadata_url.
pub async fn count_token_metadata(pool: &PgPool, metadata_url: &str) -> Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM token_metadata WHERE metadata_url = $1")
        .bind(metadata_url)
        .fetch_one(pool)
        .await
        .context("failed to count token_metadata")?;
    Ok(row.0)
}

/// Call the production `BATCH_INSERT_TOKENS_AND_MARKETS_SQL` with a single
/// token for test ergonomics. The WNATIVE_ADDRESS is bound as $25.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_tokens_and_markets(
    pool: &PgPool,
    token_id: &str,
    name: &str,
    symbol: &str,
    creator: &str,
    market_type: &str,
    virtual_native: &str,
    virtual_token: &str,
    block_number: i64,
    block_timestamp: i64,
    transaction_hash: &str,
    log_index: i32,
    tx_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let vn = bigdecimal::BigDecimal::from_str(virtual_native)
        .context("failed to parse virtual_native")?;
    let vt =
        bigdecimal::BigDecimal::from_str(virtual_token).context("failed to parse virtual_token")?;
    let price = if vt > bigdecimal::BigDecimal::from(0) {
        use bigdecimal::RoundingMode;
        (&vn / &vt).with_scale_round(10, RoundingMode::Up)
    } else {
        bigdecimal::BigDecimal::from(0)
    };
    let total_supply = bigdecimal::BigDecimal::from(1_000_000_000_000_000_000_000_000_000u128);
    let quote_id: &str = &observer::config::WNATIVE_ADDRESS;

    let token_ids = vec![token_id];
    let names = vec![name];
    let symbols = vec![symbol];
    let creators = vec![creator];
    let descriptions: Vec<Option<&str>> = vec![None];
    let twitters: Vec<Option<&str>> = vec![None];
    let telegrams: Vec<Option<&str>> = vec![None];
    let websites: Vec<Option<&str>> = vec![None];
    let image_uris = vec!["uri"];
    let is_nsfws = vec![false];
    let is_graduateds = vec![false];
    let total_supplies = vec![total_supply];
    let created_ats = vec![block_timestamp];
    let prices = vec![price];
    let market_types = vec![market_type];
    let latest_trade_ats = vec![block_timestamp];
    let block_numbers = vec![block_number];
    let transaction_hashes = vec![transaction_hash];
    let log_indices = vec![log_index];
    let tx_indices = vec![tx_index];
    let reserve_quotes = vec![virtual_native];
    let reserve_tokens = vec![virtual_token];
    let quote_ids = vec![quote_id];

    sqlx::query(observer::db::postgres::controller::token::BATCH_INSERT_TOKENS_AND_MARKETS_SQL)
        .bind(&token_ids)
        .bind(&names)
        .bind(&symbols)
        .bind(&creators)
        .bind(&descriptions)
        .bind(&twitters)
        .bind(&telegrams)
        .bind(&websites)
        .bind(&image_uris)
        .bind(&is_nsfws)
        .bind(&is_graduateds)
        .bind(&total_supplies)
        .bind(&created_ats)
        .bind(&prices)
        .bind(&market_types)
        .bind(&latest_trade_ats)
        .bind(&block_numbers)
        .bind(&transaction_hashes)
        .bind(&log_indices)
        .bind(&tx_indices)
        .bind(&reserve_quotes)
        .bind(&reserve_tokens)
        .bind(&quote_ids)
        .bind(quote_id) // $24
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_TOKENS_AND_MARKETS_SQL")?;
    Ok(())
}

/// Call `HANDLE_CURVE_SYNC_SQL` with scalar params.
#[allow(clippy::too_many_arguments)]
pub async fn call_handle_curve_sync(
    pool: &PgPool,
    token_id: &str,
    price: &str,
    reserve_token: &str,
    reserve_quote: &str,
    ath_price_usd: &str,
    ath_price_quote: &str,
    block_timestamp: i64,
    market_type: &str,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::market::HANDLE_CURVE_SYNC_SQL)
        .bind(token_id)
        .bind(parse(price))
        .bind(parse(reserve_token))
        .bind(parse(reserve_quote))
        .bind(parse(ath_price_usd))
        .bind(parse(ath_price_quote))
        .bind(block_timestamp)
        .bind(market_type)
        .execute(pool)
        .await
        .context("failed to execute HANDLE_CURVE_SYNC_SQL")?;
    Ok(())
}

/// Call `HANDLE_DEX_SYNC_SQL` with scalar params.
#[allow(clippy::too_many_arguments)]
pub async fn call_handle_dex_sync(
    pool: &PgPool,
    token_id: &str,
    price: &str,
    reserve_quote: &str,
    reserve_token: &str,
    ath_price_usd: &str,
    ath_price_quote: &str,
    block_timestamp: i64,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::market::HANDLE_DEX_SYNC_SQL)
        .bind(token_id)
        .bind(parse(price))
        .bind(parse(reserve_quote))
        .bind(parse(reserve_token))
        .bind(parse(ath_price_usd))
        .bind(parse(ath_price_quote))
        .bind(block_timestamp)
        .execute(pool)
        .await
        .context("failed to execute HANDLE_DEX_SYNC_SQL")?;
    Ok(())
}

/// Call `BATCH_HANDLE_GRADUATES_SQL` with arrays.
pub async fn call_batch_handle_graduates(
    pool: &PgPool,
    graduates: &[(&str, &str)],
    graduated_market_type: &str,
) -> Result<i64> {
    let token_ids: Vec<&str> = graduates.iter().map(|(t, _)| *t).collect();
    let pool_ids: Vec<&str> = graduates.iter().map(|(_, p)| *p).collect();
    let count: i64 =
        sqlx::query_scalar(observer::db::postgres::controller::market::BATCH_HANDLE_GRADUATES_SQL)
            .bind(&token_ids)
            .bind(&pool_ids)
            .bind(graduated_market_type)
            .fetch_one(pool)
            .await
            .context("failed to execute BATCH_HANDLE_GRADUATES_SQL")?;
    Ok(count)
}

/// Call `BATCH_INSERT_MINTS_SQL` with a single mint tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_mints(
    pool: &PgPool,
    token_id: &str,
    account_id: &str,
    market_id: &str,
    quote_amount: &str,
    token_amount: &str,
    reserve_quote: &str,
    reserve_token: &str,
    created_at: i64,
    transaction_hash: &str,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::mint::BATCH_INSERT_MINTS_SQL)
        .bind(&vec![token_id])
        .bind(&vec![account_id])
        .bind(&vec![market_id])
        .bind(&vec![parse(quote_amount)])
        .bind(&vec![parse(token_amount)])
        .bind(&vec![parse(reserve_quote)])
        .bind(&vec![parse(reserve_token)])
        .bind(&vec![created_at])
        .bind(&vec![transaction_hash])
        .bind(&vec![block_number])
        .bind(&vec![tx_index])
        .bind(&vec![log_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_MINTS_SQL")?;
    Ok(())
}

/// Call `BATCH_INSERT_BURNS_SQL` (from mint.rs) with a single burn tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_burns_mint(
    pool: &PgPool,
    token_id: &str,
    account_id: &str,
    market_id: &str,
    quote_amount: &str,
    token_amount: &str,
    reserve_quote: &str,
    reserve_token: &str,
    created_at: i64,
    transaction_hash: &str,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::mint::BATCH_INSERT_BURNS_SQL)
        .bind(&vec![token_id])
        .bind(&vec![account_id])
        .bind(&vec![market_id])
        .bind(&vec![parse(quote_amount)])
        .bind(&vec![parse(token_amount)])
        .bind(&vec![parse(reserve_quote)])
        .bind(&vec![parse(reserve_token)])
        .bind(&vec![created_at])
        .bind(&vec![transaction_hash])
        .bind(&vec![block_number])
        .bind(&vec![tx_index])
        .bind(&vec![log_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_BURNS_SQL (mint)")?;
    Ok(())
}

/// Call `HANDLE_BURN_SQL` (from burn.rs) with scalar params.
/// Bindings: $1 account_id (from), $2 token_id, $3 amount,
///           $4 transaction_hash, $5 log_index, $6 block_timestamp.
#[allow(clippy::too_many_arguments)]
pub async fn call_handle_burn(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
    amount: &str,
    transaction_hash: &str,
    log_index: i32,
    block_timestamp: i64,
) -> Result<()> {
    use std::str::FromStr;
    let amount_num = bigdecimal::BigDecimal::from_str(amount).context("failed to parse amount")?;
    sqlx::query(observer::db::postgres::controller::burn::HANDLE_BURN_SQL)
        .bind(account_id)
        .bind(token_id)
        .bind(amount_num)
        .bind(transaction_hash)
        .bind(log_index)
        .bind(block_timestamp)
        .execute(pool)
        .await
        .context("failed to execute HANDLE_BURN_SQL")?;
    Ok(())
}

/// Call `BATCH_HANDLE_BURNS_SQL` (from burn.rs) with a single burn tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_handle_burns(
    pool: &PgPool,
    token_id: &str,
    account_id: &str,
    amount: &str,
    transaction_hash: &str,
    log_index: i32,
    created_at: i64,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::burn::BATCH_HANDLE_BURNS_SQL)
        .bind(&vec![token_id])
        .bind(&vec![account_id])
        .bind(&vec![parse(amount)])
        .bind(&vec![transaction_hash])
        .bind(&vec![log_index])
        .bind(&vec![created_at])
        .execute(pool)
        .await
        .context("failed to execute BATCH_HANDLE_BURNS_SQL")?;
    Ok(())
}

/// Call `BATCH_INSERT_POOLS_SQL` with a single pool tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_pools(
    pool: &PgPool,
    pool_id: &str,
    token0: &str,
    token1: &str,
    reserve0: &str,
    reserve1: &str,
    price: &str,
    created_at: i64,
    block_number: i64,
    tx_hash: &str,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::pool::BATCH_INSERT_POOLS_SQL)
        .bind(&vec![pool_id])
        .bind(&vec![token0])
        .bind(&vec![token1])
        .bind(&vec![parse(reserve0)])
        .bind(&vec![parse(reserve1)])
        .bind(&vec![parse(price)])
        .bind(&vec![created_at])
        .bind(&vec![block_number])
        .bind(&vec![tx_hash])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_POOLS_SQL")?;
    Ok(())
}

/// Call `BATCH_UPDATE_POOL_RESERVES_SQL` with a single pool update tuple.
/// Convenience wrapper for tests that don't care about the freshness tuple —
/// the on-chain (block_number, tx_index, log_index) is filled from
/// (block_timestamp, 0, 0). Tests that exercise same-batch ordering should
/// call `call_batch_update_pool_reserves_with_freshness` instead.
pub async fn call_batch_update_pool_reserves(
    pool: &PgPool,
    pool_id: &str,
    reserve0: &str,
    reserve1: &str,
    price: &str,
    block_timestamp: i64,
) -> Result<()> {
    call_batch_update_pool_reserves_with_freshness(
        pool,
        pool_id,
        reserve0,
        reserve1,
        price,
        block_timestamp,
        block_timestamp,
        0,
        0,
    )
    .await
}

/// Same as `call_batch_update_pool_reserves` but lets the caller pin the
/// on-chain freshness tuple (block_number, tx_index, log_index). Use this
/// when the test needs to control how `DISTINCT ON ... ORDER BY ... DESC`
/// resolves ties between multiple syncs for the same pool in one batch.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_update_pool_reserves_with_freshness(
    pool: &PgPool,
    pool_id: &str,
    reserve0: &str,
    reserve1: &str,
    price: &str,
    block_timestamp: i64,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::pool::BATCH_UPDATE_POOL_RESERVES_SQL)
        .bind(&vec![pool_id])
        .bind(&vec![parse(reserve0)])
        .bind(&vec![parse(reserve1)])
        .bind(&vec![parse(price)])
        // value: NULL means "don't touch pool.value" (test helper has no
        // chain-implied TVL to record; behave like graduated-Sync arm)
        .bind(&vec![None::<bigdecimal::BigDecimal>])
        // Per-token USD prices follow the same nullable overwrite policy.
        .bind(&vec![None::<bigdecimal::BigDecimal>])
        .bind(&vec![None::<bigdecimal::BigDecimal>])
        .bind(&vec![block_timestamp])
        .bind(&vec![block_number])
        .bind(&vec![tx_index])
        .bind(&vec![log_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_UPDATE_POOL_RESERVES_SQL")?;
    Ok(())
}

/// Call `HANDLE_LP_ALLOCATE_SQL` with scalar params.
pub async fn call_handle_lp_allocate(
    pool: &PgPool,
    token_id: &str,
    quote_amount: &str,
    token_amount: &str,
    transaction_hash: &str,
    created_at: i64,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::lp::HANDLE_LP_ALLOCATE_SQL)
        .bind(token_id)
        .bind(parse(quote_amount))
        .bind(parse(token_amount))
        .bind(transaction_hash)
        .bind(created_at)
        .execute(pool)
        .await
        .context("failed to execute HANDLE_LP_ALLOCATE_SQL")?;
    Ok(())
}

/// Call `HANDLE_LP_COLLECT_SQL` with scalar params.
pub async fn call_handle_lp_collect(
    pool: &PgPool,
    token_id: &str,
    quote_amount: &str,
    token_amount: &str,
    transaction_hash: &str,
    tx_index: i32,
    log_index: i32,
    created_at: i64,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::lp::HANDLE_LP_COLLECT_SQL)
        .bind(token_id)
        .bind(parse(quote_amount))
        .bind(parse(token_amount))
        .bind(transaction_hash)
        .bind(tx_index)
        .bind(log_index)
        .bind(created_at)
        .execute(pool)
        .await
        .context("failed to execute HANDLE_LP_COLLECT_SQL")?;
    Ok(())
}

/// Read market row for a token. Returns (market_type, price, ath_price,
/// ath_price_quote, reserve_quote, reserve_token, pool_id) as strings.
pub async fn get_market_row(
    pool: &PgPool,
    token_id: &str,
) -> Result<
    Option<(
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    )>,
> {
    let row: Option<(
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    )> = sqlx::query_as(
        r#"
            SELECT market_type, price::text, ath_price::text, ath_price_quote::text,
                   reserve_quote::text, reserve_token::text, pool_id
            FROM market WHERE token_id = $1
            "#,
    )
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .context("failed to read market row")?;
    Ok(row)
}

/// Get token.total_supply as text.
pub async fn get_total_supply(pool: &PgPool, token_id: &str) -> Result<String> {
    let row: (String,) = sqlx::query_as("SELECT total_supply::text FROM token WHERE token_id = $1")
        .bind(token_id)
        .fetch_one(pool)
        .await
        .context("failed to read total_supply")?;
    Ok(row.0)
}

/// Get token.is_graduated.
pub async fn get_is_graduated(pool: &PgPool, token_id: &str) -> Result<bool> {
    let row: (bool,) = sqlx::query_as("SELECT is_graduated FROM token WHERE token_id = $1")
        .bind(token_id)
        .fetch_one(pool)
        .await
        .context("failed to read is_graduated")?;
    Ok(row.0)
}

/// Count rows in a table matching a token_id.
pub async fn count_rows_for_token(pool: &PgPool, table: &str, token_id: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {} WHERE token_id = $1", table);
    let row: (i64,) = sqlx::query_as(&sql)
        .bind(token_id)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {} rows", table))?;
    Ok(row.0)
}

/// Get token_count totals.
pub async fn get_token_count(pool: &PgPool) -> Result<(i64, i64)> {
    let row: (i64, i64) =
        sqlx::query_as("SELECT total_count, graduated_count FROM token_count LIMIT 1")
            .fetch_one(pool)
            .await
            .context("failed to read token_count")?;
    Ok(row)
}

/// Read pool row. Returns (reserve0, reserve1, price, latest_trade_at).
pub async fn get_pool_row(
    pool: &PgPool,
    pool_id: &str,
) -> Result<Option<(String, String, String, i64)>> {
    let row: Option<(String, String, String, i64)> = sqlx::query_as(
        r#"
        SELECT reserve0::text, reserve1::text, price::text, latest_trade_at
        FROM pool WHERE pool_id = $1
        "#,
    )
    .bind(pool_id)
    .fetch_optional(pool)
    .await
    .context("failed to read pool row")?;
    Ok(row)
}

/// Count lp_allocate_history rows for a token.
pub async fn count_lp_allocate(pool: &PgPool, token_id: &str) -> Result<i64> {
    count_rows_for_token(pool, "lp_allocate_history", token_id).await
}

/// Count lp_collect_history rows for a token.
pub async fn count_lp_collect(pool: &PgPool, token_id: &str) -> Result<i64> {
    count_rows_for_token(pool, "lp_collect_history", token_id).await
}

// ============================================================================
// Group C helpers: fee
// ============================================================================

/// Call `BATCH_INSERT_FEE_HISTORY_SQL` with array params.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_fee_history(
    pool: &PgPool,
    transaction_hashes: &[&str],
    tx_indices: &[i32],
    log_indices: &[i32],
    account_ids: &[&str],
    token_ids: &[&str],
    quote_amounts: &[bigdecimal::BigDecimal],
    usd_amounts: &[bigdecimal::BigDecimal],
    fee_types: &[&str],
    block_numbers: &[i64],
    created_ats: &[i64],
) -> Result<()> {
    sqlx::query(observer::db::postgres::controller::fee::BATCH_INSERT_FEE_HISTORY_SQL)
        .bind(transaction_hashes)
        .bind(tx_indices)
        .bind(log_indices)
        .bind(account_ids)
        .bind(token_ids)
        .bind(quote_amounts)
        .bind(usd_amounts)
        .bind(fee_types)
        .bind(block_numbers)
        .bind(created_ats)
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_FEE_HISTORY_SQL")?;
    Ok(())
}

/// Count `fee_history` rows matching a composite PK.
pub async fn count_fee_history(
    pool: &PgPool,
    transaction_hash: &str,
    tx_index: i32,
    log_index: i32,
) -> Result<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM fee_history
        WHERE transaction_hash = $1 AND tx_index = $2 AND log_index = $3
        "#,
    )
    .bind(transaction_hash)
    .bind(tx_index)
    .bind(log_index)
    .fetch_one(pool)
    .await
    .context("failed to count fee_history rows")?;
    Ok(row.0)
}

/// Get the aggregated `fee` row for (account_id, token_id) as (quote_amount, usd_amount) strings.
pub async fn get_fee_aggregate(
    pool: &PgPool,
    account_id: &str,
    token_id: &str,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT quote_amount::text, usd_amount::text FROM fee WHERE account_id = $1 AND token_id = $2",
    )
    .bind(account_id)
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .context("failed to read fee aggregate row")?;
    Ok(row)
}

/// Execute `GET_FALLBACK_PRICE_SQL` directly and return the single price
/// value (as text) if any.
pub async fn call_get_fallback_price(pool: &PgPool) -> Result<Option<String>> {
    let quote_id: &str = &observer::config::WNATIVE_ADDRESS;
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT price::text
        FROM price
        WHERE quote_id = $1
        ORDER BY block_number DESC
        LIMIT 1
        "#,
    )
    .bind(quote_id)
    .fetch_optional(pool)
    .await
    .context("failed to execute fallback price query")?;
    Ok(row.map(|(p,)| p))
}
