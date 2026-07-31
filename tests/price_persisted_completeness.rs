use anyhow::Result;
use bigdecimal::BigDecimal;
use observer::config::QuoteConfig;
use observer::db::postgres::{
    PostgresDatabase,
    controller::price::{PriceController, has_persisted_prices_at_blocks},
};
use observer::event::common::price::stream::retain_unpersisted_buckets_from_pool;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

async fn create_price_table(pool: &PgPool, temporary: bool) -> Result<()> {
    let table_kind = if temporary { "TEMP " } else { "" };
    sqlx::query(&format!(
        "CREATE {table_kind}TABLE price (quote_id varchar NOT NULL, block_number bigint NOT NULL, price numeric NOT NULL, created_at bigint NOT NULL, PRIMARY KEY (quote_id, block_number))"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn test_pool() -> Result<(PgPool, Option<ContainerAsync<Postgres>>)> {
    if let Ok(database_url) = std::env::var("PYTH_TEST_DATABASE_URL") {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&database_url)
            .await?;
        create_price_table(&pool, true).await?;
        return Ok((pool, None));
    }

    let container = Postgres::default().with_tag("17-alpine").start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await?;
    create_price_table(&pool, false).await?;
    Ok((pool, Some(container)))
}

async fn seed(pool: &PgPool, quote: &str, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO price (quote_id, block_number, price, created_at) VALUES ($1,$2,$3,$4)",
    )
    .bind(quote)
    .bind(block)
    .bind(BigDecimal::from_str("1.00")?)
    .bind(1_700_000_000_i64)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn canonical_completeness_requires_every_exact_requested_block() -> Result<()> {
    let (pool, _container) = test_pool().await?;
    let quote = "0x1111111111111111111111111111111111111111";
    for block in [100_i64, 102] {
        sqlx::query(
            "INSERT INTO price (quote_id, block_number, price, created_at) VALUES ($1,$2,$3,$4)",
        )
        .bind(quote)
        .bind(block)
        .bind(BigDecimal::from_str("1.00")?)
        .bind(1_700_000_000_i64)
        .execute(&pool)
        .await?;
    }

    assert!(has_persisted_prices_at_blocks(&pool, quote, &[]).await);
    assert!(has_persisted_prices_at_blocks(&pool, quote, &[100, 100, 102]).await);
    assert!(!has_persisted_prices_at_blocks(&pool, quote, &[100, 101, 102]).await);

    sqlx::query("ALTER TABLE price RENAME TO price_unavailable")
        .execute(&pool)
        .await?;
    assert!(
        !has_persisted_prices_at_blocks(&pool, quote, &[100, 102]).await,
        "database errors are not evidence of persistence"
    );
    Ok(())
}

#[tokio::test]
async fn bucket_filter_requires_all_blocks_for_every_quote_and_retains_on_db_error() -> Result<()> {
    let (pool, _container) = test_pool().await?;
    let quote_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let quote_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let quotes = [quote_a, quote_b]
        .into_iter()
        .map(|address| QuoteConfig {
            address: address.to_string(),
            pyth_feed_id: format!("feed-{address}"),
            decimals: BigDecimal::from(18),
        })
        .collect::<Vec<_>>();
    let buckets = BTreeMap::from([(60, vec![(100, 1_000), (101, 1_001), (102, 1_002)])]);

    for quote in [quote_a, quote_b] {
        seed(&pool, quote, 100).await?;
        seed(&pool, quote, 102).await?;
    }
    assert_eq!(
        retain_unpersisted_buckets_from_pool(&pool, &quotes, buckets.clone())
            .await?
            .len(),
        1,
        "endpoint rows must not hide an interior gap"
    );

    seed(&pool, quote_a, 101).await?;
    assert_eq!(
        retain_unpersisted_buckets_from_pool(&pool, &quotes, buckets.clone())
            .await?
            .len(),
        1,
        "one incomplete quote retains the shared bucket"
    );

    seed(&pool, quote_b, 101).await?;
    assert!(
        retain_unpersisted_buckets_from_pool(&pool, &quotes, buckets.clone())
            .await?
            .is_empty(),
        "only canonical DB completeness drops the bucket"
    );

    sqlx::query("ALTER TABLE price RENAME TO price_unavailable")
        .execute(&pool)
        .await?;
    assert_eq!(
        retain_unpersisted_buckets_from_pool(&pool, &quotes, buckets)
            .await?
            .len(),
        1,
        "a DB error must retain the bucket for a safe refetch"
    );
    Ok(())
}

#[tokio::test]
async fn multi_quote_write_is_atomic_and_replay_returns_canonical_prices() -> Result<()> {
    let (pool, _container) = test_pool().await?;
    let controller = PriceController::new(Arc::new(PostgresDatabase { pool: pool.clone() }));
    let quote_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let quote_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let attempted = vec![
        (quote_a.to_string(), 100, BigDecimal::from(7), 1_000),
        (quote_b.to_string(), 100, BigDecimal::from(8), 1_000),
    ];

    let oversized = i64::MAX as u64 + 1;
    let overflow = vec![(
        quote_a.to_string(),
        oversized,
        BigDecimal::from(1),
        oversized,
    )];
    assert!(
        controller
            .persist_price_batch(&overflow)
            .await
            .unwrap_err()
            .to_string()
            .contains("out of PostgreSQL BIGINT range")
    );
    let overflow_bucket = BTreeMap::from([(60, vec![(oversized, 1_000)])]);
    assert!(
        retain_unpersisted_buckets_from_pool(&pool, &[], overflow_bucket)
            .await
            .unwrap_err()
            .to_string()
            .contains("out of PostgreSQL BIGINT range")
    );

    sqlx::query(&format!(
        "ALTER TABLE price ADD CONSTRAINT reject_quote_b CHECK (quote_id <> '{quote_b}')"
    ))
    .execute(&pool)
    .await?;
    assert!(controller.persist_price_batch(&attempted).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM price")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0, "one rejected quote must roll back every quote");

    sqlx::query("ALTER TABLE price DROP CONSTRAINT reject_quote_b")
        .execute(&pool)
        .await?;
    let first = controller.persist_price_batch(&attempted).await?;
    assert_eq!(
        first,
        vec![
            (quote_a.to_string(), 100, BigDecimal::from(7)),
            (quote_b.to_string(), 100, BigDecimal::from(8)),
        ]
    );

    let changed_replay = vec![
        (quote_a.to_string(), 100, BigDecimal::from(70), 1_200),
        (quote_b.to_string(), 100, BigDecimal::from(80), 1_200),
    ];
    let replayed = controller.persist_price_batch(&changed_replay).await?;
    assert_eq!(
        replayed, first,
        "cache input must match canonical conflict rows"
    );
    Ok(())
}
