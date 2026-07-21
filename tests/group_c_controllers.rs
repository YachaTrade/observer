//! Integration tests for the active Group C fee controller.
//! Each test section validates one controller method at the SQL level via
//! testcontainers-backed Postgres 17.

mod common;

use anyhow::Result;
use common::{
    call_batch_insert_fee_history, call_batch_insert_set_fee_protocols,
    call_handle_set_fee_protocol, count_fee_history, count_set_fee_history, get_fee_aggregate,
    setup_test_db,
};

// Shared test constants
const TOKEN: &str = "0x1111111111111111111111111111111111111111";
const ALICE: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const POOL_ID: &str = "0xpppppppppppppppppppppppppppppppppppppppp";

// ============================================================================
// fee.rs -- set_fee_history + fee_history
// ============================================================================

/// handle_set_fee_protocol: happy path inserts one row.
#[tokio::test]
async fn fee_handle_set_fee_protocol_happy() -> Result<()> {
    let db = setup_test_db().await?;
    call_handle_set_fee_protocol(
        &db.pool, POOL_ID, 100, "0xtx1", 0, 0, 10, 20, 30, 40,
    )
    .await?;
    assert_eq!(count_set_fee_history(&db.pool, POOL_ID, 100, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// handle_set_fee_protocol: duplicate insert is silently ignored (ON CONFLICT DO NOTHING).
#[tokio::test]
async fn fee_handle_set_fee_protocol_duplicate() -> Result<()> {
    let db = setup_test_db().await?;
    call_handle_set_fee_protocol(
        &db.pool, POOL_ID, 100, "0xtx1", 0, 0, 10, 20, 30, 40,
    )
    .await?;
    // Same PK
    call_handle_set_fee_protocol(
        &db.pool, POOL_ID, 100, "0xtx1", 0, 0, 99, 99, 99, 99,
    )
    .await?;
    assert_eq!(count_set_fee_history(&db.pool, POOL_ID, 100, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// batch_insert_set_fee_protocols: inserts multiple rows via UNNEST.
#[tokio::test]
async fn fee_batch_insert_set_fee_protocols_happy() -> Result<()> {
    let db = setup_test_db().await?;
    call_batch_insert_set_fee_protocols(
        &db.pool,
        &[POOL_ID, POOL_ID],
        &[100, 101],
        &["0xtx1", "0xtx2"],
        &[0, 1],
        &[0, 0],
        &[10, 11],
        &[20, 21],
        &[30, 31],
        &[40, 41],
    )
    .await?;
    assert_eq!(count_set_fee_history(&db.pool, POOL_ID, 100, "0xtx1", 0, 0).await?, 1);
    assert_eq!(count_set_fee_history(&db.pool, POOL_ID, 101, "0xtx2", 1, 0).await?, 1);
    Ok(())
}

/// batch_insert_fee_history: happy path inserts and triggers fee aggregate.
#[tokio::test]
async fn fee_batch_insert_fee_history_happy() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let q = bigdecimal::BigDecimal::from_str("1000")?;
    let u = bigdecimal::BigDecimal::from_str("5")?;
    call_batch_insert_fee_history(
        &db.pool,
        &["0xtx1"],
        &[0],
        &[0],
        &[ALICE],
        &[TOKEN],
        &[q],
        &[u],
        &["curve_buy"],
        &[100],
        &[1_700_000_000],
    )
    .await?;
    assert_eq!(count_fee_history(&db.pool, "0xtx1", 0, 0).await?, 1);
    // Trigger should have created a fee aggregate row
    let agg = get_fee_aggregate(&db.pool, ALICE, TOKEN).await?;
    assert!(agg.is_some());
    let (qa, ua) = agg.unwrap();
    assert_eq!(qa, "1000");
    assert_eq!(ua, "5");
    Ok(())
}

/// batch_insert_fee_history: duplicate is silently ignored; aggregate stays the same.
#[tokio::test]
async fn fee_batch_insert_fee_history_duplicate() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let q = bigdecimal::BigDecimal::from_str("1000")?;
    let u = bigdecimal::BigDecimal::from_str("5")?;
    call_batch_insert_fee_history(
        &db.pool,
        &["0xtx1"],
        &[0],
        &[0],
        &[ALICE],
        &[TOKEN],
        &[q.clone()],
        &[u.clone()],
        &["curve_buy"],
        &[100],
        &[1_700_000_000],
    )
    .await?;
    // Insert same PK again
    call_batch_insert_fee_history(
        &db.pool,
        &["0xtx1"],
        &[0],
        &[0],
        &[ALICE],
        &[TOKEN],
        &[q],
        &[u],
        &["curve_buy"],
        &[100],
        &[1_700_000_000],
    )
    .await?;
    assert_eq!(count_fee_history(&db.pool, "0xtx1", 0, 0).await?, 1);
    // Aggregate should NOT double
    let (qa, _) = get_fee_aggregate(&db.pool, ALICE, TOKEN).await?.unwrap();
    assert_eq!(qa, "1000");
    Ok(())
}

/// fee_history trigger: multiple inserts accumulate in fee aggregate.
#[tokio::test]
async fn fee_history_trigger_accumulates() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let q1 = bigdecimal::BigDecimal::from_str("1000")?;
    let u1 = bigdecimal::BigDecimal::from_str("5")?;
    let q2 = bigdecimal::BigDecimal::from_str("2000")?;
    let u2 = bigdecimal::BigDecimal::from_str("10")?;
    // First insert
    call_batch_insert_fee_history(
        &db.pool,
        &["0xtx1"],
        &[0],
        &[0],
        &[ALICE],
        &[TOKEN],
        &[q1],
        &[u1],
        &["curve_buy"],
        &[100],
        &[1_700_000_000],
    )
    .await?;
    // Second insert (different PK)
    call_batch_insert_fee_history(
        &db.pool,
        &["0xtx2"],
        &[0],
        &[0],
        &[ALICE],
        &[TOKEN],
        &[q2],
        &[u2],
        &["swap_buy"],
        &[101],
        &[1_700_000_001],
    )
    .await?;
    let (qa, ua) = get_fee_aggregate(&db.pool, ALICE, TOKEN).await?.unwrap();
    assert_eq!(qa, "3000");
    assert_eq!(ua, "15");
    Ok(())
}
