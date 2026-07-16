//! Integration tests for the active Group C fee and point controllers.
//! Each test section validates one controller method at the SQL level via
//! testcontainers-backed Postgres 17.

mod common;

use anyhow::Result;
use common::{
    call_batch_insert_fee_history, call_batch_insert_graduate_points, call_batch_insert_points,
    call_batch_insert_set_fee_protocols, call_handle_set_fee_protocol, count_fee_history,
    count_point_history, count_point_history_for_account, count_set_fee_history,
    get_fee_aggregate, insert_token, setup_test_db,
};

// Shared test constants
const TOKEN: &str = "0x1111111111111111111111111111111111111111";
const CREATOR: &str = "0x9999999999999999999999999999999999999999";
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

// ============================================================================
// point.rs -- point_history + graduate points
// ============================================================================

/// batch_insert_points: happy path inserts one point_history row.
#[tokio::test]
async fn point_batch_insert_happy() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let val = bigdecimal::BigDecimal::from_str("100")?;
    call_batch_insert_points(
        &db.pool,
        &[ALICE],
        &["CURVE"],
        &[val],
        &["0xtx1"],
        &[0],
        &[0],
        &[1_700_000_000],
    )
    .await?;
    assert_eq!(count_point_history(&db.pool, ALICE, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// batch_insert_points: duplicate is silently ignored.
#[tokio::test]
async fn point_batch_insert_duplicate() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let val = bigdecimal::BigDecimal::from_str("100")?;
    call_batch_insert_points(
        &db.pool,
        &[ALICE],
        &["CURVE"],
        &[val.clone()],
        &["0xtx1"],
        &[0],
        &[0],
        &[1_700_000_000],
    )
    .await?;
    call_batch_insert_points(
        &db.pool,
        &[ALICE],
        &["CURVE"],
        &[val],
        &["0xtx1"],
        &[0],
        &[0],
        &[1_700_000_000],
    )
    .await?;
    assert_eq!(count_point_history(&db.pool, ALICE, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// batch_insert_points: multiple distinct events for the same account.
#[tokio::test]
async fn point_batch_insert_multiple() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let v1 = bigdecimal::BigDecimal::from_str("100")?;
    let v2 = bigdecimal::BigDecimal::from_str("200")?;
    call_batch_insert_points(
        &db.pool,
        &[ALICE, ALICE],
        &["CURVE", "DEX"],
        &[v1, v2],
        &["0xtx1", "0xtx2"],
        &[0, 0],
        &[0, 0],
        &[1_700_000_000, 1_700_000_001],
    )
    .await?;
    assert_eq!(count_point_history_for_account(&db.pool, ALICE).await?, 2);
    Ok(())
}

/// batch_insert_graduate_points: happy path looks up token.creator and inserts GRADUATE point.
#[tokio::test]
async fn point_graduate_happy() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    let val = bigdecimal::BigDecimal::from_str("500")?;
    call_batch_insert_graduate_points(
        &db.pool,
        &[TOKEN],
        &["0xtx1"],
        &[0],
        &[0],
        &[val],
        &[1_700_000_000],
    )
    .await?;
    // The creator should have a point_history row with type GRADUATE
    assert_eq!(count_point_history(&db.pool, CREATOR, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// batch_insert_graduate_points: duplicate is silently ignored.
#[tokio::test]
async fn point_graduate_duplicate() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    let val = bigdecimal::BigDecimal::from_str("500")?;
    call_batch_insert_graduate_points(
        &db.pool,
        &[TOKEN],
        &["0xtx1"],
        &[0],
        &[0],
        &[val.clone()],
        &[1_700_000_000],
    )
    .await?;
    call_batch_insert_graduate_points(
        &db.pool,
        &[TOKEN],
        &["0xtx1"],
        &[0],
        &[0],
        &[val],
        &[1_700_000_000],
    )
    .await?;
    assert_eq!(count_point_history(&db.pool, CREATOR, "0xtx1", 0, 0).await?, 1);
    Ok(())
}

/// batch_insert_graduate_points: no token row means no insert (INNER JOIN fails silently).
#[tokio::test]
async fn point_graduate_no_token() -> Result<()> {
    use std::str::FromStr;
    let db = setup_test_db().await?;
    let val = bigdecimal::BigDecimal::from_str("500")?;
    // No token row exists; INNER JOIN returns 0 rows, no error
    call_batch_insert_graduate_points(
        &db.pool,
        &["0xnonexistent_token_address_here_00000"],
        &["0xtx1"],
        &[0],
        &[0],
        &[val],
        &[1_700_000_000],
    )
    .await?;
    // No point_history should exist for any account
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM point_history")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(row.0, 0);
    Ok(())
}
