mod common;

use anyhow::Result;
use common::{insert_token, setup_test_db};

const TOKEN_MON: &str = "0x1111111111111111111111111111111111111111";
const TOKEN_DEFAULT: &str = "0x2222222222222222222222222222222222222222";
const CREATOR: &str = "0x9999999999999999999999999999999999999999";

#[tokio::test]
async fn token_chain_migration_backfills_defaults_and_rejects_null() -> Result<()> {
    let db = setup_test_db().await?;

    // Simulate an existing deployment where a nullable chain column was
    // introduced before this migration finished configuring it.
    sqlx::raw_sql(
        "ALTER TABLE token ALTER COLUMN chain DROP NOT NULL;
         ALTER TABLE token ALTER COLUMN chain DROP DEFAULT;",
    )
    .execute(&db.pool)
    .await?;

    insert_token(&db.pool, TOKEN_MON, CREATOR).await?;
    let (before,): (Option<String>,) =
        sqlx::query_as("SELECT chain FROM token WHERE token_id = $1")
            .bind(TOKEN_MON)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(before, None);

    let migration = std::fs::read_to_string("migrations/0036_token_chain.sql")?;
    sqlx::raw_sql(&migration).execute(&db.pool).await?;

    let (backfilled,): (String,) =
        sqlx::query_as("SELECT chain FROM token WHERE token_id = $1")
            .bind(TOKEN_MON)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(backfilled, "MON");

    insert_token(&db.pool, TOKEN_DEFAULT, CREATOR).await?;
    let (defaulted,): (String,) =
        sqlx::query_as("SELECT chain FROM token WHERE token_id = $1")
            .bind(TOKEN_DEFAULT)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(defaulted, "MON");

    let null_result = sqlx::query("UPDATE token SET chain = NULL WHERE token_id = $1")
        .bind(TOKEN_DEFAULT)
        .execute(&db.pool)
        .await;
    assert!(null_result.is_err(), "token.chain must reject NULL");

    // The migration is operationally safe to rerun.
    sqlx::raw_sql(&migration).execute(&db.pool).await?;
    Ok(())
}
