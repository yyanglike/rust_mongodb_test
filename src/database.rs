use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use std::env;

pub async fn init_db() -> Result<SqlitePool, sqlx::Error> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Create initial tables if they don't exist
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uri TEXT NOT NULL,
            data TEXT NOT NULL
        )
        "#
    )
    .execute(&pool)
    .await?;
    
    Ok(pool)
}