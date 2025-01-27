use actix_web::{web, HttpResponse};
use serde_json::Value;
use sqlx::{SqlitePool, Row};
use crate::models::JsonData;

// 动态创建表
async fn create_table(pool: &SqlitePool, table_name: &str, data: &Value) -> Result<(), sqlx::Error> {
    let mut fields = Vec::new();
    for (key, value) in data.as_object().unwrap() {
        let field_type = match value {
            Value::String(_) => "TEXT",
            Value::Number(_) => "INTEGER",
            Value::Bool(_) => "BOOLEAN",
            Value::Object(_) => "TEXT", // 嵌套对象存储为 JSON 字符串
            _ => "TEXT",
        };
        fields.push(format!("{} {}", key, field_type));
    }

    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT, {})",
        table_name,
        fields.join(", ")
    );

    sqlx::query(&query).execute(pool).await?;
    Ok(())
}

// 插入 JSON 数据
pub async fn insert_json(
    data: web::Json<JsonData>,
    pool: web::Data<SqlitePool>,
) -> HttpResponse {
    let json_data = data.into_inner();
    let table_name = json_data.uri.replace("/", "_");

    // 动态创建表
    if let Err(e) = create_table(&pool, &table_name, &json_data.data).await {
        return HttpResponse::InternalServerError().json(format!("Failed to create table: {}", e));
    }

    // 插入数据
    let fields = json_data.data.as_object().unwrap().keys().map(|k| k.as_str()).collect::<Vec<_>>().join(", ");
    let values = json_data.data.as_object().unwrap().values().map(|v| format!("'{}'", v)).collect::<Vec<_>>().join(", ");

    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name, fields, values
    );

    if let Err(e) = sqlx::query(&query).execute(&**pool).await {
        return HttpResponse::InternalServerError().json(format!("Failed to insert data: {}", e));
    }

    HttpResponse::Ok().json("Data inserted successfully")
}

// 查询所有 JSON 数据
pub async fn get_all_json(
    uri: web::Path<String>,
    pool: web::Data<SqlitePool>,
) -> HttpResponse {
    let table_name = uri.replace("/", "_");

    let rows = sqlx::query(&format!("SELECT * FROM {}", table_name))
        .fetch_all(&**pool)
        .await;

    match rows {
        Ok(rows) => {
            let result: Vec<serde_json::Value> = rows.iter()
                .map(|row| {
                    let mut map = serde_json::Map::new();
                    for i in 0..row.len() {
                        let value: Value = row.try_get(i).unwrap();
                        map.insert(i.to_string(), value);
                    }
                    Value::Object(map)
                })
                .collect();
            HttpResponse::Ok().json(result)
        }
        Err(e) => HttpResponse::InternalServerError().json(format!("Failed to query data: {}", e)),
    }
}

// 查询特定 JSON 数据
pub async fn get_json_by_id(
    path: web::Path<(String, i32)>,
    pool: web::Data<SqlitePool>,
) -> HttpResponse {
    let (uri, id) = path.into_inner();
    let table_name = uri.replace("/", "_");

    let row = sqlx::query(&format!("SELECT * FROM {} WHERE id = $1", table_name))
        .bind(id)
        .fetch_one(&**pool)
        .await;

    match row {
        Ok(row) => {
            let mut map = serde_json::Map::new();
            for i in 0..row.len() {
                let value: Value = row.try_get(i).unwrap();
                map.insert(i.to_string(), value);
            }
            HttpResponse::Ok().json(Value::Object(map))
        }
        Err(e) => HttpResponse::InternalServerError().json(format!("Failed to query data: {}", e)),
    }
}