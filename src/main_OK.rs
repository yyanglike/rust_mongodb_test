use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use chrono::Utc;
use std::io::Write;

#[derive(Debug, Serialize, Deserialize)]
struct JsonNode {
    id: Option<i64>,
    table_name: String,
    key: String,
    value: Option<String>,
    is_object: bool,
    child_table: Option<String>,
    timestamp: i64,
}

struct JsonStore {
    conn: Connection,
}

impl JsonStore {
    fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Ok(Self { conn })
    }

    fn create_table(&self, table_name: &str, columns: &[String]) -> Result<()> {
        println!("Creating table {} with columns: {:?}", table_name, columns);
        std::io::stdout().flush().unwrap();
        let columns_def = columns
            .iter()
            .map(|col| format!("{} TEXT", col))
            .collect::<Vec<_>>()
            .join(", ");
            
        // Drop existing table if it exists
        self.conn.execute(
            &format!("DROP TABLE IF EXISTS {}", table_name),
            [],
        )?;
        
        // Create new table with current schema
        self.conn.execute(
            &format!(
                "CREATE TABLE {} (
                    id INTEGER PRIMARY KEY,
                    timestamp INTEGER NOT NULL,
                    {}
                )",
                table_name, columns_def
            ),
            [],
        )?;
        Ok(())
    }

    fn store_json(&self, json: &Value, table_name: Option<&str>) -> Result<()> {
        // Clean up old data before storing new data
        self.cleanup_old_data(table_name.unwrap_or("root"))?;
        
        if let Value::Object(obj) = json {
            // Collect all columns for this level
            let mut columns = Vec::new();
            let mut values = Vec::new();
            
            for (key, value) in obj {
                let column_name = key.to_string();
                
                if value.is_object() {
                    // For nested objects, store the path and recurse
                    columns.push(column_name.clone());
                    values.push("OBJECT".to_string());
                    let nested_table_name = if let Some(parent_table) = table_name {
                        format!("{}_{}", parent_table, column_name)
                    } else {
                        column_name.clone()
                    };
                    self.store_json(value, Some(&nested_table_name))?;
                } else if value.is_array() {
                    // For arrays, store as JSON string
                    columns.push(column_name.clone());
                    values.push(value.to_string());
                } else {
                    // For primitive values, store directly
                    columns.push(column_name.clone());
                    match value {
                        Value::Null => values.push("null".to_string()),
                        Value::Bool(b) => values.push(b.to_string()),
                        _ => values.push(value.to_string().trim_matches('"').to_string()),
                    }
                }
            }
            
            // Create table with all columns if not exists
            let current_table_name = table_name.unwrap_or("root");
            self.create_table(current_table_name, &columns)?;
            
            // Insert row
            let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let mut stmt = self.conn.prepare(
                &format!(
                    "INSERT INTO {} (timestamp, {}) VALUES (?, {})",
                    current_table_name,
                    columns.join(", "),
                    placeholders
                )
            )?;
            
            let mut params = vec![Utc::now().timestamp().to_string()];
            params.extend(values);
            stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            
            Ok(())
        } else {
            Err(rusqlite::Error::InvalidQuery)
        }
    }

    fn cleanup_old_data(&self, table_name: &str) -> Result<()> {
        self.cleanup_old_data_with_age(table_name, 10)
    }

    fn cleanup_old_data_with_age(&self, table_name: &str, days: i64) -> Result<()> {
        let cutoff = Utc::now().timestamp() - (days * 24 * 60 * 60);
        self.conn.execute(
            &format!("DELETE FROM {} WHERE timestamp < ?", table_name),
            [cutoff],
        )?;
        
        // Recursively clean up child tables
        let child_tables = self.get_child_tables(table_name)?;
        for child_table in child_tables {
            self.cleanup_old_data_with_age(&child_table, days)?;
        }
        Ok(())
    }


    fn get_child_tables(&self, table_name: &str) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? || '_%'"
        )?;
        
        let child_tables = stmt.query_map([format!("{}_", table_name)], |row| {
            Ok(row.get::<_, String>(0)?)
        })?
        .filter_map(|c| c.ok())
        .collect::<Vec<_>>();
        
        Ok(child_tables)
    }

    fn query_json(&self, table_name: &str) -> Result<Value> {
        // Get all columns in the table
        let mut stmt = self.conn.prepare(
            &format!("PRAGMA table_info({})", table_name)
        )?;
        
        let columns = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(1)?) // column name
        })?
        .filter_map(|c| c.ok())
        .filter(|c| c != "id" && c != "timestamp")
        .collect::<Vec<_>>();

        // Query the latest row
        if columns.is_empty() {
            return Ok(Value::Object(serde_json::Map::new()));
        }
        
        let query = format!("SELECT {} FROM {} ORDER BY timestamp DESC LIMIT 1", 
            columns.join(", "), table_name);
        
        let mut stmt = match self.conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                eprintln!("Failed to prepare query: {}: {}", query, e);
                return Ok(Value::Object(serde_json::Map::new()));
            }
        };

        let mut map = serde_json::Map::new();
        match stmt.query_row([], |row| {
            for (i, col) in columns.iter().enumerate() {
                let value: String = row.get(i)?;
                if value == "OBJECT" {
                    // Handle nested object
                    let nested_table = if table_name == "root" {
                        col.to_string()
                    } else {
                        format!("{}_{}", table_name, col)
                    };
                    println!("Querying nested table: {}", nested_table);
                    let nested = self.query_json(&nested_table)?;
                    map.insert(col.to_string(), nested);
                } else {
                    // Handle primitive value
                    if let Ok(parsed) = serde_json::from_str::<Value>(&value) {
                        map.insert(col.to_string(), parsed);
                    } else {
                        map.insert(col.to_string(), Value::String(value));
                    }
                }
            }
            Ok(())
        }) {
            Ok(_) => (),
            Err(_) => return Ok(Value::Object(serde_json::Map::new())),
        }

        Ok(Value::Object(map))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = JsonStore::new("data.db")?;

    // Example JSON
    let json = serde_json::json!({
        "user": {
            "name": "John",
            "active": true,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "location": {
                    "coordinates": {
                        "latitude": 40.7128,
                        "longitude": -74.0060
                    }
                },
                "tags": ["home", "primary"]
            }
        },
        "age": 30,
        "metadata": null
    });

    // Store JSON
    store.store_json(&json, None)?;
    println!("JSON data stored successfully");

    // Query JSON
    let result = store.query_json("root")?;
    match serde_json::to_string_pretty(&result) {
        Ok(json_str) => println!("Queried JSON: {}", json_str),
        Err(e) => eprintln!("Error formatting JSON: {}", e),
    }

    // Test cleanup functionality
    println!("\nTesting cleanup functionality...");
    
    // Create test data with old timestamp using existing columns
    let old_timestamp = Utc::now().timestamp() - (30 * 24 * 60 * 60);
    store.conn.execute(
        "INSERT INTO root (timestamp, age, metadata) VALUES (?, ?, ?)",
        &[&old_timestamp as &dyn rusqlite::ToSql, &30, &"test_data"],
    )?;
    
    store.conn.execute(
        "INSERT INTO user (timestamp, name, active) VALUES (?, ?, ?)",
        &[&old_timestamp as &dyn rusqlite::ToSql, &"old_user", &false],
    )?;

    // Clean up data older than 7 days
    store.cleanup_old_data_with_age("root", 7)?;

    // Verify cleanup results
    let count: i64 = store.conn
        .query_row("SELECT COUNT(*) FROM root", [], |row| row.get(0))?;
    println!("Rows in root table after cleanup: {}", count);

    // Query child tables
    let child_tables = store.get_child_tables("root")?;
    for child_table in child_tables {
        let count: i64 = store.conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", child_table), [], |row| row.get(0))?;
        println!("Rows in {} table after cleanup: {}", child_table, count);
    }

    Ok(())
}
