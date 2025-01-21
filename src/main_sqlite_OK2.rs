use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use chrono::Utc;

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
        
        // Enable foreign key support
        conn.execute("PRAGMA foreign_keys = ON", [])?;
        
        // Create root table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS root (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL
            )",
            [],
        )?;
        
        Ok(Self { conn })
    }

    fn create_tables_recursive(&self, json: &Value, table_name: &str) -> Result<()> {
        if let Value::Object(obj) = json {
            // Collect columns for current level
            let mut columns = Vec::new();
            
            for (key, value) in obj {
                let column_name = key.to_string();
                columns.push(column_name.clone());
                
                if value.is_object() {
                    // Create nested table
                    let nested_table_name = format!("{}_{}", table_name, column_name);
                    self.create_tables_recursive(value, &nested_table_name)?;
                }
            }
            
            // Create current table if it doesn't exist
            self.create_table_if_not_exists(table_name, &columns)?;
        }
        Ok(())
    }

    fn create_table_if_not_exists(&self, table_name: &str, columns: &[String]) -> Result<()> {
        // Check if table exists
        let table_exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?",
            [table_name],
            |row| row.get(0),
        )?;

        if table_exists == 0 {
            // Create new table with dynamic columns
            let mut columns_def = vec![
                "id INTEGER PRIMARY KEY".to_string(),
                "timestamp INTEGER NOT NULL".to_string()
            ];
            
            // Add JSON columns
            for col in columns {
                if col != "id" && col != "timestamp" {
                    columns_def.push(format!("{} TEXT", col));
                }
            }
            
            // Create table with all columns
            self.conn.execute(
                &format!(
                    "CREATE TABLE {} ({})",
                    table_name,
                    columns_def.join(", ")
                ),
                [],
            )?;
        } else {
            // Get existing columns excluding id and timestamp
            let existing_columns: Vec<String> = self.conn
                .prepare(&format!("PRAGMA table_info({})", table_name))?
                .query_map([], |row| {
                    Ok(row.get::<_, String>(1)?) // column name
                })?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|c| c != "id" && c != "timestamp")
                .collect();

            // Add missing columns
            for col in columns {
                if col != "id" && col != "timestamp" && !existing_columns.contains(col) {
                    self.conn.execute(
                        &format!("ALTER TABLE {} ADD COLUMN {} TEXT", table_name, col),
                        [],
                    )?;
                }
            }
        }
        Ok(())
    }

    fn store_json(&self, json: &Value, table_name: Option<&str>) -> Result<()> {
        if let Value::Object(obj) = json {
            // Get current table name
            let current_table_name = table_name.unwrap_or("root");
            
            // First create all necessary tables recursively
            self.create_tables_recursive(json, current_table_name)?;
            
            // Clean up old data before storing new data
            self.cleanup_old_data(current_table_name)?;
            
            // Collect all columns and values for this level
            let mut columns = Vec::new();
            let mut values = Vec::new();
            
            for (key, value) in obj {
                let column_name = key.to_string();
                
                if value.is_object() {
                    // For nested objects, store the path and recurse
                    columns.push(column_name.clone());
                    values.push("OBJECT".to_string());
                    let nested_table_name = format!("{}_{}", current_table_name, column_name);
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
            
            // Check if record exists
            let exists: i64 = self.conn.query_row(
                &format!("SELECT COUNT(*) FROM {} WHERE id = ?", current_table_name),
                [1], // Using id=1 since we're only storing one record per table
                |row| row.get(0),
            )?;

            if exists > 0 {
                // Update existing record
                let updates = columns.iter()
                    .map(|col| format!("{} = ?", col))
                    .collect::<Vec<_>>()
                    .join(", ");
                
                let mut stmt = self.conn.prepare(
                    &format!(
                        "UPDATE {} SET timestamp = ?, {} WHERE id = 1",
                        current_table_name,
                        updates
                    )
                )?;
                
                let mut params = vec![Utc::now().timestamp().to_string()];
                params.extend(values.clone());
                stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            } else {
                // Insert new record
                let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
                let mut stmt = self.conn.prepare(
                    &format!(
                        "INSERT INTO {} (id, timestamp, {}) VALUES (1, ?, {})",
                        current_table_name,
                        columns.join(", "),
                        placeholders
                    )
                )?;
                
                let mut params = vec![Utc::now().timestamp().to_string()];
                params.extend(values.clone());
                stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            }
            
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

    /// Query JSON documents by key-value pair
    fn query_by_key_value(&self, search_key: &str, search_value: &str) -> Result<Vec<Value>> {
        // Get all tables that might contain the key
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )?;
        
        let tables = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(0)?)
        })?
        .filter_map(|t| t.ok())
        .collect::<Vec<_>>();

        let mut results = Vec::new();
        
        for table in tables {
            // Check if table has the search key
            let mut stmt = self.conn.prepare(
                &format!("PRAGMA table_info({})", table)
            )?;
            
            let has_key = stmt.query_map([], |row| {
                Ok(row.get::<_, String>(1)?)
            })?
            .filter_map(|c| c.ok())
            .any(|col| col == search_key);

            if has_key {
                // Get all columns except id and timestamp
                let mut stmt = self.conn.prepare(
                    &format!("PRAGMA table_info({})", table)
                )?;
                
                let columns = stmt.query_map([], |row| {
                    Ok(row.get::<_, String>(1)?)
                })?
                .filter_map(|c| c.ok())
                .filter(|c| c != "id" && c != "timestamp")
                .collect::<Vec<_>>();

                // Build query to get latest version of matching records
                let query = format!(
                    "SELECT {} FROM {} WHERE {} = ? AND timestamp = (
                        SELECT MAX(timestamp) FROM {} WHERE {} = ?
                    )",
                    columns.join(", "),
                    table,
                    search_key,
                    table,
                    search_key
                );
                
                let mut stmt = self.conn.prepare(&query)?;
                let rows = stmt.query_map([search_value, search_value], |row| {
                    // Reconstruct JSON from row
                    let mut map = serde_json::Map::new();
                    
                    for (i, col) in columns.iter().enumerate() {
                        let value: String = row.get(i)?;
                        if value == "OBJECT" {
                            // Handle nested object
                            let nested_table = if table == "root" {
                                col.to_string()
                            } else {
                                format!("{}_{}", table, col)
                            };
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
                    
                    Ok(Value::Object(map))
                })?;
                
                for row in rows {
                    if let Ok(json) = row {
                        results.push(json);
                    }
                }
            }
        }
        
        Ok(results)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = JsonStore::new("data.db")?;

    // Store multiple JSON documents with different structures
    let users = vec![
        serde_json::json!({
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
            }
        }),
        serde_json::json!({
            "user": {
                "name": "Emily",
                "active": false,
                "address": {
                    "street": "456 Elm St",
                    "city": "Los Angeles",
                    "location": {
                        "coordinates": {
                            "latitude": 34.0522,
                            "longitude": -118.2437
                        }
                    },
                    "tags": ["work", "secondary"]
                }
            }
        }),
        serde_json::json!({
            "user": {
                "name": "Michael",
                "active": true,
                "address": {
                    "street": "789 Oak St",
                    "city": "Chicago",
                    "location": {
                        "coordinates": {
                            "latitude": 41.8781,
                            "longitude": -87.6298
                        }
                    },
                    "tags": ["home", "primary"]
                }
            }
        }),
        serde_json::json!({
            "customer": {
                "first_name": "Alice",
                "last_name": "Smith",
                "status": "active",
                "contact": {
                    "email": "alice@example.com",
                    "phone": "555-1234"
                },
                "preferences": {
                    "newsletter": true,
                    "notifications": {
                        "email": true,
                        "sms": false
                    }
                }
            }
        }),
        serde_json::json!({
            "employee": {
                "id": 1001,
                "name": "Bob Johnson",
                "department": "Engineering",
                "skills": ["Rust", "Python", "JavaScript"],
                "manager": {
                    "name": "Sarah Lee",
                    "email": "sarah@company.com"
                }
            }
        })
    ];

    for json in users {
        println!("\nStoring document: {}", serde_json::to_string_pretty(&json)?);
        match store.store_json(&json, None) {
            Ok(_) => println!("Stored JSON document with top-level key: {}", json.as_object().unwrap().keys().next().unwrap()),
            Err(e) => eprintln!("Error storing document: {}", e),
        }
    }

    // Test queries across different documents
    println!("\nTesting queries across different documents:");
    
    // Query by name across all documents
    println!("\nSearching for name 'John':");
    let results = store.query_by_key_value("name", "John")?;
    for (i, result) in results.iter().enumerate() {
        println!("\nMatch {}:\n{}", i + 1, serde_json::to_string_pretty(result)?);
    }

    // Query by email across all documents
    println!("\nSearching for email 'alice@example.com':");
    let results = store.query_by_key_value("email", "alice@example.com")?;
    for (i, result) in results.iter().enumerate() {
        println!("\nMatch {}:\n{}", i + 1, serde_json::to_string_pretty(result)?);
    }

    // Query by department across all documents
    println!("\nSearching for department 'Engineering':");
    let results = store.query_by_key_value("department", "Engineering")?;
    for (i, result) in results.iter().enumerate() {
        println!("\nMatch {}:\n{}", i + 1, serde_json::to_string_pretty(result)?);
    }

    // Test cleanup functionality
    println!("\nTesting cleanup functionality...");
    
    // Create test data with old timestamp using user's JSON structure
    let old_timestamp = Utc::now().timestamp() - (30 * 24 * 60 * 60);
    let user_json = serde_json::json!({
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
        }
    });
    
    // Store with old timestamp
    store.store_json(&user_json, None)?;
    
    // Manually update timestamp to be old
    store.conn.execute(
        "UPDATE root SET timestamp = ?",
        [old_timestamp],
    )?;

  // Verify cleanup results
    let count: i64 = store.conn
        .query_row("SELECT COUNT(*) FROM root", [], |row| row.get(0))?;
    println!("Rows in root table after cleanup: {}", count);

    // Query child tables
    let child_tables = store.get_child_tables("root")?;
    for child_table in child_tables {
        let count: i64 = store.conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", child_table), [], |row| row.get(0))?;
        println!("Rows in '{}' table after cleanup: {}", child_table, count);
    }

    // Test query by key-value
    println!("\nTesting query by key-value...");
    let results = store.query_by_key_value("name", "John")?;
    for (i, result) in results.iter().enumerate() {
        println!("\nMatch {}:\n{}", i + 1, serde_json::to_string_pretty(result)?);
    }

    // Test query by nested key-value
    println!("\nTesting query by nested key-value...");
    let results = store.query_by_key_value("city", "New York")?;
    for (i, result) in results.iter().enumerate() {
        println!("\nMatch {}:\n{}", i + 1, serde_json::to_string_pretty(result)?);
    }
    // Clean up data older than 7 days
    store.cleanup_old_data_with_age("root", 7)?;

  
    Ok(())
}
