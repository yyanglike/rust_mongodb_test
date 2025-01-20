use mongodb::{bson::{doc, Bson, oid::ObjectId, Document}, options::{ClientOptions, FindOptions}, Client, Collection};
use serde_json::Value;
use std::error::Error;
use serde::{Deserialize, Serialize};
use futures::StreamExt;
use async_recursion::async_recursion;
use std::pin::Pin; // Import Pin
use std::future::Future; // Import Future

struct JsonTreeManager {
    collection: Collection<Document>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub key: String,
    pub value: Option<String>,
    pub is_leaf: bool,
    pub parent_id: Option<mongodb::bson::oid::ObjectId>,
}

impl JsonTreeManager {
    async fn new(uri: &str, db_name: &str, collection_name: &str) -> Result<Self, Box<dyn Error>> {
        let client_options = ClientOptions::parse(uri).await?;
        let client = Client::with_options(client_options)?;
        let db = client.database(db_name);
        let collection = db.collection(collection_name);
        collection.delete_many(doc! {}).await?;
        Ok(JsonTreeManager { collection })
    }

    async fn build_path(&self, node_path: &str) -> Result<ObjectId, Box<dyn Error>> {
        let parts: Vec<&str> = node_path.trim_start_matches('/').split('/').collect();
        let mut current_parent_id: Option<ObjectId> = None;

        for part in parts {
            let filter = doc! { "parent_id": current_parent_id.clone(), "key": part };
            if let Some(doc) = self.collection.find_one(filter.clone()).await? {
                current_parent_id = doc.get_object_id("_id").ok().map(|id| id.clone());
            } else {
                let new_doc = doc! { "parent_id": current_parent_id.clone(), "key": part, "value": None::<String>, "is_leaf": false };
                let result = self.collection.insert_one(new_doc).await?;
                
                current_parent_id = result.inserted_id.as_object_id().map(|id| id.clone());
            }
        }
        Ok(current_parent_id.unwrap())
    }

    async fn store_json_data(&self, node_path: &str, json_data: &Value, overwrite: bool) -> Result<(), Box<dyn Error>> {
        let leaf_node_id = self.build_path(node_path).await?;
        self.flatten(json_data, Some(leaf_node_id), overwrite).await
    }

    fn flatten<'a>(
        &'a self,
        data: &'a Value,
        parent_id: Option<ObjectId>,
        overwrite: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + 'a>> {
        Box::pin(async move {
            match data {
                Value::Object(map) => {
                    for (key, value) in map {
                        self.process_entry(key, value, parent_id, overwrite).await?;
                    }
                }
                Value::Array(arr) => {
                    for (index, item) in arr.iter().enumerate() {
                        self.process_entry(&index.to_string(), item, parent_id, overwrite).await?;
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }

    async fn process_entry(&self, key: &str, value: &Value, parent_id: Option<ObjectId>, overwrite: bool) -> Result<(), Box<dyn Error>> {
        let is_leaf = !matches!(value, Value::Object(_) | Value::Array(_));
        let filter = doc! { "parent_id": parent_id, "key": key };

        if overwrite {
            self.collection.update_one(
                filter.clone(),
                doc! { "$set": { 
                    "value": if is_leaf { Some(value.to_string()) } else { None }, 
                    "is_leaf": is_leaf 
                }}
            ).await?;
        } else {
            let new_doc = doc! { 
                "parent_id": parent_id, 
                "key": key, 
                "value": if is_leaf { Some(value.to_string()) } else { None }, 
                "is_leaf": is_leaf 
            };
            self.collection.insert_one(new_doc).await?;
        }

        if !is_leaf {
            self.flatten(value, parent_id, overwrite).await?;
        }

        Ok(())
    }

    #[async_recursion]
    pub async fn query_data_by_path(
        &self,
        node_path: &str,
        page: u64,
        page_size: u64,
        max_depth: Option<i64>,
        sort_key: &str,
        sort_order: i64,
    ) -> Result<Bson, Box<dyn Error>> {
        let parts: Vec<&str> = node_path.split('/').skip(1).collect();
        let mut current_parent_id = None;
    
        for part in parts {
            let query = doc! {
                "parent_id": current_parent_id,
                "key": part,
            };
    
            let doc = self.collection.find_one(query).await.map_err(|e| Box::new(e) as Box<dyn Error>)?;
            match doc {
                Some(d) => {
                    current_parent_id = d.get("_id").and_then(|id| id.as_object_id()).map(|id| id.clone());
                }
                None => {
                    return Err(format!("Path '{}' not found.", node_path).into());
                }
            }
        }
    
        let skip = (page - 1) * page_size;
        let query = doc! { "parent_id": current_parent_id };
    
        let sort_criteria = if sort_order == 1 {
            doc! { sort_key: 1 } // Ascending
        } else {
            doc! { sort_key: -1 } // Descending
        };
    
        let _options = FindOptions::builder() // Prefix with underscore to suppress warning
            .skip(skip)
            .limit(page_size as i64)
            .sort(sort_criteria)
            .build();
    
        let mut cursor = self.collection.find(query).await.map_err(|e| Box::new(e) as Box<dyn Error>)?;
        let mut result = Bson::Document(Document::new());
    
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(d) => {
                    let key = d.get_str("key").map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    if d.get_bool("is_leaf").map_err(|e| Box::new(e) as Box<dyn Error>)? {
                        let value = d.get_str("value").map(|v| v.to_string()).unwrap_or_default();
                        if let Some(doc) = result.as_document_mut() {
                            doc.insert(key.to_string(), Bson::String(value));
                        } else {
                            return Err("Result is not a document".into());
                        }
                    } else {
                        if max_depth.is_none() || max_depth.unwrap_or(0) > 1 {
                            let sub_path = format!("{}/{}", node_path, key);
                            let sub_result = self.query_data_by_path(
                                &sub_path,
                                1,
                                page_size,
                                max_depth.map(|depth| depth - 1),
                                sort_key,
                                sort_order,
                            ).await?;
                            if let Some(doc) = result.as_document_mut() {
                                doc.insert(key.to_string(), sub_result);
                            } else {
                                return Err("Result is not a document".into());
                            }
                        } else {
                            if let Some(doc) = result.as_document_mut() {
                                doc.insert(key.to_string(), Bson::String("...".to_string()));
                            } else {
                                return Err("Result is not a document".into());
                            }
                        }
                    }
                }
                Err(e) => return Err(Box::new(e) as Box<dyn Error>),
            }
        }
    
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 初始化 JsonTreeManager
    let manager = JsonTreeManager::new("mongodb://Admin:Password@192.168.1.23:27017/json_tree_db?authSource=admin&tls=false", "json_tree_db", "json_tree_collection").await?;

    // 定义多组 JSON 数据
    let json_data_list: Vec<serde_json::Value> = vec![
        serde_json::from_str::<serde_json::Value>(r#"
        {
            "fullQuoteStr": {
                "a": "1598845083",
                "b": "上证指数",
                "c": "SH000001",
                "d": "4",
                "e": "some_value"
            },
            "time": 1598845095156,
            "stockCode": "SH000001"
        }
        "#)?,
        serde_json::from_str::<serde_json::Value>(r#"
        {
            "fullQuoteStr": {
                "a": "1598845084",
                "b": "深证成指",
                "c": "SZ399001",
                "d": "5",
                "e": "another_value"
            },
            "time": 1598845095157,
            "stockCode": "SZ399001"
        }
        "#)?,
        serde_json::from_str::<serde_json::Value>(r#"
        {
            "fullQuoteStr": {
                "a": "1598845085",
                "b": "创业板指",
                "c": "SZ399006",
                "d": "6",
                "e": "yet_another_value"
            },
            "time": 1598845095158,
            "stockCode": "SZ399006"
        }
        "#)?,
    ];

    // 插入多组数据
    for json_data in json_data_list {
        let node_path = format!("/quote_provider_dev/{}", json_data["stockCode"].as_str().unwrap());
        manager.store_json_data(&node_path, &json_data, false).await?;
        println!("Inserted data for path: {}", node_path);
    }

    // 查询数据
    let node_path = "/quote_provider_dev";
    let page = 1; // 第一页
    let page_size = 10; // 每页 10 条数据
    let max_depth = Some(2); // 最大深度为 2
    let sort_key = "d"; // 按 key 字段排序
    let sort_order = -1; // 升序

    let result = manager.query_data_by_path(node_path, page, page_size, max_depth, sort_key, sort_order).await?;

    // 打印查询结果
    println!("Query Result: {:?}", result);

    Ok(())
}