use actix_web::{web, App, HttpServer};
use dotenv::dotenv;
use crate::database::init_db;
use crate::handlers::{insert_json, get_all_json, get_json_by_id};

mod database;
mod models;
mod handlers;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let pool = init_db().await.expect("Failed to initialize database");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .route("/{uri}", web::post().to(insert_json))
            .route("/{uri}", web::get().to(get_all_json))
            .route("/{uri}/{id}", web::get().to(get_json_by_id))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}