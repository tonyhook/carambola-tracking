mod entity;
mod service;
mod tracker;

use std::{fs::File, io::Read, sync::OnceLock};

use axum::{routing::get, Router};
use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio_cron_scheduler::{Job, JobScheduler};
use tower_http::{compression::CompressionLayer, decompression::RequestDecompressionLayer};
use yaml_rust2::YamlLoader;

use service::*;
use tracker::*;

#[derive(Serialize, Deserialize)]
pub struct EnvConfig {
    pub storage_path: String,

    pub db_connection: String,

    pub performance_connection_write: String,
    pub notification_connection_write: String,
    pub notification_connection_read: String,
    pub trafficcontrol_connection_write: String,
    pub antifraud_connection_write: String,

    pub performance_interval: u32,

    pub listen_address: String,
    pub listen_port: String,
}

impl EnvConfig {
    fn new() -> Self {
        let mut file = File::open("tracking.yml").unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).expect("Failed to read tracking.yml");

        let docs = YamlLoader::load_from_str(&buffer)
            .expect("tracking.yml parsing failed!");
        let yaml = &docs[0];

        EnvConfig {
            storage_path: yaml["storage_path"].as_str().unwrap_or("").to_string(),

            db_connection: yaml["db_connection"].as_str().unwrap_or("").to_string(),

            performance_connection_write: yaml["performance_connection_write"].as_str().unwrap_or("").to_string(),
            notification_connection_write: yaml["notification_connection_write"].as_str().unwrap_or("").to_string(),
            notification_connection_read: yaml["notification_connection_read"].as_str().unwrap_or("").to_string(),
            trafficcontrol_connection_write: yaml["trafficcontrol_connection_write"].as_str().unwrap_or("").to_string(),
            antifraud_connection_write: yaml["antifraud_connection_write"].as_str().unwrap_or("").to_string(),

            performance_interval: yaml["performance_interval"].as_i64().unwrap_or(0) as u32,

            listen_address: yaml["listen_address"].as_str().unwrap_or("").to_string(),
            listen_port: yaml["listen_port"].as_i64().unwrap_or(0).to_string(),
        }
    }
}
static GLOBAL_CONFIG: OnceLock<EnvConfig> = OnceLock::new();

#[tokio::main]
async fn main() {
    match GLOBAL_CONFIG.set(EnvConfig::new()) {
        Ok(_) => (),
        Err(_) => panic!("Could not get configuration!"),
    }

    let database = Database::new(&GLOBAL_CONFIG.get().unwrap().db_connection);
    let cache = Cache::new(&GLOBAL_CONFIG.get().unwrap());
    let tracking_v1 = TrackingV1::new();

    let sched_db = JobScheduler::new().await.unwrap();
    let database_for_cron = database.clone();
    let _ = sched_db.add(
        Job::new("0/2 * * * * *", {
            move |_uuid, _lock| {
                database_for_cron.get_connections();
            }
        }).unwrap()
    ).await;
    sched_db.start().await.unwrap();

    let sched = JobScheduler::new().await.unwrap();
    let cache_for_cron = cache.clone();
    let tracking_v1_for_cron = tracking_v1.clone();
    let _ = sched.add(
        // Note:
        // collecting interval should equal or larger than performance interval
        // postpone should less than collecting interval
        Job::new("15 0/15 * * * *", {
            move |_uuid, _lock| {
                let mut utc: DateTime<Utc> = Utc::now();
                let minute_aligned = utc.minute() / GLOBAL_CONFIG.get().unwrap().performance_interval * GLOBAL_CONFIG.get().unwrap().performance_interval;
                utc = utc.with_minute(minute_aligned).unwrap();

                let from = utc.checked_add_signed(Duration::minutes(-15)).unwrap();
                let from_str = from.format("%Y%m%d%H%M").to_string();

                let to = utc.checked_add_signed(Duration::minutes(-1)).unwrap();
                let to_str = to.format("%Y%m%d%H%M").to_string();

                TrackingV1::collect(&tracking_v1_for_cron, cache_for_cron.clone(), from_str, to_str);
            }
        }).unwrap()
    ).await;
    sched.start().await.unwrap();

    let comression_layer: CompressionLayer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);
    let decomression_layer: RequestDecompressionLayer = RequestDecompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);

    let app = Router::new()
        .route("/v1/{event_connection}/{request_id}", get(TrackingV1::handler))
        .route("/amend/v1/{from}/{to}", get(TrackingV1::amend))
        .layer(comression_layer)
        .layer(decomression_layer)
        .with_state((tracking_v1, database, cache));

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", GLOBAL_CONFIG.get().unwrap().listen_address, GLOBAL_CONFIG.get().unwrap().listen_port))
        .await
        .unwrap();

    axum::serve(listener, app)
        .await
        .unwrap();
}
