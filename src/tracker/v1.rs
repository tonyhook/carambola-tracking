use std::{collections::{HashMap, HashSet}, io::Read, sync::Arc};

use axum::{extract::{Path, State}, http::StatusCode};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex};

use crate::{Cache, GLOBAL_CONFIG};

#[derive(Clone)]
pub struct TrackingV1 {
    pub state: Arc::<Mutex::<Option<(String, String, File)>>>,
}

impl TrackingV1 {

    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn handler(
        State((tracking_v1, cache)): State<(TrackingV1, Cache)>,
        Path((event_connection, request_id)): Path<(u64, u64)>)
    -> StatusCode {
        let utc: DateTime<Utc> = Utc::now();
        let date = utc.format("%Y%m%d").to_string();
        let time = utc.format("%H%M").to_string();

        let timestamp_report = utc.timestamp() as u64;
        let timestamp_issue = request_id >> 32;

        if timestamp_report.saturating_sub(timestamp_issue) > 86400 {
            return StatusCode::REQUEST_TIMEOUT;
        }

        let tracking1 = timestamp_report << 32 | event_connection;
        let tracking2 = request_id;

        {
            let mut current_state = tracking_v1.state.lock().await;

            let need_new = match &*current_state {
                Some((current_date, current_time, _)) => current_date != &date || current_time != &time,
                None => true,
            };

            if need_new {
                if let Some((_, _, file)) = current_state.as_mut() {
                    let _ = file.shutdown().await;
                }

                let dir = tokio::fs::create_dir_all(format!("{}/{}", GLOBAL_CONFIG.get().unwrap().storage_path, date)).await;

                match dir {
                    Ok(_) => {
                        let file = tokio::fs::OpenOptions::new()
                            .write(true)
                            .append(true)
                            .create(true)
                            .open(format!("{}/{}/{}", GLOBAL_CONFIG.get().unwrap().storage_path, date, time))
                            .await;

                        match file {
                            Ok(file) => {
                                *current_state = Some((date.clone(), time.clone(), file));
                            },
                            Err(_) => {
                                return StatusCode::INTERNAL_SERVER_ERROR;
                            }
                        }
                    },
                    Err(_) => {
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }

            let mut tmp_buffer = [0u8; 64];

            tmp_buffer[0..8].copy_from_slice(&tracking1.to_le_bytes());
            tmp_buffer[8..16].copy_from_slice(&tracking2.to_le_bytes());

            let bundle = cache.get_bundle(&tracking2.to_string());
            match bundle {
                Some(bundle) => {
                    let mut bundle_bytes = bundle.as_bytes();
                    let mut len = bundle_bytes.len();
                    if len > 48 {
                        bundle_bytes = &bundle_bytes[0..48];
                        len = 48;
                    }

                    tmp_buffer[16..(16+len)].copy_from_slice(&bundle_bytes);
                },
                None => (),
            }

            let (_, _, file) = current_state.as_mut().unwrap();
            let _ = file.write_all(&tmp_buffer).await;
        }

        let event = event_connection >> 22;
        if event == 501 {
            match cache.get_notification_cost(&format!("{}", request_id)) {
                Some(price) => {
                    let client_port = price.split(":").nth(0).unwrap().parse::<i32>().unwrap();
                    let vendor_port = price.split(":").nth(1).unwrap().parse::<i32>().unwrap();
                    let client_win_price = price.split(":").nth(2).unwrap().parse::<i32>().unwrap();
                    let vendor_win_price = price.split(":").nth(3).unwrap().parse::<i32>().unwrap();

                    cache.set_cost(client_port, vendor_port, client_win_price, vendor_win_price);
                },
                None => (),
            }
        }

        StatusCode::OK
    }

    pub async fn amend(
        State((tracking_v1, cache)): State<(TrackingV1, Cache)>,
        Path((from_str, to_str)): Path<(String, String)>)
    -> StatusCode {
        tracking_v1.collect(cache, from_str, to_str);
        StatusCode::OK
    }

    pub fn collect(&self, cache: Cache, from_str: String, to_str: String) {
        let from = NaiveDateTime::parse_from_str(&from_str, "%Y%m%d%H%M").unwrap();
        let mut time = from.checked_add_signed(Duration::days(-1)).unwrap();

        let mut map = HashMap::<String, u32>::new();
        let mut deduplicate = HashSet::<u128>::new();

        loop {
            let date = time.format("%Y%m%d").to_string();
            let minute = time.format("%H%M").to_string();

            let time_str = time.format("%Y%m%d%H%M").to_string();

            let file = std::fs::OpenOptions::new()
                .read(true)
                .open(format!("{}/{}/{}", GLOBAL_CONFIG.get().unwrap().storage_path, date, minute));

            match file {
                Ok(mut file) => {
                    loop {
                        let mut buffer = [0u8; 8];

                        match file.read_exact(&mut buffer) {
                            Ok(_) => (),
                            Err(_) => break,
                        }
                        let tracking1 = u64::from_le_bytes(buffer);

                        match file.read_exact(&mut buffer) {
                            Ok(_) => (),
                            Err(_) => break,
                        }
                        let tracking2 = u64::from_le_bytes(buffer);

                        let mut bundle_bytes = [0u8; 48];
                        let bundle: String;
                        match file.read_exact(&mut bundle_bytes) {
                            Ok(_) => (),
                            Err(_) => break,
                        }
                        let mut iter = bundle_bytes.split(|&byte| byte == 0);
                        match iter.next() {
                            Some(slice) => {
                                bundle = format!("{}", String::from_utf8_lossy(slice));
                            }
                            None => break,
                        }

                        let event = (tracking1 & 0x00000000ffffffff) >> 22;
                        let connection = tracking1 & 0x00000000003fffff;

                        let deduplicate_key: u128 = (tracking2 as u128) << 10 | event as u128;
                        if deduplicate.contains(&deduplicate_key) {
                            continue;
                        }
                        deduplicate.insert(deduplicate_key);

                        if time_str >= from_str && time_str.to_string() <= to_str {
                            let key = format!("T1{}:{}:{}:{}", minute, connection, bundle, event);

                            if map.contains_key(&key) {
                                let value = map.get_mut(&key).unwrap();
                                *value += 1;
                            } else {
                                map.insert(key, 1);
                            }
                        }
                    }
                },
                Err(_) => ()
            }

            if time_str == to_str {
                break;
            }

            time = time.checked_add_signed(Duration::minutes(1)).unwrap();
        }

        let connection = {
            let cl = cache.pa.clone();
            let rs_client = cl.lock().unwrap();
            rs_client.get()
        };

        match connection {
            Ok(mut connection) => {
                for (key, value) in map.iter() {
                    let _ = redis::cmd("SET").arg(key).arg(value).query::<Option<bool>>(&mut connection);
                }
            },
            Err(_) => (),
        }

    }

}
