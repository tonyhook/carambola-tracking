use std::sync::Arc;

use chrono::{DateTime, Timelike, Utc};
use r2d2::Pool;
use redis::Client;

use crate::{EnvConfig, GLOBAL_CONFIG};

#[derive(Clone)]
pub struct Cache {
    pub pw: Pool<Client>, // performance (write)
    pub nw: Pool<Client>, // notification bundle, url & cost (write)
    pub nr: Pool<Client>, // notification bundle, url & cost (read)
}

impl Cache {

    pub fn new(config: &EnvConfig) -> Self {
        Self {
            pw: Pool::builder().build(redis::Client::open(config.performance_connection_write.clone()).unwrap()).unwrap(),
            nw: Pool::builder().build(redis::Client::open(config.notification_connection_write.clone()).unwrap()).unwrap(),
            nr: Pool::builder().build(redis::Client::open(config.notification_connection_read.clone()).unwrap()).unwrap(),
        }
    }

    // performance

    pub fn update_cost(&self, client_port: i32, vendor_port: i32, bundle: &String, income: i32, outcome: i32) {
        let cache = self.clone();
        let bundle = Arc::new(bundle.to_string());
        tokio::spawn({
            async move {
                cache.update_cost_async(client_port, vendor_port, &bundle, income, outcome).await;
            }
        });
    }

    async fn update_cost_async(&self, client_port: i32, vendor_port: i32, bundle: &String, income: i32, outcome: i32) {
        let utc: DateTime<Utc> = Utc::now();
        let hour = utc.hour();
        let minute_aligned = utc.minute() / GLOBAL_CONFIG.get().unwrap().performance_interval * GLOBAL_CONFIG.get().unwrap().performance_interval;
        let minute_fragment = utc.minute() - minute_aligned;
        let second = utc.second();

        let key_income = format!("CI{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
        let key_outcome = format!("CO{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
        let expire = 86400 - minute_fragment * 60 - second - GLOBAL_CONFIG.get().unwrap().performance_interval * 60;

        let connection = self.pw.get();

        match connection {
            Ok(mut connection) => {
                let result = redis::cmd("INCRBY").arg(&key_income).arg(&income).query::<Option<i32>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == income {
                                    let _ = redis::cmd("EXPIRE").arg(&key_income).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                }
                let result = redis::cmd("INCRBY").arg(&key_outcome).arg(&outcome).query::<Option<i32>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == outcome {
                                    let _ = redis::cmd("EXPIRE").arg(&key_outcome).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                }
            },
            Err(_) => (),
        }
    }

    pub fn set_tracking(&self, hour: u32, minute: u32, connection_id: u64, bundle: &String, event: u64, value: u32) {
        let cache = self.clone();
        let bundle = Arc::new(bundle.to_string());
        tokio::spawn({
            async move {
                cache.set_tracking_async(hour, minute, connection_id, &bundle, event, value).await;
            }
        });
    }

    async fn set_tracking_async(&self, hour: u32, minute: u32, connection_id: u64, bundle: &String, event: u64, value: u32) {
        let connection = self.pw.get();

        match connection {
            Ok(mut connection) => {
                let key = format!("T1{:0>2}{:0>2}:{}:{}:{}", hour, minute, connection_id, bundle.replace(":", "_"), event);
                let expire = 86400 - GLOBAL_CONFIG.get().unwrap().performance_interval * 60;

                let result = redis::cmd("SET").arg(&key).arg(value).query::<Option<String>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == "OK" {
                                    let _ = redis::cmd("EXPIRE").arg(&key).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                }
            },
            Err(_) => (),
        }
    }

    // notification bundle, url & cost

    pub fn get_bundle(&self, request_id: &String) -> Option<String> {
        let connection = self.nr.get();

        match connection {
            Ok(mut connection) => {
                let key = format!("bundle:{}", request_id);

                let result = redis::cmd("GET").arg(&key).query::<Option<String>>(&mut connection);
                match result {
                    Ok(result) => {
                        return result;
                    },
                    Err(_) => {
                        return None;
                    }
                }
            },
            Err(_) => {
                return None;
            },
        }
    }

    pub fn get_notification_cost(&self, request_id: &String) -> Option<String> {
        let connection = self.nw.get();

        match connection {
            Ok(mut connection) => {
                let key = format!("cost:{}", request_id);

                let result = redis::cmd("GETDEL").arg(&key).query::<Option<String>>(&mut connection);
                match result {
                    Ok(result) => {
                        return result;
                    },
                    Err(_) => {
                        return None;
                    }
                }
            },
            Err(_) => {
                return None;
            },
        }
    }

}
