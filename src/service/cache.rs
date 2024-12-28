use std::sync::{Arc, Mutex};

use chrono::{DateTime, Timelike, Utc};
use r2d2::Pool;
use redis::Client;

use crate::{EnvConfig, GLOBAL_CONFIG};

#[derive(Clone)]
pub struct Cache {
    pub pa: Arc<Mutex<Pool<Client>>>, // performance
    pub na: Arc<Mutex<Pool<Client>>>, // notification url & cost
}

impl Cache {

    pub fn new(config: &EnvConfig) -> Self {
        Self {
            pa: Arc::new(Mutex::new(Pool::builder().build(redis::Client::open(config.performance_connection.clone()).unwrap()).unwrap())),
            na: Arc::new(Mutex::new(Pool::builder().build(redis::Client::open(config.notification_connection.clone()).unwrap()).unwrap())),
        }
    }

    pub fn get_notification_cost(&self, request_id: &String) -> Option<String> {
        let connection = {
            let cl = self.na.clone();
            let rs_client = cl.lock().unwrap();
            rs_client.get()
        };

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

    pub fn set_cost(&self, client_port: i32, vendor_port: i32, income: i32, outcome: i32) {
        let cache = self.clone();
        tokio::spawn({
            async move {
                cache.set_cost_async(client_port, vendor_port, income, outcome).await;
            }
        });
    }

    async fn set_cost_async(&self, client_port: i32, vendor_port: i32, income: i32, outcome: i32) {
        let utc: DateTime<Utc> = Utc::now();
        let hour = utc.hour();
        let minute_aligned = utc.minute() / GLOBAL_CONFIG.get().unwrap().performance_interval * GLOBAL_CONFIG.get().unwrap().performance_interval;
        let minute_fragment = utc.minute() - minute_aligned;
        let second = utc.second();

        let key_income = format!("CI{:0>2}{:0>2}:{}:{}", hour, minute_aligned, client_port, vendor_port);
        let key_outcome = format!("CO{:0>2}{:0>2}:{}:{}", hour, minute_aligned, client_port, vendor_port);
        let expire = 86400 - minute_fragment * 60 - second - GLOBAL_CONFIG.get().unwrap().performance_interval * 60;

        let connection = {
            let cl = self.pa.clone();
            let rs_client = cl.lock().unwrap();
            rs_client.get()
        };

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

}
