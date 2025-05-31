use std::sync::Arc;

use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use r2d2::Pool;
use redis::Client;

use crate::{entity::{AF_PERIOD_DAY, AF_PERIOD_HOUR, AF_PERIOD_MINUTE, AF_PERIOD_SECOND, TC_INDICATOR_COST, TC_INDICATOR_REQUEST, TC_PERIOD_DAY, TC_PERIOD_HOUR, TC_PERIOD_MINUTE, TC_PERIOD_SECOND}, EnvConfig, GLOBAL_CONFIG};

#[derive(Clone)]
pub struct Cache {
    pub pw: Pool<Client>, // performance (write)
    pub nw: Pool<Client>, // notification bundle, url & cost (write)
    pub nr: Pool<Client>, // notification bundle, url & cost (read)
    pub tcw: Pool<Client>, // traffic control (write)
    pub afw: Pool<Client>, // anti fraud (write)
}

impl Cache {

    pub fn new(config: &EnvConfig) -> Self {
        Self {
            pw: Pool::builder().build(redis::Client::open(config.performance_connection_write.clone()).unwrap()).unwrap(),
            nw: Pool::builder().build(redis::Client::open(config.notification_connection_write.clone()).unwrap()).unwrap(),
            nr: Pool::builder().build(redis::Client::open(config.notification_connection_read.clone()).unwrap()).unwrap(),
            tcw: Pool::builder().build(redis::Client::open(config.trafficcontrol_connection_write.clone()).unwrap()).unwrap(),
            afw: Pool::builder().build(redis::Client::open(config.antifraud_connection_write.clone()).unwrap()).unwrap(),
        }
    }

    // performance

    pub fn update_cost(&self, client_port: i32, vendor_port: i32, bundle: &String, income: i32, outcome_upstream: f64, outcome_rebate: f64, outcome_downstream: f64) {
        let cache = self.clone();
        let bundle = Arc::new(bundle.to_string());
        tokio::spawn({
            async move {
                cache.update_cost_async(client_port, vendor_port, &bundle, income, outcome_upstream, outcome_rebate, outcome_downstream).await;
            }
        });
    }

    async fn update_cost_async(&self, client_port: i32, vendor_port: i32, bundle: &String, income: i32, outcome_upstream: f64, outcome_rebate: f64, outcome_downstream: f64) {
        let utc: DateTime<Utc> = Utc::now();
        let hour = utc.hour();
        let minute_aligned = utc.minute() / GLOBAL_CONFIG.get().unwrap().performance_interval * GLOBAL_CONFIG.get().unwrap().performance_interval;
        let minute_fragment = utc.minute() - minute_aligned;
        let second = utc.second();

        let key_income = format!("CI{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
        let key_outcome_upstream = format!("CU{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
        let key_outcome_rebate = format!("CR{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
        let key_outcome_downstream = format!("CD{:0>2}{:0>2}:{}:{}:{}", hour, minute_aligned, client_port, vendor_port, bundle.replace(":", "_"));
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
                };
                let result = redis::cmd("INCRBYFLOAT").arg(&key_outcome_upstream).arg(&outcome_upstream).query::<Option<f64>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == outcome_upstream {
                                    let _ = redis::cmd("EXPIRE").arg(&key_outcome_upstream).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                };
                let result = redis::cmd("INCRBYFLOAT").arg(&key_outcome_rebate).arg(&outcome_rebate).query::<Option<f64>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == outcome_rebate {
                                    let _ = redis::cmd("EXPIRE").arg(&key_outcome_rebate).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                };
                let result = redis::cmd("INCRBYFLOAT").arg(&key_outcome_downstream).arg(&outcome_downstream).query::<Option<f64>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == outcome_downstream {
                                    let _ = redis::cmd("EXPIRE").arg(&key_outcome_downstream).arg(expire).query::<Option<u32>>(&mut connection);
                                }
                            },
                            None => (),
                        }
                    },
                    Err(_) => (),
                };
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

    // traffic control & anti fraud

    pub fn set_traffic_control_amount(&self, time: DateTime<FixedOffset>, client_port: i32, vendor_port: i32, bundle: &String, indicator: i32, period: i32, amount: i64) {
        let cache = self.clone();
        let bundle = Arc::new(bundle.to_string());
        tokio::spawn({
            async move {
                cache.set_traffic_control_amount_async(time, client_port, vendor_port, &bundle, indicator, period, amount).await;
            }
        });
    }

    async fn set_traffic_control_amount_async(&self, time: DateTime<FixedOffset>, client_port: i32, vendor_port: i32, bundle: &String, indicator: i32, period: i32, amount: i64) {
        let day = time.day();
        let hour = time.hour();
        let minute = time.minute();

        let mut indicator_code = "";
        if indicator == TC_INDICATOR_REQUEST {
            indicator_code = "R";
        }
        if indicator == TC_INDICATOR_COST {
            indicator_code = "C";
        }

        let mut key = "".to_string();
        let mut expire = 0;
        if period == TC_PERIOD_DAY {
            key = format!("Q{}D{:0>2}:{}:{}:{}", indicator_code, day, client_port, vendor_port, bundle.replace(":", "_"));
            expire = 86400 + 60;
        }
        if period == TC_PERIOD_HOUR {
            key = format!("Q{}H{:0>2}:{}:{}:{}", indicator_code, hour, client_port, vendor_port, bundle.replace(":", "_"));
            expire = 3600 + 60;
        }
        if period == TC_PERIOD_MINUTE {
            key = format!("Q{}M{:0>2}{:0>2}:{}:{}:{}", indicator_code, hour, minute, client_port, vendor_port, bundle.replace(":", "_"));
            expire = 60 + 60;
        }
        if period == TC_PERIOD_SECOND {
            key = format!("Q{}S{:0>2}{:0>2}:{}:{}:{}", indicator_code, hour, minute, client_port, vendor_port, bundle.replace(":", "_"));
            expire = 60 + 60;
        }

        let connection = self.tcw.get();

        match connection {
            Ok(mut connection) => {
                let result = redis::cmd("INCRBY").arg(&key).arg(amount).query::<Option<i64>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == amount {
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

    pub fn set_anti_fraud_amount(&self, time: DateTime<FixedOffset>, client_port: i32, period: i32, code: &String, identifier: &String, amount: i64) {
        let cache = self.clone();
        let code = Arc::new(code.to_string());
        let identifier = Arc::new(identifier.to_string());
        tokio::spawn({
            async move {
                cache.set_anti_fraud_amount_async(time, client_port, period, &code, &identifier, amount).await;
            }
        });
    }

    async fn set_anti_fraud_amount_async(&self, time: DateTime<FixedOffset>, client_port: i32, period: i32, code: &String, identifier: &String, amount: i64) {
        let day = time.day();
        let hour = time.hour();
        let minute = time.minute();
        let second = time.second();

        let mut key = "".to_string();
        let mut expire = 0;
        if period == AF_PERIOD_DAY {
            key = format!("AD{:0>2}:{}:{}:{}", day, client_port, code, identifier);
            expire = 86400 + 60;
        }
        if period == AF_PERIOD_HOUR {
            key = format!("AH{:0>2}:{}:{}:{}", hour, client_port, code, identifier);
            expire = 3600 + 60;
        }
        if period == AF_PERIOD_MINUTE {
            key = format!("AM{:0>2}{:0>2}:{}:{}:{}", hour, minute, client_port, code, identifier);
            expire = 60 + 60;
        }
        if period == AF_PERIOD_SECOND {
            key = format!("AS{:0>2}{:0>2}{:0>2}:{}:{}:{}", hour, minute, second, client_port, code, identifier);
            expire = 60;
        }

        let connection = self.afw.get();

        match connection {
            Ok(mut connection) => {
                let result = redis::cmd("INCRBY").arg(&key).arg(amount).query::<Option<i64>>(&mut connection);
                match result {
                    Ok(result) => {
                        match result {
                            Some(result) => {
                                if result == amount {
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

    pub fn get_ids(&self, request_id: &String) -> Option<Vec<String>> {
        let connection = self.nr.get();

        match connection {
            Ok(mut connection) => {
                let key = format!("ids:{}", request_id);

                let result = redis::cmd("SMEMBERS").arg(&key).query::<Option<Vec<String>>>(&mut connection);
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
