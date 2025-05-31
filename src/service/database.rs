use std::{collections::HashMap, sync::{Arc, RwLock}};

use mysql::{*, prelude::*};

use crate::entity::{AntiFraud, TrafficControl};

#[derive(Clone)]
pub struct Database {
    pub conn_pool: Pool,

    // traffic control map: client_port + "|" + vendor_port + "|" + bundle => tc[]
    pub tcla: Arc<RwLock<HashMap<String, Vec<TrafficControl>>>>,

    // anti fraud map: client_port => af[]
    pub afla: Arc<RwLock<HashMap<String, Vec<AntiFraud>>>>,

}

impl Database {

    pub fn new(db_url: &str) -> Self {
        Self {
            conn_pool: Pool::new(db_url).unwrap(),
            tcla: Arc::new(RwLock::new(HashMap::<String, Vec<TrafficControl>>::new())),
            afla: Arc::new(RwLock::new(HashMap::<String, Vec<AntiFraud>>::new())),
        }
    }

    pub fn get_connections(&self) {
        // use query_map if there's no boolean type in result
        // otherwise, use query_iter

        let mut conn = match self.conn_pool.get_conn() {
            Ok(conn) => {
                conn
            },
            Err(_) => {
                return;
            }
        };

        let tcs = conn.query_map(
            "SELECT
                ad_traffic_control.client_port,
                ad_traffic_control.vendor_port,
                ad_traffic_control.bundle,
                ad_traffic_control.indicator,
                ad_traffic_control.period
            FROM ad_traffic_control;",
            | (client_port, vendor_port, bundle, indicator, period)
                : (i32, i32, String, i32, i32)
            | (client_port, vendor_port, bundle, indicator, period),
        ).unwrap();

        let tcll = self.tcla.clone();
        let mut tcl = tcll.write().unwrap();
        tcl.clear();
        for tc in tcs.iter() {
            let traffic_control = TrafficControl {
                client_port: tc.0,
                vendor_port: tc.1,
                bundle: tc.2.clone(),
                indicator: tc.3,
                period: tc.4,
            };
            let key = format!("{}|{}|{}", traffic_control.client_port, traffic_control.vendor_port, traffic_control.bundle);
            if !tcl.contains_key(&key) {
                tcl.insert(key.clone(), Vec::<TrafficControl>::new());
            }
            tcl.get_mut(&key).unwrap().push(traffic_control);
        }

        let afs = conn.query_map(
            "SELECT
                ad_anti_fraud.client_port,
                ad_anti_fraud.rule,
                ad_anti_fraud.period
            FROM ad_anti_fraud, ad_anti_fraud_rule
            WHERE ad_anti_fraud.rule = ad_anti_fraud_rule.code
            AND ad_anti_fraud_rule.enabled;",
            | (client_port, rule, period)
                : (i32, String, i32)
            | (client_port, rule, period),
        ).unwrap();

        let afll = self.afla.clone();
        let mut afl = afll.write().unwrap();
        afl.clear();
        for af in afs.iter() {
            let anti_fraud = AntiFraud {
                client_port: af.0,
                rule: af.1.clone(),
                period: af.2,
            };
            let key = format!("{}", anti_fraud.client_port);
            if !afl.contains_key(&key) {
                afl.insert(key.clone(), Vec::<AntiFraud>::new());
            }
            afl.get_mut(&key).unwrap().push(anti_fraud);
        }
    }

}
