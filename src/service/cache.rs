use std::sync::{Arc, Mutex};

use redis::Client;

use crate::EnvConfig;

#[derive(Clone)]
pub struct Cache {
    pub pa: Arc<Mutex<Client>>,
}

impl Cache {

    pub fn new(config: &EnvConfig) -> Self {
        Self {
            pa: Arc::new(Mutex::new(redis::Client::open(config.performance_connection.clone()).unwrap())),
        }
    }

}
