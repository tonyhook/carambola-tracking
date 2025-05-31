pub const TC_INDICATOR_REQUEST: i32 = 1;
pub const TC_INDICATOR_COST:    i32 = 2;

pub const TC_PERIOD_SECOND:     i32 = 1;
pub const TC_PERIOD_MINUTE:     i32 = 2;
pub const TC_PERIOD_HOUR:       i32 = 3;
pub const TC_PERIOD_DAY:        i32 = 4;

#[derive(Clone)]
pub struct TrafficControl {
    pub client_port: i32,
    pub vendor_port: i32,
    pub bundle: String,
    pub indicator: i32,
    pub period: i32,
}
