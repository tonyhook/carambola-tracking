pub const AF_PERIOD_SECOND: i32 = 1;
pub const AF_PERIOD_MINUTE: i32 = 2;
pub const AF_PERIOD_HOUR:   i32 = 3;
pub const AF_PERIOD_DAY:    i32 = 4;

#[derive(Clone)]
pub struct AntiFraud {
    pub client_port: i32,
    pub rule: String,
    pub period: i32,
}
