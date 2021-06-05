use std::time::{Duration, SystemTime};

pub type DateTime = chrono::DateTime<chrono::Local>;

pub fn duration_to_datetime(d: Duration) -> DateTime {
    DateTime::from(SystemTime::UNIX_EPOCH + d)
}
