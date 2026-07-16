pub mod metadata;

use std::str::FromStr;

use sqlx::types::BigDecimal;

// pub fn convert_chart_timestamp(timestamp: i64, interval: &str) -> i64 {
//     let total_minutes = timestamp / 60;

//     let rounded_minutes = match interval {
//         "1" => total_minutes,
//         "5" => (total_minutes / 5) * 5,
//         "15" => (total_minutes / 15) * 15,
//         "30" => (total_minutes / 30) * 30,
//         "1H" => (total_minutes / 60) * 60,
//         "4H" => (total_minutes / 240) * 240,
//         "D" => (total_minutes / 1440) * 1440,
//         "W" => {
//             let dt = chrono::DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap();
//             let weekday = dt.weekday().num_days_from_monday();
//             let monday = dt.date_naive() - chrono::Duration::days(weekday as i64);
//             monday.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() / 60
//         }
//         "M" => {
//             let dt = chrono::DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap();
//             let first_of_month = dt.date_naive().with_day(1).unwrap();
//             first_of_month.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() / 60
//         }
//         _ => total_minutes, // 기본적으로 1분 단위로 처리
//     };

//     rounded_minutes * 60
// }

pub fn to_big_decimal<T: ToString>(value: T) -> BigDecimal {
    BigDecimal::from_str(&value.to_string()).unwrap_or_default()
}
