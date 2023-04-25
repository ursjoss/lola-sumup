use std::fmt::{Debug, Formatter};
use std::{env, error::Error, ffi::OsString, fmt, io, process};

use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

/*
* TODO
* - Derive topic
* - write header
* - remove Refunds
# - aggregate per day
*/

#[derive(Debug, Deserialize, Serialize)]
enum RecordType {
    Sales,
    Refund,
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
enum PaymentMethod {
    Cash,
    Card,
}

impl fmt::Display for PaymentMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
enum Topic {
    LoLa,
    MiTi,
    Vermietung,
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum Purpose {
    Consumption,
    Tip,
}

impl fmt::Display for Purpose {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RecordIn {
    account: String,
    #[serde(with = "parse_date")]
    date: NaiveDate,
    #[serde(with = "parse_time")]
    time: NaiveTime,
    #[serde(rename = "Type")]
    record_type: RecordType,
    #[serde(rename = "Transaction ID")]
    transaction_id: String,
    #[serde(rename = "Receipt Number")]
    receipt_number: String,
    #[serde(rename = "Payment Method")]
    payment_method: Option<PaymentMethod>,
    quantity: Option<u64>,
    description: String,
    currency: String,
    #[serde(rename = "Price (Gross)")]
    price_gross: f64,
    #[serde(rename = "Price (Net)")]
    price_net: Option<f64>,
    tax: Option<f64>,
    #[serde(rename = "Tax rate")]
    tax_rate: Option<String>,
    #[serde(rename = "Transaction refunded")]
    transaction_refunded: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct RecordOut {
    account: String,
    #[serde(with = "parse_date")]
    date: NaiveDate,
    #[serde(with = "parse_time")]
    time: NaiveTime,
    #[serde(rename = "Type")]
    record_type: RecordType,
    #[serde(rename = "Transaction ID")]
    transaction_id: String,
    #[serde(rename = "Receipt Number")]
    receipt_number: String,
    #[serde(rename = "Payment Method")]
    payment_method: Option<PaymentMethod>,
    quantity: Option<u64>,
    description: String,
    currency: String,
    #[serde(rename = "Price (Gross)")]
    price_gross: f64,
    #[serde(rename = "Price (Net)")]
    price_net: Option<f64>,
    tax: Option<f64>,
    #[serde(rename = "Tax rate")]
    tax_rate: Option<String>,
    #[serde(rename = "Transaction refunded")]
    transaction_refunded: String,
    topic: Topic,
    purpose: Purpose,
    comment: String,
}

impl From<RecordIn> for RecordOut {
    fn from(ri: RecordIn) -> Self {
        RecordOut {
            account: ri.account,
            date: ri.date,
            time: ri.time,
            record_type: ri.record_type,
            transaction_id: ri.transaction_id,
            receipt_number: ri.receipt_number,
            payment_method: ri.payment_method,
            quantity: ri.quantity,
            description: ri.description.trim().to_string(),
            currency: ri.currency,
            price_gross: ri.price_gross,
            price_net: ri.price_net,
            tax: ri.tax,
            tax_rate: ri.tax_rate,
            transaction_refunded: ri.transaction_refunded,
            topic: infer_topic(ri.time),
            purpose: infer_purpose(ri.description.trim().to_string()),
            comment: "".into(),
        }
    }
}

fn infer_topic(time: NaiveTime) -> Topic {
    let th1 = NaiveTime::from_hms_opt(15, 0, 0).unwrap();
    let th2 = NaiveTime::from_hms_opt(18, 0, 0).unwrap();
    if time < th1 {
        Topic::MiTi
    } else if time > th2 {
        Topic::Vermietung
    } else {
        Topic::LoLa
    }
}

fn infer_purpose(description: String) -> Purpose {
    if description == "Trinkgeld" {
        Purpose::Tip
    } else {
        Purpose::Consumption
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    // let query = match env::args().nth(2) {
    //     None => return Err(From::from("expected 2 arguments, but got less")),
    //     Some(query) => query,
    // };
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    let mut wtr = csv::Writer::from_writer(io::stdout());

    // wtr.write_record(rdr.headers()?)?;

    for result in rdr.deserialize() {
        let record_in: RecordIn = result?;
        let record_out: RecordOut = From::from(record_in);
        wtr.serialize(&record_out)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

mod parse_date {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format("%d.%m.%Y"));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, "%d.%m.%y").map_err(serde::de::Error::custom)
    }
}

mod parse_time {
    use chrono::NaiveTime;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format("%H:%M:%S"));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveTime::parse_from_str(&s, "%H:%M").map_err(serde::de::Error::custom)
    }
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test() {
        let csv = "\
Account,Date,Time,Type,Transaction ID,Receipt Number,Payment Method,Quantity,Description,Currency,Price (Gross),Price (Net),Tax,Tax rate,Transaction refunded
test@org.org,15.03.23,11:53,Sales,TD4KP497FR,S20230000001,Cash,2,Kaffee ,CHF,7.00,7.00,0.00,0%,
";
        let mut reader = csv::Reader::from_reader(csv.as_bytes());
        let ri: RecordIn = reader.deserialize().next().unwrap().unwrap();
        let ro: RecordOut = From::from(ri);

        assert_eq!("test@org.org", ro.account);
        assert_eq!("15.03.2023", format!("{}", ro.date.format("%d.%m.%Y")));
        assert_eq!("11:53:00", format!("{}", ro.time.format("%H:%M:%S")));
        assert_eq!("Sales", ro.record_type.to_string());
        assert_eq!("TD4KP497FR", ro.transaction_id);
        assert_eq!("S20230000001", ro.receipt_number);
        assert_eq!("Cash", ro.payment_method.unwrap().to_string());
        assert_eq!(2, ro.quantity.unwrap());
        assert_eq!("Kaffee", ro.description);
        assert_eq!("CHF", ro.currency);
        assert_eq!(7.00, ro.price_gross);
        assert_eq!(7.00, ro.price_net.unwrap());
        assert_eq!(0.00, ro.tax.unwrap());
        assert_eq!("0%", ro.tax_rate.unwrap());
        assert_eq!("", ro.transaction_refunded);
        assert_eq!("MiTi", ro.topic.to_string());
        assert_eq!("Consumption", ro.purpose.to_string());
        assert_eq!("", ro.comment);
    }
}
