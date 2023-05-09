use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, fmt, io};

use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

/// Prepares the intermediate working copy of the original file.
/// Some derived fields are prepared in a best-effort approach.
/// The values can optionally be tweaked until it matches the expectations.
pub fn prepare(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    let mut wtr = csv::WriterBuilder::new().delimiter(b';').from_writer(iowtr);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(input_path)?;

    let mut all_trx: Vec<Intermediate> = Vec::new();
    for result in rdr.deserialize() {
        let record_in: SalesReport = result?;
        all_trx.push(From::from(record_in));
    }

    let ids_of_refunded = get_ids_of_refunds(&all_trx);

    for trx in all_trx {
        if is_not_refunded(&ids_of_refunded, &trx) {
            wtr.serialize(&trx)?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn get_ids_of_refunds(records: &[Intermediate]) -> Vec<String> {
    records
        .iter()
        .map(|r| r.transaction_refunded.clone())
        .filter(|id| !id.is_empty())
        .filter(|id| contains_id(records, id))
        .collect()
}

fn contains_id(records: &[Intermediate], id: &String) -> bool {
    records
        .iter()
        .filter(|r| r.transaction_id == *id)
        .peekable()
        .peek()
        .is_some()
}

fn is_not_refunded(refunded: &[String], record_out: &Intermediate) -> bool {
    !refunded.contains(&record_out.transaction_id)
        && !refunded.contains(&record_out.transaction_refunded)
}

mod parse_date {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer, Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
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

    #[allow(clippy::trivially_copy_pass_by_ref)]
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

#[derive(Debug, Deserialize, Serialize, PartialEq)]
enum RecordType {
    Sales,
    Refund,
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum PaymentMethod {
    Cash,
    Card,
}

impl fmt::Display for PaymentMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Topic {
    LoLa,
    MiTi,
    Verm,
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Purpose {
    Consumption,
    Tip,
}

impl fmt::Display for Purpose {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The file format for the `Intermediate` file
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Intermediate {
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

/// The struct matching the Sumup `SalesReport` export,
/// e.g. 20230315-0000_20230331-2359-sales_report.csv
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SalesReport {
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

impl From<SalesReport> for Intermediate {
    fn from(ri: SalesReport) -> Self {
        Intermediate {
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
            purpose: infer_purpose(ri.description.trim()),
            comment: String::new(),
        }
    }
}

fn infer_topic(time: NaiveTime) -> Topic {
    let th1 = NaiveTime::from_hms_opt(14, 15, 0).unwrap();
    let th2 = NaiveTime::from_hms_opt(18, 0, 0).unwrap();
    if time < th1 {
        Topic::MiTi
    } else if time > th2 {
        Topic::Verm
    } else {
        Topic::LoLa
    }
}

fn infer_purpose(description: &str) -> Purpose {
    if description == "Trinkgeld" {
        Purpose::Tip
    } else {
        Purpose::Consumption
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use csv::Reader;
    use rstest::rstest;

    use crate::prepare::PaymentMethod::Cash;

    use super::*;

    #[rstest]
    #[case("09:15", "Kaffee ", Topic::MiTi, Purpose::Consumption)]
    #[case("11:53", "Tee", Topic::MiTi, Purpose::Consumption)]
    #[case("14:16", "Tee", Topic::LoLa, Purpose::Consumption)]
    #[case("18:01", "Bier", Topic::Verm, Purpose::Consumption)]
    #[case("22:59", "Trinkgeld", Topic::Verm, Purpose::Tip)]
    fn test(
        #[case] time: &str,
        #[case] description: &str,
        #[case] topic: Topic,
        #[case] purpose: Purpose,
    ) {
        let csv = new_csv(time, description);
        let expected = new_record(
            format!("{time}:00").as_str(),
            description.trim(),
            topic,
            purpose,
        );

        let mut reader = Reader::from_reader(csv.as_bytes());
        let ri: SalesReport = reader.deserialize().next().unwrap().unwrap();
        let ro: Intermediate = From::from(ri);

        assert_eq!(expected, ro);
    }

    fn new_csv(time: &str, description: &str) -> String {
        format!("\
Account,Date,Time,Type,Transaction ID,Receipt Number,Payment Method,Quantity,Description,Currency,Price (Gross),Price (Net),Tax,Tax rate,Transaction refunded
test@org.org,15.03.23,{time},Sales,TD4KP497FR,S20230000001,Cash,2,{description},CHF,7.00,7.00,0.00,0%,
")
    }

    fn new_record(time: &str, description: &str, topic: Topic, purpose: Purpose) -> Intermediate {
        Intermediate {
            account: "test@org.org".into(),
            date: NaiveDate::from_ymd_opt(2023, 3, 15).unwrap(),
            time: NaiveTime::parse_from_str(time, "%H:%M:%S").unwrap(),
            record_type: RecordType::Sales,
            transaction_id: "TD4KP497FR".into(),
            receipt_number: "S20230000001".into(),
            payment_method: Some(Cash),
            quantity: Some(2),
            description: description.into(),
            currency: "CHF".into(),
            price_gross: 7.0,
            price_net: Some(7.0),
            tax: Some(0.0),
            tax_rate: Some("0%".into()),
            transaction_refunded: String::new(),
            topic,
            purpose,
            comment: String::new(),
        }
    }
}
