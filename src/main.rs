use std::{env, error::Error, ffi::OsString, io, process};

use serde::{Deserialize, Serialize};

/*
* TODO
* - Typed date, time
* - Enum for type (Sales, Refund), payment method (Cash, Card)
* - Derive topic
* - write header
* - remove Refunds
# - aggregate per day
*/

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RecordIn {
    account: String,
    date: String,
    time: String,
    type_: String,
    #[serde(rename = "Transaction ID")]
    transaction_id: String,
    #[serde(rename = "Receipt Number")]
    receipt_number: String,
    #[serde(rename = "Payment Method")]
    payment_method: String,
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
    date: String,
    time: String,
    type_: String,
    #[serde(rename = "Transaction ID")]
    transaction_id: String,
    #[serde(rename = "Receipt Number")]
    receipt_number: String,
    #[serde(rename = "Payment Method")]
    payment_method: String,
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
    topic: String,
}

impl From<RecordIn> for RecordOut {
    fn from(ri: RecordIn) -> Self {
        RecordOut {
            account: ri.account,
            date: ri.date,
            time: ri.time + ":00",
            type_: ri.type_,
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
            topic: From::from("TODO"),
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let query = match env::args().nth(2) {
        None => return Err(From::from("expected 2 arguments, but got less")),
        Some(query) => query,
    };
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    let mut wtr = csv::Writer::from_writer(io::stdout());

    // wtr.write_record(rdr.headers()?)?;

    for result in rdr.deserialize() {
        let record_in: RecordIn = result?;
        let record_out: RecordOut = From::from(record_in);
        if record_out.payment_method == query {
            wtr.serialize(&record_out)?;
        }
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

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}
