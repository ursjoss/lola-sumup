use std::{env, error::Error, ffi::OsString, process};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Record {
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

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    for result in rdr.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }
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
