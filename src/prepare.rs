use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, fmt, io};

use polars::datatypes::DataType;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars::io::{SerReader, SerWriter};
use polars::prelude::{
    col, lit, when, CsvReader, CsvWriter, Expr, IntoLazy, JoinType, LazyFrame, StrptimeOptions,
};
use serde::{Deserialize, Serialize};

/// Prepares the intermediate working copy of the original file.
/// Some derived fields are prepared in a best-effort approach.
/// The values of the intermediate file can (manually) be optionally tweaked until it matches the expectations.
pub fn prepare(
    input_path: &Path,
    commission_path: &Path,
    output_path: &Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let mut df = read_data(input_path, commission_path)?;
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .with_date_format(Some("%d.%m.%Y".into()))
        .with_time_format(Some("%H:%M:%S".into()))
        .finish(&mut df)?;
    Ok(())
}

fn read_data(input_path: &Path, commission_path: &Path) -> Result<DataFrame, Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b',')
        .finish()?;
    let commission_df = CsvReader::from_path(commission_path)?
        .has_header(true)
        .with_delimiter(b',')
        .finish()?;
    build_df(&raw_df, &commission_df)
}

fn build_df(
    raw_df: &DataFrame,
    raw_commission_df: &DataFrame,
) -> Result<DataFrame, Box<dyn Error>> {
    let raw_date_format = StrptimeOptions {
        format: Some("%d.%m.%y".into()),
        strict: true,
        exact: true,
        ..Default::default()
    };
    let time_format = StrptimeOptions {
        format: Some("%H:%M:%S".into()),
        strict: true,
        exact: true,
        ..Default::default()
    };

    let commission_df = raw_commission_df
        .clone()
        .lazy()
        .filter(
            col("Zahlungsart")
                .eq(lit("Umsatz"))
                .and(col("Status").eq(lit("Erfolgreich"))),
        )
        .select([
            col("Transaktions-ID"),
            col("Betrag inkl. MwSt.").alias("Commissioned Total"),
            col("Gebühr").alias("Commission"),
        ]);

    let (refunded_ids_1, refunded_ids_2) = refunded_id_dfs(raw_df);

    let df = raw_df
        .clone()
        .lazy()
        .with_column(
            col("Date")
                .str()
                .strptime(DataType::Date, raw_date_format)
                .alias("Date"),
        )
        .with_column(
            (col("Time") + lit(":00"))
                .str()
                .to_time(time_format.clone())
                .alias("Time"),
        )
        .with_column(col("Description").str().strip(None).alias("Description"))
        .with_column(infer_topic(time_format).alias("Topic"))
        .with_column(lit("").alias("Comment"))
        .join(
            refunded_ids_1,
            [col("Transaction ID")],
            [col("TransactionId")],
            JoinType::Left,
        )
        .join(
            refunded_ids_2,
            [col("Transaction refunded")],
            [col("TransactionId")],
            JoinType::Left,
        )
        .join(
            commission_df,
            [col("Transaction ID")],
            [col("Transaktions-ID")],
            JoinType::Left,
        )
        .with_column(
            (col("Price (Gross)") / col("Commissioned Total") * col("Commission"))
                .alias("Commission"),
        )
        .filter(col("refunded1").is_null().and(col("refunded2").is_null()))
        .select([
            col("Account"),
            col("Date"),
            col("Time"),
            col("Type"),
            col("Transaction ID"),
            col("Receipt Number"),
            col("Payment Method"),
            col("Quantity"),
            col("Description"),
            col("Currency"),
            col("Price (Gross)"),
            col("Price (Net)"),
            col("Tax"),
            col("Tax rate"),
            col("Transaction refunded"),
            col("Commission"),
            col("Topic"),
            infer_owner().alias("Owner"),
            infer_purpose().alias("Purpose"),
            col("Comment"),
        ])
        .collect()?;
    Ok(df)
}

fn refunded_id_dfs(raw_df: &DataFrame) -> (LazyFrame, LazyFrame) {
    let transaction_ids = raw_df.clone().lazy().select([col("Transaction ID")]);
    let ids_of_refunded = raw_df
        .clone()
        .lazy()
        .filter(col("Transaction refunded").is_not_null())
        .select([col("Transaction refunded")])
        .join(
            transaction_ids,
            [col("Transaction refunded")],
            [col("Transaction ID")],
            JoinType::Inner,
        )
        .select([col("Transaction refunded")])
        .unique(None, UniqueKeepStrategy::First);
    let refunded_ids_1 = ids_of_refunded
        .with_column(col("Transaction refunded").alias("TransactionId"))
        .with_column(lit("x").alias("refunded1"))
        .select([col("TransactionId"), col("refunded1")]);
    let refunded_ids_2 = refunded_ids_1
        .clone()
        .select([col("TransactionId"), col("refunded1").alias("refunded2")]);
    (refunded_ids_1, refunded_ids_2)
}

fn infer_topic(time_options: StrptimeOptions) -> Expr {
    when(col("Time").lt(lit("14:15:00").str().to_time(time_options.clone())))
        .then(lit(Topic::MiTi.to_string()))
        .when(col("Time").gt(lit("18:00:00").str().to_time(time_options)))
        .then(lit(Topic::Verm.to_string()))
        .otherwise(lit(Topic::LoLa.to_string()))
}

fn infer_owner() -> Expr {
    when(col("Topic").neq(lit(Topic::MiTi.to_string())))
        .then(lit(""))
        .when(
            col("Description")
                .str()
                .contains(lit("Hauptgang"), true)
                .or(col("Description").str().contains(lit("Kinderteller"), true))
                .or(col("Description").str().contains(lit("Menü"), true))
                .or(col("Description").str().contains(lit("Dessert"), true))
                .or(col("Description").str().contains(lit("Vorspeise"), true)),
        )
        .then(lit(Owner::MiTi.to_string()))
        .otherwise(lit(Owner::LoLa.to_string()))
}

fn infer_purpose() -> Expr {
    when(col("Description").str().contains(lit("Trinkgeld"), true))
        .then(lit(Purpose::Tip.to_string()))
        .otherwise(lit(Purpose::Consumption.to_string()))
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

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Owner {
    LoLa,
    MiTi,
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use polars::df;
    use polars::prelude::NamedFrom;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_collect_data() {
        let df = df!(
            "Account" => &["a@b.ch"],
            "Date" => &["17.04.23"],
            "Time" => &["12:32"],
            "Type" => &["Sales"],
            "Transaction ID" => &["TEGUCXAGDE"],
            "Receipt Number" => &["S20230000303"],
            "Payment Method" => &["Cash"],
            "Quantity" => &[1],
            "Description" => &["foo"],
            "Currency" => &["CHF"],
            "Price (Gross)" => &[16.0],
            "Price (Net)" => &[16.0],
            "Tax" => &["0.0%"],
            "Tax rate" => &[""],
            "Transaction refunded" => &[""],
        )
        .expect("Misconfigured test data frame");

        let raw_commission_df = df!(
            "Transaktions-ID" => &["TEGUCXAGDE"],
            "Zahlungsart" => &["Umsatz"],
            "Status" => &["Erfolgreich"],
            "Betrag inkl. MwSt." => &[16.0],
            "Gebühr" => &[0.24],
        )
        .expect("Misconfigured commission df");

        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let time = NaiveTime::parse_from_str("12:32:00", "%H:%M:%S").expect("valid time");
        let expected = df!(
            "Account" => &["a@b.ch"],
            "Date" => &[date],
            "Time" => &[time],
            "Type" => &["Sales"],
            "Transaction ID" => &["TEGUCXAGDE"],
            "Receipt Number" => &["S20230000303"],
            "Payment Method" => &["Cash"],
            "Quantity" => &[1],
            "Description" => &["foo"],
            "Currency" => &["CHF"],
            "Price (Gross)" => &[16.0],
            "Price (Net)" => &[16.0],
            "Tax" => &["0.0%"],
            "Tax rate" => &[""],
            "Transaction refunded" => &[""],
            "Commission" => &[0.24],
            "Topic" => &["MiTi"],
            "Owner" => &["LoLa"],
            "Purpose" => &["Consumption"],
            "Comment" => &[""],
        );

        let out = build_df(&df, &raw_commission_df).expect("should parse");

        assert_eq!(out, expected.expect("valid data frame"));
    }
}
