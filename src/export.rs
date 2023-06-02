use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::export::export_accounting::gather_df_accounting;
use crate::export::export_miti::gather_df_miti;
use crate::export::export_summary::collect_data;
use crate::prepare::Topic;

mod export_accounting;
mod export_miti;
mod export_summary;

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;
    validate_topic_owner_constraint(&raw_df)?;

    let mut df = collect_data(raw_df)?;
    df.extend(&df.sum())?;

    write_summary(output_path, &mut df)?;
    write_additional_file(&mut gather_df_miti(&df)?, "miti.csv")?;
    write_additional_file(&mut gather_df_accounting(&df)?, "accounting.csv")
}

fn validate_topic_owner_constraint(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::MiTiWithoutOwner,
        col("Topic")
            .eq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).eq(lit(""))),
    )?;
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::LoLaWithOwner,
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).neq(lit(""))),
    )?;
    Ok(())
}

enum TopicOwnerConstraintViolationError {
    MiTiWithoutOwner(DataFrame),
    LoLaWithOwner(DataFrame),
}

impl Display for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicOwnerConstraintViolationError::MiTiWithoutOwner(df) => {
                write!(f, "Row with topic 'MiTi' must have an Owner! {df}")
            }
            TopicOwnerConstraintViolationError::LoLaWithOwner(df) => {
                write!(
                    f,
                    "Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner! {df}"
                )
            }
        }
    }
}

impl Debug for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}:?")
    }
}

impl Error for TopicOwnerConstraintViolationError {}

fn topic_owner_constraint(
    raw_df: &DataFrame,
    error: fn(DataFrame) -> TopicOwnerConstraintViolationError,
    predicate: Expr,
) -> Result<(), Box<dyn Error>> {
    let df = raw_df
        .clone()
        .lazy()
        .with_row_count("Row-No", Some(2))
        .filter(predicate)
        .select([
            col("Row-No"),
            col("Date"),
            col("Time"),
            col("Transaction ID"),
            col("Description"),
            col("Price (Gross)"),
            col("Topic"),
            col("Owner"),
            col("Purpose"),
            col("Comment"),
        ])
        .collect()?;
    if df.shape().0 > 0 {
        Err(Box::try_from(error(df)).unwrap())
    } else {
        Ok(())
    }
}

fn write_summary(output_path: &Option<PathBuf>, df: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(df)?;
    Ok(())
}

fn write_additional_file(df: &mut DataFrame, file_name: &str) -> Result<(), Box<dyn Error>> {
    let output_path = Some(PathBuf::from(file_name));
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    // MiTi must have an owner
    #[case(Topic::MiTi, Some("LoLa"), true, None)]
    #[case(Topic::MiTi, Some("MiTi"), true, None)]
    // LoLa (Cafe/Verm) must have no owner
    #[case(Topic::Cafe, Some(""), true, None)]
    #[case(Topic::Verm, Some(""), true, None)]
    #[case(Topic::Verm, None, true, None)]
    // MiTi w/o or non-MiTi with owner should fail
    #[case(
        Topic::MiTi,
        Some(""),
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::MiTi,
        None,
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::Cafe,
        Some("Cafe"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
    )]
    #[case(
        Topic::Verm,
        Some("MiTi"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
    )]
    fn test_constraints(
        #[case] topic: Topic,
        #[case] owner: Option<&str>,
        #[case] expected_valid: bool,
        #[case] error_msg: Option<&str>,
    ) {
        let df = df!(
            "Account" => &["a@b.ch"],
            "Date" => &["17.04.2023"],
            "Time" => &["12:32:00"],
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
            "Topic" => &[topic.to_string()],
            "Owner" => &[owner],
            "Purpose" => &["Consumption"],
            "Comment" => &[None::<String>],
        )
        .expect("single record df");
        match validate_topic_owner_constraint(&df) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                let msg = error_msg.expect("should have an error message");
                assert!(e.to_string().starts_with(msg));
            }
        }
    }
}
