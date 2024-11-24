use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::export::constraint::{
    validate_owners, validate_payment_methods, validate_purposes, validate_topics,
    validation_topic_owner,
};
use crate::export::export_accounting::{gather_df_accounting, validate_acc_constraint};
use crate::export::export_miti::gather_df_miti;
use crate::export::export_summary::collect_data;
use crate::prepare::warn_on_zero_value_trx;

mod constraint;
mod export_accounting;
mod export_miti;
mod export_summary;

/// Reads the intermediate files and exports all configured reports.
pub fn export(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let parse_option = CsvParseOptions::default()
        .with_separator(b';')
        .with_try_parse_dates(true);
    let raw_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(parse_option)
        .try_into_reader_with_file_path(Some(input_path.into()))?
        .finish()?;

    warn_on_zero_value_trx(&raw_df)?;

    let (mut df, mut df_acc) = crunch_data(raw_df)?;

    export_summary(&month, &ts, &mut df)?;
    export_mittagstisch(&month, &ts, &mut df)?;
    export_accounting(&month, &ts, &mut df_acc)
}

/// returns two dataframes, one for summary/miti, the other for the accounting export.
fn crunch_data(raw_df: DataFrame) -> Result<(DataFrame, DataFrame), Box<dyn Error>> {
    validate(&raw_df)?;

    let mut df = collect_data(raw_df)?;
    df.extend(&df.clone().lazy().sum().collect()?)?;

    let df_acc = gather_df_accounting(&df)?;
    validate_acc_constraint(&df_acc)?;
    Ok((df, df_acc))
}

fn validate(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    let validated_columns = [
        col("Row-No"),
        col("Date"),
        col("Time"),
        col("Transaction ID"),
        col("Payment Method"),
        col("Description"),
        col("Price (Gross)"),
        col("Topic"),
        col("Owner"),
        col("Purpose"),
        col("Comment"),
    ];

    validate_payment_methods(raw_df, &validated_columns)?;
    validate_topics(raw_df, &validated_columns)?;
    validate_owners(raw_df, &validated_columns)?;
    validate_purposes(raw_df, &validated_columns)?;
    validation_topic_owner(raw_df, &validated_columns)?;
    Ok(())
}

fn export_summary(month: &&str, ts: &&str, df: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    write_to_file(df, &path_with_prefix("summary", month, ts))?;
    Ok(())
}

fn export_mittagstisch(month: &&str, ts: &&str, df: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    write_to_file(
        &mut gather_df_miti(df)?,
        &path_with_prefix("mittagstisch", month, ts),
    )?;
    Ok(())
}

fn export_accounting(
    month: &&str,
    ts: &&str,
    df_acc: &mut DataFrame,
) -> Result<(), Box<dyn Error>> {
    write_to_file(df_acc, &path_with_prefix("accounting", month, ts))
}

/// Constructs a path for a CSV file from `prefix`, `month` and `ts` (timestamp).
fn path_with_prefix(prefix: &str, month: &str, ts: &str) -> PathBuf {
    PathBuf::from(format!("{prefix}_{month}_{ts}.csv"))
}

/// Writes the dataframe `df` to the file system into path `path`.
fn write_to_file(df: &mut DataFrame, path: &dyn AsRef<Path>) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = Box::new(File::create(path)?);
    CsvWriter::new(iowtr)
        .include_header(true)
        .with_separator(b';')
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::configure_the_environment;
    use pretty_assertions::assert_ne;
    use rstest::rstest;

    use crate::test_fixtures::{intermediate_df_02, intermediate_df_04, summary_df_04};

    use super::*;

    #[rstest]
    fn can_crunch_data_without_panic(intermediate_df_02: DataFrame) {
        let (df1, df2) = crunch_data(intermediate_df_02).expect("should crunch");

        assert_ne!(df1.shape().0, 0, "df1 does not contain records");
        assert_ne!(df2.shape().0, 0, "df2 does not contain records");
    }

    #[rstest]
    fn can_calculate_summary_row(intermediate_df_04: DataFrame, summary_df_04: DataFrame) {
        configure_the_environment();
        let (df1, _) = crunch_data(intermediate_df_04).expect("should crunch");
        assert_eq!(
            df1.shape().0,
            4,
            "df1 does not contain 3 records and 1 summary line"
        );
        assert_eq!(
            df1, summary_df_04,
            "unexpected summary for intermediate_df_04"
        );
    }
}
