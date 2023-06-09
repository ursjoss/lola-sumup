use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::export::constraint::validation_topic_owner;
use crate::export::export_accounting::{gather_df_accounting, validate_acc_constraint};
use crate::export::export_miti::gather_df_miti;
use crate::export::export_summary::collect_data;

mod constraint;
mod export_accounting;
mod export_miti;
mod export_summary;

/// Reads the intermediate files and exports all configured reports.
pub fn export(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;

    let (mut df, mut df_acc) = crunch_data(raw_df)?;

    write_to_file(&mut df, &path_with_prefix("summary", month, ts))?;
    write_to_file(
        &mut gather_df_miti(&df)?,
        &path_with_prefix("mittagstisch", month, ts),
    )?;
    write_to_file(&mut df_acc, &path_with_prefix("accounting", month, ts))
}

/// returns two dataframes, one for summary/miti, the other for the accounting export.
fn crunch_data(raw_df: DataFrame) -> Result<(DataFrame, DataFrame), Box<dyn Error>> {
    validation_topic_owner(&raw_df)?;

    let mut df = collect_data(raw_df)?;
    print!("{df}");
    df.extend(&df.sum())?;
    print!("{df}");

    let df_acc = gather_df_accounting(&df)?;
    validate_acc_constraint(&df_acc)?;
    Ok((df, df_acc))
}

/// Constructs a path for a CSV file from `prefix`, `month` and `ts` (timestamp).
fn path_with_prefix(prefix: &str, month: &str, ts: &str) -> PathBuf {
    PathBuf::from(format!("{prefix}_{month}_{ts}.csv"))
}

/// Writes the dataframe `df` to the file system into path `path`.
fn write_to_file(df: &mut DataFrame, path: &dyn AsRef<Path>) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = Box::new(File::create(path)?);
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use polars::df;
    use polars::prelude::{AnyValue, NamedFrom};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn can_crunch_data_without_panic() {
        let raw_df = df!(
            "Account" => &["a@b.ch", "a@b.ch"],
            "Date" => &["17.4.2023", "17.4.2023"],
            "Time" => &["12:32:00", "12:32:00"],
            "Type" => &["Sales", "Sales"],
            "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDE"],
            "Receipt Number" => &["S20230000303", "S20230000303"],
            "Payment Method" => &["Card", "Card"],
            "Quantity" => &[1_i64, 1_i64],
            "Description" => &["foo", "Trinkgeld"],
            "Currency" => &["CHF", "CHF"],
            "Price (Gross)" => &[16.0, 1.0],
            "Price (Net)" => &[16.0, 1.0],
            "Tax" => &[0.0, 0.0],
            "Tax rate" => &["", ""],
            "Transaction refunded" => &["", ""],
            "Commission" => &[0.2259, 0.0141],
            "Topic" => &["MiTi", "MiTi"],
            "Owner" => &["LoLa", "MiTi"],
            "Purpose" => &["Consumption", "Tip"],
            "Comment" => &[AnyValue::Null, AnyValue::Null],
        )
        .expect("valid dataframe");

        let (df1, df2) = crunch_data(raw_df).expect("should crunch");

        assert_ne!(df1.shape().0, 0, "df1 does not contain records");
        assert_ne!(df2.shape().0, 0, "df2 does not contain records");
    }
}
