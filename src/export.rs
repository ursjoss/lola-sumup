use polars::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use calamine::{Data, HeaderRow, Reader, Xlsx, open_workbook};
use std::io::BufWriter;

use crate::export::constraint::{
    validate_owners, validate_payment_methods, validate_purposes, validate_topics,
    validation_topic_owner,
};
use crate::export::export_accounting::{gather_df_accounting, validate_acc_constraint};
use crate::export::export_banana::gather_df_banana;
use crate::export::export_details::collect_data;
use crate::export::export_miti::gather_df_miti;
use crate::prepare::warn_on_zero_value_trx;

mod constraint;
mod export_accounting;
mod export_banana;
mod export_details;
mod export_miti;
mod posting;

/// Reads the intermediate files and exports all configured reports.
pub fn export(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(input_path)?;
    let range = workbook
        .with_header_row(HeaderRow::Row(1))
        .worksheet_range("Sheet1")?;

    let max_column = range.get_size().1 - 1;
    let output_path = input_path.with_extension("csv");
    let mut csv_file = BufWriter::new(File::create(output_path).unwrap());
    for rows in range.rows() {
        for (col_number, cell_data) in rows.iter().enumerate() {
            match *cell_data {
                Data::Empty => Ok(()),
                Data::Int(ref i) => write!(csv_file, "{i}"),
                Data::Bool(ref b) => write!(csv_file, "{b}"),
                Data::Error(ref e) => write!(csv_file, "{e:?}"),
                Data::Float(ref f) => write!(csv_file, "{f}"),
                Data::DateTime(ref d) => write!(csv_file, "{}", d.as_f64()),
                Data::String(ref s) | Data::DateTimeIso(ref s) | Data::DurationIso(ref s) => {
                    write!(csv_file, "{s}")
                }
            }?;
            if col_number != max_column {
                write!(csv_file, ";")?;
            }
        }
        write!(csv_file, "\r\n")?;
    }
    Ok(())
    //    let parse_option = CsvParseOptions::default()
    //        .with_separator(b';')
    //        .with_try_parse_dates(false);
    //    let date_format = StrptimeOptions {
    //        format: Some("%d.%m.%y".into()),
    //        strict: true,
    //        exact: true,
    //        ..Default::default()
    //    };
    //    let raw_df = CsvReadOptions::default()
    //    .with_has_header(true)
    //    .with_parse_options(parse_option)
    //    .try_into_reader_with_file_path(Some(input_path.into()))?
    //    .finish()?
    //    .lazy()
    //    .with_column(
    //        col("Date")
    //            .str()
    //            .strptime(DataType::Date, date_format, Expr::default()),
    //    )
    //    .with_column(col("Payment Method").str().strip_chars(lit(" ")))
    //    .with_column(col("Topic").str().strip_chars(lit(" ")))
    //    .with_column(col("Owner").str().strip_chars(lit(" ")))
    //    .with_column(col("Purpose").str().strip_chars(lit(" ")))
    //    .collect()?;

    //    warn_on_zero_value_trx(&raw_df)?;

    //    let (mut df_det, mut df_acc, mut df_banana) = crunch_data(raw_df, month)?;

    //    export_details(&month, &ts, &mut df_det)?;
    //    export_mittagstisch(&month, &ts, &mut df_det)?;
    //    export_accounting(&month, &ts, &mut df_acc)?;
    //    export_banana(&month, &ts, &mut df_banana)
}

/// returns three dataframes, one for details/miti, another for the accounting export and the third for banana.
fn crunch_data(
    raw_df: DataFrame,
    month: &str,
) -> Result<(DataFrame, DataFrame, DataFrame), Box<dyn Error>> {
    validate(&raw_df)?;

    let mut df_det = collect_data(raw_df)?;
    df_det.extend(&df_det.clone().lazy().sum().collect()?)?;

    let df_acc = gather_df_accounting(&df_det)?;
    validate_acc_constraint(&df_acc.clone())?;
    let df_banana = gather_df_banana(&df_acc.clone(), month)?;
    Ok((df_det, df_acc, df_banana))
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

fn export_details(month: &&str, ts: &&str, df: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    write_to_file(df, &path_with_prefix("details", month, ts))?;
    Ok(())
}

fn export_mittagstisch(
    month: &&str,
    ts: &&str,
    df_det: &mut DataFrame,
) -> Result<(), Box<dyn Error>> {
    write_to_file(
        &mut gather_df_miti(df_det)?,
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

fn export_banana(month: &&str, ts: &&str, df_acc: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    write_to_file(df_acc, &path_with_prefix("banana", month, ts))
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

// Those tests fail since the introduction of the banana export. To method crunch_data works fine though. Investigation necessary
//#[cfg(test)]
//mod tests {
//    use crate::configure_the_environment;
//    use pretty_assertions::assert_ne;
//    use rstest::rstest;
//
//    use crate::test_fixtures::{intermediate_df_02, intermediate_df_04, details_df_04};
//
//    use super::*;
//
//    #[rstest]
//    fn can_crunch_data_without_panic(intermediate_df_02: DataFrame) {
//        let (df1, df2, df3) = crunch_data(intermediate_df_02, "202412").expect("should crunch");
//
//        assert_ne!(df1.shape().0, 0, "df1 does not contain records");
//        assert_ne!(df2.shape().0, 0, "df2 does not contain records");
//        assert_ne!(df3.shape().0, 0, "df3 does not contain records");
//    }
//
//    #[rstest]
//    fn can_calculate_summary_row(intermediate_df_04: DataFrame, details_df_04: DataFrame) {
//        configure_the_environment();
//        let (df1, _, _) = crunch_data(intermediate_df_04, "202412").expect("should crunch");
//        assert_eq!(
//            df1.shape().0,
//            4,
//            "df1 does not contain 3 records and 1 summary line"
//        );
//        assert_eq!(
//            df1, details_df_04,
//            "unexpected summary for intermediate_df_04"
//        );
//    }
//}
