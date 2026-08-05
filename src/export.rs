use calamine::{Data, Reader, Xlsx, open_workbook};
use chrono::NaiveDate;
use polars::prelude::*;
use polars_excel_writer::PolarsExcelWriter;
use rust_xlsxwriter::Workbook;
use std::collections::HashSet;
use std::error::Error;
use std::path::{Path, PathBuf};

use crate::export::constraint::{
    validate_owners, validate_payment_methods, validate_purposes, validate_topics,
    validation_topic_owner,
};
use crate::export::export_accounting::{
    gather_df_accounting, validate_acc_constraint_and_calculate_correction_postings,
};
use crate::export::export_banana::{gather_df_banana, gather_df_banana_details};
use crate::export::export_details::collect_data;
use crate::export::export_miti::gather_df_miti;
use crate::export::export_reconciliation::gather_df_reconciliation;
use crate::prepare::{Topic, warn_on_zero_value_trx};

mod constraint;
mod export_accounting;
mod export_banana;
mod export_details;
mod export_miti;
mod export_reconciliation;
mod posting;

const EXCEL_EPOCH_OFFSET: i32 = 25569;
const MS_PER_DAY: f64 = 86_400_000_000_000f64;

/// Reads the intermediate files and exports all configured reports.
pub fn export(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let raw_df = read_intermediate_from_excel(input_path, month)?;

    warn_on_zero_value_trx(&raw_df)?;

    let (df_det, df_acc, df_banana, raw_df_corr) = crunch_data(&raw_df.clone(), month)?;

    export_details(month, ts, &df_det, &raw_df_corr)?;
    export_mittagstisch(month, ts, &df_det, &raw_df_corr)?;
    export_accounting(month, ts, &df_acc, &raw_df_corr)?;
    export_reconciliation(month, ts, &df_det, &raw_df_corr)?;
    export_banana(month, ts, &df_banana, &raw_df_corr)
}

#[allow(clippy::too_many_lines)]
fn read_intermediate_from_excel(
    input_path: &Path,
    month: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let columns_vec = read_columns_from_excel(input_path, month)?;
    let height = columns_vec.first().map_or(0, polars::prelude::Column::len);
    let df = DataFrame::new(height, columns_vec)?
        .lazy()
        .with_column(
            col("Date")
                .apply(
                    |s| {
                        let parsed: Int32Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| {
                                opt_s
                                    .and_then(|s| s.parse::<i32>().ok())
                                    .map(|n| n - EXCEL_EPOCH_OFFSET)
                            })
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int32)),
                )
                .cast(DataType::Date)
                .alias("Date"),
        )
        .with_column(
            col("Time")
                .apply(
                    |s| {
                        let parsed: Float64Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| {
                                opt_s
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .map(|f| f * MS_PER_DAY)
                            })
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Float64)),
                )
                .cast(DataType::Time)
                .alias("Time"),
        )
        .with_column(
            col("Quantity")
                .map(
                    |s| {
                        let parsed: Int32Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| opt_s.and_then(|s| s.parse::<i32>().ok()))
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int32)),
                )
                .alias("Quantity"),
        )
        .with_column(
            col("Price (Gross)")
                .map(
                    |s| {
                        let parsed: Float64Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| opt_s.and_then(|s| s.parse::<f64>().ok()))
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int32)),
                )
                .alias("Price (Gross)"),
        )
        .with_column(
            col("Price (Net)")
                .map(
                    |s| {
                        let parsed: Float64Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| opt_s.and_then(|s| s.parse::<f64>().ok()))
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int32)),
                )
                .alias("Price (Net)"),
        )
        .with_column(
            col("Commission")
                .map(
                    |s| {
                        let parsed: Float64Chunked = s
                            .str()?
                            .iter()
                            .map(|opt_s| opt_s.and_then(|s| s.parse::<f64>().ok()))
                            .collect();
                        Ok(parsed.into_column())
                    },
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int32)),
                )
                .alias("Commission"),
        )
        .with_column(col("Payment Method").str().strip_chars(lit(" ")))
        .with_column(col("Topic").str().strip_chars(lit(" ")))
        .with_column(col("Owner").str().strip_chars(lit(" ")))
        .with_column(col("Purpose").str().strip_chars(lit(" ")))
        .sort(["Date"], SortMultipleOptions::new().with_nulls_last(true))
        .collect()?;
    Ok(df)
}

/// reads the columns of Sheet1 as Vec of Columns
fn read_columns_from_excel(input_path: &Path, month: &str) -> Result<Vec<Column>, Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(input_path)?;
    let range = workbook.worksheet_range(month)?;
    let headers = range.headers().expect("No headers found in Sheet1");
    let rows: Vec<Vec<Data>> = range.rows().map(<[calamine::Data]>::to_vec).collect();

    let data_rows = &rows[1..];
    let n_cols = headers.len();
    let mut columns: Vec<Vec<Data>> = vec![Vec::with_capacity(data_rows.len()); n_cols];
    for row in data_rows {
        for (i, cell) in row.iter().enumerate().take(n_cols) {
            columns[i].push(cell.clone());
        }
    }
    let mut columns_vec = Vec::with_capacity(n_cols);
    for (i, col_cells) in columns.into_iter().enumerate() {
        let col_strings: Vec<String> = col_cells
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        let pl_header = PlSmallStr::from(headers[i].as_str());
        columns_vec.push(Column::new(pl_header, col_strings));
    }
    Ok(columns_vec)
}

/// Returns for the raw intermediate dataframe `raw_df` and `month` returns four dataframes:
/// - details/miti
/// - accounting export
/// - banana (filterd and enriched in regards of trx that need individual reporting instead of summary)
/// - `raw_df_corr`: The raw data frame passed in enriched with potential rounding corrections
fn crunch_data(
    raw_df: &DataFrame,
    month: &str,
) -> Result<(DataFrame, DataFrame, DataFrame, DataFrame), Box<dyn Error>> {
    validate(raw_df)?;

    let mut df_det = collect_data(raw_df.clone())?;
    df_det.extend(&df_det.clone().lazy().sum().collect()?)?;
    let df_det_extended =
        df_det.sort(["Date"], SortMultipleOptions::new().with_nulls_last(true))?;
    let df_acc = gather_df_accounting(&df_det_extended)?;
    let mut df_banana_summary = gather_df_banana(&df_acc.clone(), month)?;
    let correction = validate_acc_constraint_and_calculate_correction_postings(&df_acc, month)?;
    df_banana_summary.extend(&correction)?;
    let df_banana_details = gather_df_banana_details(raw_df)?;
    let df_banana = filter_and_enrich_banana(&df_banana_summary, &df_banana_details)?;
    Ok((df_det_extended, df_acc, df_banana, raw_df.clone()))
}

fn validate(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    let validated_columns = [
        col("Row-No"),
        col("Date"),
        col("Time"),
        col("Transaction ID"),
        col("Payment Method"),
        col("Description"),
        col("Price (Net)"),
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

fn export_details(
    month: &str,
    ts: &str,
    df: &DataFrame,
    df_trx: &DataFrame,
) -> Result<(), Box<dyn Error>> {
    write_to_file(df, df_trx, "details", month, ts)?;
    Ok(())
}

fn export_mittagstisch(
    month: &str,
    ts: &str,
    df_det: &DataFrame,
    df_trx: &DataFrame,
) -> Result<(), Box<dyn Error>> {
    let df_miti = gather_df_miti(df_det)?;
    let df_miti_trx = df_trx
        .clone()
        .lazy()
        .filter(col("Topic").eq(lit(Topic::MiTi.to_string())))
        .collect()?;
    write_to_file(&df_miti, &df_miti_trx, "mittagstisch", month, ts)?;
    Ok(())
}

fn export_reconciliation(
    month: &str,
    ts: &str,
    df_det: &DataFrame,
    df_trx: &DataFrame,
) -> Result<(), Box<dyn Error>> {
    let df_reconciliation = gather_df_reconciliation(df_det)?;
    write_to_file(&df_reconciliation, df_trx, "belegskontrolle", month, ts)?;
    Ok(())
}

fn export_accounting(
    month: &str,
    ts: &str,
    df_acc: &DataFrame,
    df_trx: &DataFrame,
) -> Result<(), Box<dyn Error>> {
    write_to_file(df_acc, df_trx, "accounting", month, ts)
}

/// filters out the summary records for accounts that need individual trx
/// instead of monthly summaries and enriches it with the individual rows
fn filter_and_enrich_banana(
    df_banana_summary: &DataFrame,
    df_banana_details: &DataFrame,
) -> PolarsResult<DataFrame> {
    if df_banana_details.shape().0 == 0 {
        df_banana_summary.sort(["Datum"], SortMultipleOptions::new().with_nulls_last(true))
    } else {
        let unwanted_accounts_df = df_banana_details
            .clone()
            .lazy()
            .select([col("KtHaben")])
            .unique(None, UniqueKeepStrategy::First)
            .collect()?;
        let unwanted_accounts: HashSet<&str> = unwanted_accounts_df
            .column("KtHaben")?
            .str()?
            .iter()
            .flatten()
            .collect::<Vec<&str>>()
            .into_iter()
            .collect();
        let mask_vals: Vec<bool> = df_banana_summary
            .column("KtHaben")?
            .str()?
            .iter()
            .map(|opt| match opt {
                Some(v) => !unwanted_accounts.contains(v),
                None => false,
            })
            .collect();
        let mask = BooleanChunked::from_slice("mask".into(), &mask_vals);
        let df_banana_filtered = df_banana_summary.clone().filter(&mask)?;
        let df_banana_enriched = df_banana_filtered.vstack(df_banana_details)?;
        df_banana_enriched.sort(
            ["Datum", "Beschreibung"],
            SortMultipleOptions::new().with_nulls_last(true),
        )
    }
}

fn export_banana(
    month: &str,
    ts: &str,
    df_banana: &DataFrame,
    df_trx: &DataFrame,
) -> Result<(), Box<dyn Error>> {
    write_to_file(df_banana, df_trx, "banana", month, ts)
}

/// Constructs a path for an XLSX file from `prefix`, `month` and `ts` (timestamp).
fn path_with_prefix(prefix: &str, month: &str, ts: &str) -> PathBuf {
    PathBuf::from(format!("{prefix}_{month}_{ts}.xlsx"))
}

/// Returns the name of the first of the `sheets` that has no rows to export, if any.
fn first_empty_sheet<'a>(sheets: &[(&'a str, usize)]) -> Option<&'a str> {
    sheets
        .iter()
        .find(|(_, rows)| *rows == 0)
        .map(|(name, _)| *name)
}

/// German message stating that the report `prefix` was skipped because sheet `empty_sheet` has no rows.
fn skipped_message(prefix: &str, empty_sheet: &str) -> String {
    format!(
        "Bericht «{prefix}» übersprungen: keine Zeilen zum Exportieren (Blatt «{empty_sheet}» ist leer)."
    )
}

/// German message stating that the report `prefix` was written to `path`, listing the rows per sheet.
fn exported_message(prefix: &str, path: &Path, sheets: &[(&str, usize)]) -> String {
    let per_sheet: Vec<String> = sheets
        .iter()
        .map(|(name, rows)| format!("«{name}»: {rows} Zeilen"))
        .collect();
    format!(
        "Bericht «{prefix}» nach {} exportiert, mit {} Blättern ({}).",
        path.display(),
        sheets.len(),
        per_sheet.join(", ")
    )
}

/// Writes the dataframe `df` to the file system into path `path`.
///
/// Skips the report entirely if any of its sheets has no rows - `rust_xlsxwriter` cannot
/// create a table without at least one row of data.
fn write_to_file(
    main_df: &DataFrame,
    trx_df: &DataFrame,
    prefix: &str,
    month: &str,
    ts: &str,
) -> Result<(), Box<dyn Error>> {
    // work around https://github.com/jmcnamara/polars_excel_writer/issues/26
    let mut trx_df = trx_df.clone();
    trx_df.rechunk_mut();

    let sheets = [
        (prefix, main_df.height()),
        ("transaktionen", trx_df.height()),
    ];
    if let Some(empty_sheet) = first_empty_sheet(&sheets) {
        println!("{}", skipped_message(prefix, empty_sheet));
        return Ok(());
    }

    let path = &path_with_prefix(prefix, month, ts);
    let mut excel_writer = PolarsExcelWriter::new();

    excel_writer.set_autofit(true);

    excel_writer.set_column_format("Date", "dd.mm.YYYY");
    excel_writer.set_dtype_float_format("#,##0.00");

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet().set_name(prefix)?;
    excel_writer.set_freeze_panes(1, 1);

    excel_writer.write_dataframe_to_worksheet(main_df, worksheet, 0, 0)?;

    let worksheet = workbook.add_worksheet().set_name("transaktionen")?;
    excel_writer.set_freeze_panes(1, 3);
    excel_writer.write_dataframe_to_worksheet(&trx_df, worksheet, 0, 0)?;
    excel_writer.set_column_format("Time", "HH:MM:SS");

    workbook.save(path)?;

    println!("{}", exported_message(prefix, path, &sheets));
    Ok(())
}

/// return the last of the month from provided `month`
pub fn get_last_of_month_nd(month: &str) -> Result<NaiveDate, Box<dyn Error>> {
    let y: i32 = month.chars().take(4).collect::<String>().parse()?;
    let m: u32 = month.chars().skip(4).collect::<String>().parse()?;
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let lom = NaiveDate::from_ymd_opt(ny, nm, 1)
        .ok_or("should be able to create next_day")
        .map(|nd| nd - chrono::Duration::days(1))?;
    Ok(lom)
}

#[cfg(test)]
mod tests {
    use crate::{configure_the_environment, test_utils::assert_dataframe};
    use pretty_assertions::assert_ne;
    use rstest::rstest;

    use crate::test_fixtures::{
        accounting_df_06, banana_df_ext_06, details_df_04, intermediate_df_02, intermediate_df_04,
        intermediate_df_06,
    };

    use super::*;

    #[rstest]
    #[case(&[("details", 21), ("transaktionen", 20)], None)]
    #[case(&[("mittagstisch", 6), ("transaktionen", 0)], Some("transaktionen"))]
    #[case(&[("banana", 0), ("transaktionen", 20)], Some("banana"))]
    #[case(&[("banana", 0), ("transaktionen", 0)], Some("banana"))]
    fn test_first_empty_sheet(#[case] sheets: &[(&str, usize)], #[case] expected: Option<&str>) {
        assert_eq!(first_empty_sheet(sheets), expected);
    }

    #[test]
    fn test_skipped_message() {
        assert_eq!(
            skipped_message("mittagstisch", "transaktionen"),
            "Bericht «mittagstisch» übersprungen: keine Zeilen zum Exportieren (Blatt «transaktionen» ist leer)."
        );
    }

    #[test]
    fn test_exported_message() {
        assert_eq!(
            exported_message(
                "details",
                &path_with_prefix("details", "202607", "20260805_202727"),
                &[("details", 21), ("transaktionen", 20)],
            ),
            "Bericht «details» nach details_202607_20260805_202727.xlsx exportiert, mit 2 Blättern («details»: 21 Zeilen, «transaktionen»: 20 Zeilen)."
        );
    }

    #[rstest]
    fn can_crunch_data_without_panic(intermediate_df_02: DataFrame) {
        println!("{intermediate_df_02:?}");
        let (df1, df2, df3, df4) =
            crunch_data(&intermediate_df_02, "202412").expect("should crunch");

        assert_ne!(df1.shape().0, 0, "df1 does not contain records");
        assert_ne!(df2.shape().0, 0, "df2 does not contain records");
        assert_ne!(df3.shape().0, 0, "df3 does not contain records");
        assert_ne!(df4.shape().0, 0, "df4 does not contain records");
    }

    #[rstest]
    fn can_calculate_summary_row(intermediate_df_04: DataFrame, details_df_04: DataFrame) {
        configure_the_environment();
        let (df1, _, _, _) = crunch_data(&intermediate_df_04, "202412").expect("should crunch");
        assert_eq!(
            df1.shape().0,
            4,
            "df1 does not contain 3 records and 1 summary line"
        );
        assert_eq!(
            df1, details_df_04,
            "unexpected summary for intermediate_df_04"
        );
    }

    #[rstest]
    fn can_calculate_banana(
        intermediate_df_06: DataFrame,
        accounting_df_06: DataFrame,
        banana_df_ext_06: DataFrame,
    ) {
        let mut acct_df = accounting_df_06.clone();
        acct_df
            .extend(
                &accounting_df_06
                    .clone()
                    .lazy()
                    .sum()
                    .collect()
                    .expect("Should be able to sum accounting_df_06"),
            )
            .expect("Should be able to extend accounting_df_06");
        let acct_df_ext = acct_df
            .sort(["Date"], SortMultipleOptions::new().with_nulls_last(true))
            .expect("Should be able to sort extended accounting_df_06");
        let df_banana_summary =
            gather_df_banana(&acct_df_ext, "202303").expect("Unable to get banana df");
        let df_banana_details =
            gather_df_banana_details(&intermediate_df_06).expect("Unable to get banana df details");
        let out = filter_and_enrich_banana(&df_banana_summary, &df_banana_details)
            .expect("Unable to get filtered and enriched df");
        assert_dataframe(&out, &banana_df_ext_06);
    }
}
