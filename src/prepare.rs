use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;

use polars::datatypes::DataType;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars::io::SerReader;
use polars::prelude::*;
use polars::series::Series;
use polars_excel_writer::PolarsExcelWriter;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumIter, EnumString};

/// Processes the sumup input files (sales-report and transaction report) to produce an intermediate file.
/// Some derived fields are prepared based on heuristics in a best-effort approach (Topic, Owner, Purpose).
/// The user may optionally redact those where the heuristics are not sufficient.
/// The (potentially redacted) intermediate file will be the input for the exports.
pub fn prepare(
    sales_report: &Path,
    transaction_report: &Path,
    output_path: &Path,
    month: &str,
) -> Result<(), Box<dyn Error>> {
    let df = process_input(sales_report, transaction_report)?.sort(
        ["Date", "Time", "Transaction ID", "Description"],
        SortMultipleOptions::new()
            .with_multithreaded(false)
            .with_maintain_order(true),
    )?;
    let mut excel_writer = PolarsExcelWriter::new();
    excel_writer.set_worksheet_name(month)?;
    excel_writer.set_autofit(true);
    excel_writer.set_column_format("Date", "dd.mm.YYYY");
    excel_writer.set_dtype_float_format("#'##0.00");
    excel_writer.set_freeze_panes(1, 2);
    excel_writer.write_dataframe(&df)?;
    excel_writer.save(output_path)?;
    Ok(())
}

/// Reads the two input csv files from filesystem, processes the data returns it as dataframe.
fn process_input(
    sales_report: &Path,
    transaction_report: &Path,
) -> Result<DataFrame, Box<dyn Error>> {
    let parse_options = CsvParseOptions::default().with_separator(b',');
    let sr_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(1500))
        .with_parse_options(parse_options.clone())
        .try_into_reader_with_file_path(Some(sales_report.into()))?
        .finish()?
        .lazy()
        .with_column(
            when(
                col("Beschreibung")
                    .eq(lit("SCHICHTWECHSEL"))
                    .or(col("Beschreibung").eq(lit("Mittagstisch-Nachmittag")))
                    .or(col("Beschreibung").eq(lit("Nachmittag-Abend"))),
            )
            .then(0.0)
            .otherwise(col("Preis (brutto)"))
            .alias("Preis (brutto)"),
        )
        .with_column(
            when(
                col("Beschreibung")
                    .eq(lit("SCHICHTWECHSEL"))
                    .or(col("Beschreibung").eq(lit("Mittagstisch-Nachmittag")))
                    .or(col("Beschreibung").eq(lit("Nachmittag-Abend"))),
            )
            .then(0.0)
            .otherwise(col("Preis (netto)"))
            .alias("Preis (netto)"),
        )
        .collect()?;
    let txr_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(1500))
        .with_parse_options(parse_options)
        .try_into_reader_with_file_path(Some(transaction_report.into()))?
        .finish()?;
    fail_on_missing_trx(&txr_df, &sr_df)?;
    combine_input_dfs(&sr_df, &txr_df)
}

enum MissingTrxViolationError {
    MissingInSalesReport(DataFrame),
    MissingInTrxReport(DataFrame),
}

impl Display for MissingTrxViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MissingTrxViolationError::MissingInSalesReport(df) => {
                write!(f, "Some Transaktionsnummer missing in sales report! {df}")
            }
            MissingTrxViolationError::MissingInTrxReport(df) => {
                write!(
                    f,
                    "Some Transaktionsnummer missing in transaction report! {df}"
                )
            }
        }
    }
}

impl Debug for MissingTrxViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Error for MissingTrxViolationError {}

/// Abort if valid transactions are found in transaction report but not in sales report - or vice versa
fn fail_on_missing_trx(txr_df: &DataFrame, sr_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    let valid_stati = vec!["Erfolgreich", "Geplant"];
    let txr = txr_df
        .clone()
        .lazy()
        .filter(
            col("Status")
                .is_in(lit(Series::from_iter(valid_stati.clone())).implode(), false)
                .and(col("Transaktions-ID").is_not_null()),
        )
        .select([col("Transaktions-ID")]);
    let sr = sr_df
        .clone()
        .lazy()
        .filter(col("Transaktionsnummer").is_not_null())
        .select([col("Transaktionsnummer")]);
    let missing_in_sr = txr
        .clone()
        .join(
            sr.clone(),
            [col("Transaktions-ID")],
            [col("Transaktionsnummer")],
            JoinType::Anti.into(),
        )
        .collect()?;
    if missing_in_sr.shape().0 > 0 {
        Err(Box::from(MissingTrxViolationError::MissingInSalesReport(
            missing_in_sr,
        )))
    } else {
        let missing_in_txr = sr
            .join(
                txr,
                [col("Transaktionsnummer")],
                [col("Transaktions-ID")],
                JoinType::Anti.into(),
            )
            .collect()?;
        if missing_in_txr.shape().0 > 0 {
            Err(Box::from(MissingTrxViolationError::MissingInTrxReport(
                missing_in_txr,
            )))
        } else {
            Ok(())
        }
    }
}

/// Combines the sales report and transaction report dataframes into the intermediate dataframe.
#[allow(clippy::too_many_lines)]
fn combine_input_dfs(sr_df: &DataFrame, txr_df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    let raw_date_format = StrptimeOptions {
        format: Some("%d.%m.%Y".into()),
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

    let change_of_shift_df = sr_df
        .clone()
        .lazy()
        .filter(col("Beschreibung").eq(lit("SCHICHTWECHSEL")))
        .with_column(
            col("Datum")
                .str()
                .extract(lit(r".?(\d\d\.\d\d\.\d\d\d\d), "), 1)
                .str()
                .strptime(DataType::Date, raw_date_format.clone(), Expr::default())
                .alias("Date"),
        )
        .with_column(
            col("Date")
                .dt()
                .weekday()
                .cast(DataType::Int64)
                .is_in(
                    lit(Series::from_vec("we".into(), vec![6, 7])).implode(),
                    false,
                )
                .alias("is_weekend"),
        )
        .with_column(
            (col("Datum").str().extract(lit(r".{10}, (\d\d:\d\d)"), 1) + lit(":00"))
                .str()
                .to_time(time_format.clone())
                .alias("Time"),
        )
        .filter(
            col("is_weekend")
                .eq(lit(false))
                .and(col("Time").gt(lit("12:00:00").str().to_time(time_format.clone())))
                .and(col("Time").lt(lit("16:00:00").str().to_time(time_format.clone()))),
        )
        .select([col("Date"), col("Time").alias("ChangeOfShift")])
        .group_by([col("Date")])
        .agg([col("ChangeOfShift").last().alias("ChangeOfShift")]);

    let commission_df = txr_df
        .clone()
        .lazy()
        .filter(
            col("Transaktionsart")
                .eq(lit("Umsatz"))
                .and(col("Status").eq(lit("Erfolgreich"))),
        )
        .select([
            col("Transaktions-ID"),
            col("Betrag inkl. MwSt.").alias("Commissioned Total"),
            col("Gebühr").alias("Commission"),
        ]);

    let add_tips_df = txr_df
        .clone()
        .lazy()
        .filter(
            col("Transaktionsart")
                .eq(lit("Umsatz"))
                .and(col("Status").eq(lit("Erfolgreich")))
                .and(col("Trinkgeldbetrag").fill_null(0.0).gt(0.0)),
        )
        .select([
            col("Transaktions-ID"),
            col("Trinkgeldbetrag").fill_null(0.0).alias("TG"),
        ]);

    let additional_tip_df = sr_df
        .clone()
        .lazy()
        .join(
            add_tips_df,
            [col("Transaktionsnummer")],
            [col("Transaktions-ID")],
            JoinType::Inner.into(),
        )
        .filter(col("TG").gt(lit(0.0)))
        .select([
            col("Datum"),
            col("Typ"),
            col("Transaktionsnummer"),
            col("Zahlungsmethode"),
            lit(1).alias("Menge").cast(DataType::Int64),
            lit("Trinkgeld").alias("Beschreibung"),
            col("Währung"),
            col("TG").alias("Preis vor Rabatt"),
            lit(0.0).alias("Rabatt"),
            col("TG").alias("Preis (brutto)"),
            col("TG").alias("Preis (netto)"),
            lit(0.0).alias("Steuer"),
            lit(NULL).alias("Steuersatz"),
            col("Konto"),
        ])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?;

    let union_df = sr_df.vstack(&additional_tip_df)?;

    let df = union_df
        .lazy()
        .with_column(
            col("Datum")
                .str()
                .extract(lit(r".?(\d\d\.\d\d\.\d\d\d\d), "), 1)
                .str()
                .strptime(DataType::Date, raw_date_format.clone(), Expr::default())
                .alias("Date"),
        )
        .with_column(
            col("Date")
                .dt()
                .weekday()
                .cast(DataType::Int64)
                .is_in(
                    lit(Series::from_vec("we".into(), vec![6, 7])).implode(),
                    false,
                )
                .alias("is_weekend"),
        )
        .with_column(
            (col("Datum").str().extract(lit(r".{10}, (\d\d:\d\d)"), 1) + lit(":00"))
                .str()
                .to_time(time_format.clone())
                .alias("Time"),
        )
        .with_column(
            col("Beschreibung")
                .str()
                .strip_chars(lit(NULL))
                .alias("Beschreibung"),
        )
        .with_column(
            when(col("Steuer").eq(0.0))
                .then(lit(NULL))
                .otherwise(col("Steuer"))
                .alias("Tax"),
        )
        .with_column(
            when(col("Steuersatz").eq(lit("")))
                .then(lit(NULL))
                .when(col("Steuersatz").eq(lit("0%")))
                .then(lit(NULL))
                .otherwise(col("Steuersatz"))
                .alias("Tax rate"),
        )
        .with_column(
            when(col("Beschreibung").eq(lit("SCHICHTWECHSEL")))
                .then(lit(0.0))
                .otherwise(col("Preis (brutto)"))
                .alias("Preis (brutto)"),
        )
        .with_column(
            when(col("Beschreibung").eq(lit("SCHICHTWECHSEL")))
                .then(lit(0.0))
                .otherwise(col("Preis (netto)"))
                .alias("Preis (netto)"),
        )
        .join(
            commission_df,
            [col("Transaktionsnummer")],
            [col("Transaktions-ID")],
            JoinType::Left.into(),
        )
        .join(
            change_of_shift_df,
            [col("Date")],
            [col("Date")],
            JoinType::Left.into(),
        )
        .with_column(
            (col("Preis (brutto)") / col("Commissioned Total") * col("Commission"))
                .round(4, RoundMode::HalfToEven)
                .alias("Commission"),
        )
        .with_column(
            col("ChangeOfShift")
                .fill_null(lit("14:15:00").str().to_time(time_format.clone()))
                .alias("ChangeOfShift"),
        )
        .with_column(infer_payment_method().alias("Payment Method"))
        .with_column(infer_type().alias("Type"))
        .with_column(infer_topic(&time_format).alias("Topic"))
        .select([
            col("Konto").alias("Account"),
            col("Date"),
            col("Time"),
            col("Type"),
            col("Transaktionsnummer").alias("Transaction ID"),
            col("Payment Method"),
            col("Menge").alias("Quantity"),
            col("Beschreibung").alias("Description"),
            col("Währung").alias("Currency"),
            col("Preis (brutto)").alias("Price (Gross)"),
            col("Preis (netto)").alias("Price (Net)"),
            col("Commission"),
            col("Topic"),
            infer_owner().alias("Owner"),
            infer_purpose().alias("Purpose"),
            lit(NULL).alias("Comment"),
        ])
        .collect()?;
    warn_on_zero_value_trx(&df)?;
    Ok(df)
}

/// Accepts Typ with various values, returning either `Cash` or `Card`
fn infer_type() -> Expr {
    when(col("Typ").eq(lit("Verkauf")))
        .then(lit(RecordType::Sales.to_string()))
        .otherwise(lit(RecordType::Refund.to_string()))
}

/// Normalize payment methods
fn infer_payment_method() -> Expr {
    when(col("Zahlungsmethode").eq(lit("Bar")))
        .then(lit(PaymentMethod::Cash.to_string()))
        .otherwise(lit(PaymentMethod::Card.to_string()))
}

/// Infers the `Topic` from the time of sale:
/// before 06:00 and after 18:00 -> `Culture` or `PaidOut` if description contains " (PO)" or if it is a week-end.
/// between 06:00 and `ChangeOfShift` -> `MiTi`
/// between `ChangeOfShift` and 18:00 -> `Cafe`
/// If the description starts with "Recircle Tupper Depot", the topic will be `Packaging` regardless of time or day.
/// If the description is "Miete", the topic will be `Rental` regardless of time or day.
/// If the description starts with "Kerze", the topic will be Culture regardless of time or day.
fn infer_topic(time_options: &StrptimeOptions) -> Expr {
    when(
        col("Beschreibung")
            .str()
            .starts_with(lit("Recircle Tupper Depot")),
    )
    .then(lit(Topic::Packaging.to_string()))
    .when(col("Beschreibung").str().starts_with(lit("Kerze")))
    .then(lit(Topic::Culture.to_string()))
    .when(col("Beschreibung").eq(lit("Miete")))
    .then(lit(Topic::Rental.to_string()))
    .when(
        col("Time")
            .gt_eq(lit("06:00:00").str().to_time(time_options.clone()))
            .and(col("Time").lt(lit("18:00:00").str().to_time(time_options.clone())))
            .and(col("is_weekend").eq(lit(true))),
    )
    .then(lit(Topic::Culture.to_string()))
    .when(
        col("Time")
            .gt_eq(lit("06:00:00").str().to_time(time_options.clone()))
            .and(col("Time").lt_eq(col("ChangeOfShift")))
            .and(col("is_weekend").eq(lit(false))),
    )
    .then(lit(Topic::MiTi.to_string()))
    .when(
        col("Time")
            .gt(col("ChangeOfShift"))
            .and(col("Time").lt(lit("18:00:00").str().to_time(time_options.clone())))
            .and(col("is_weekend").eq(lit(false))),
    )
    .then(lit(Topic::Cafe.to_string()))
    .otherwise(lit(Topic::Culture.to_string()))
}

/// Infers the `Owner` from `Topic` and `Beschreibung`:
/// - if `Topic` is neither `MiTi` nor `Culture`, the `Owner` will be blank
/// - for `Topic::Culture`, the Owner is `PaidOut` if the description ends with ' (PO)'
/// - for `Topic::MiTi`, the Owner is `MiTi` if the description matches one of a hardcoded list of Strings.
/// - Otherwise the owner is `LoLa` (for both `MiTi` and `Culture`)
fn infer_owner() -> Expr {
    when(
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Topic").neq(lit(Topic::Culture.to_string()))),
    )
    .then(lit(NULL))
    .when(
        col("Topic")
            .eq(lit(Topic::Culture.to_string()))
            .and(col("Beschreibung").str().contains(lit(" \\(PO\\)"), true)),
    )
    .then(lit("PaidOut"))
    .when(
        col("Topic").eq(lit(Topic::MiTi.to_string())).and(
            col("Beschreibung")
                .str()
                .contains(lit("Hauptgang"), true)
                .or(col("Beschreibung")
                    .str()
                    .contains(lit("Kinderteller"), true))
                .or(col("Beschreibung")
                    .str()
                    .contains(lit("Senioren-Mittagstisch"), true))
                .or(col("Beschreibung")
                    .str()
                    .contains(lit("Seniorenmittagstisch"), true))
                .or(col("Beschreibung").str().contains(lit("Kindermenü"), true))
                .or(col("Beschreibung").str().contains(lit("Kinderpasta"), true))
                .or(col("Beschreibung").str().contains(lit("Menü"), true))
                .or(col("Beschreibung").str().contains(lit("Dessert"), true))
                .or(col("Beschreibung").str().contains(lit("Praktik"), true))
                .or(col("Beschreibung").str().contains(lit("Vorspeise"), true))
                .or(col("Beschreibung")
                    .str()
                    .contains(lit("Vorsp\\. \\+ Hauptsp\\."), true))
                .or(col("Beschreibung").str().contains(lit("Hauptspeise"), true))
                .or(col("Beschreibung").str().contains(lit("Trinkgeld"), true))
                .or(col("Beschreibung")
                    .str()
                    .strip_chars(lit(NULL))
                    .eq(lit(""))
                    .and(col("Preis (brutto)").gt_eq(5.0))),
        ),
    )
    .then(lit(Owner::MiTi.to_string()))
    .otherwise(lit(Owner::LoLa.to_string()))
}

/// Infers the purpose based on the `Description`: Description "Trinkgeld" leads to `Tip`,
/// `Consumption` otherwise.
fn infer_purpose() -> Expr {
    when(col("Beschreibung").str().contains(lit("Trinkgeld"), true))
        .then(lit(Purpose::Tip.to_string()))
        .otherwise(lit(Purpose::Consumption.to_string()))
}

// Outputs logs to console if one or more transactions with a net price 0.0 are found
pub fn warn_on_zero_value_trx(df: &DataFrame) -> Result<(), Box<dyn Error>> {
    let violating = df
        .clone()
        .lazy()
        .filter(
            col("Price (Net)")
                .eq(lit(0.0))
                .or(col("Price (Net)").is_null()),
        )
        .filter(
            col("Description")
                .neq(lit("Mittagstisch-Nachmittag"))
                .and(col("Description").neq(lit("Nachmittag-Abend")))
                .and(col("Description").neq(lit("SCHICHTWECHSEL"))),
        )
        .collect()?;
    if violating.shape().0 > 0 {
        println!("Records found in intermediate document with net price missing or 0.0:");
        println!("{violating:?}");
        println!("Please verify is this is correct and adjust if necessary.");
    }
    Ok(())
}

//Different types of transactions, in data currently only Verkauf (`Sales`)
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display, EnumIter)]
pub enum RecordType {
    Sales,
    Refund,
}

/// Payment method as defined in the sumup sales report
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display, EnumIter)]
pub enum PaymentMethod {
    /// Sales paid with cash
    Cash,
    /// Sales paid with card
    Card,
}

/// Derived Topics, based on time of day
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display, EnumIter)]
pub enum Topic {
    /// "Mittags-Tisch", managed by Verein Mittagstisch, includes the morning café
    MiTi,
    /// Afternoon café, managed by `LoLa`
    Cafe,
    /// ("Vermietung") Room rentals by third parties, managed by `LoLa`
    Verm,
    /// ("Sommerfest" or summer party) Items sold during summer party
    SoFe,
    /// Key deposit
    Deposit,
    /// Rental fee
    Rental,
    /// Cultural Payments
    Culture,
    /// Sold reusable packaging material - reduces the material costs.
    Packaging,
}

/// Derived purpose
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display, EnumIter)]
pub enum Purpose {
    /// Sold products
    Consumption,
    /// Optional tips
    Tip,
}

/// Derived main owner of the income of sales by Mittagstisch.
/// Sales by `LoLa` directly does not have an owner.
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display, EnumIter)]
pub enum Owner {
    /// `LoLa` as main owner of sales by Mittags-Tisch (Topic `MiTi`) or Cooperation partners (Topic Culture)
    LoLa,
    /// Mittags-Tisch as main owner of sales by Mittags-Tisch.
    MiTi,
    /// Cooparation partner as main owner of their sales (-> `PaidOut`)
    PaidOut,
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use rstest::*;

    use crate::test_fixtures::{
        intermediate_df_01, intermediate_df_07, sales_report_df_01, sales_report_df_02,
        sales_report_df_07, transaction_report_df_01, transaction_report_df_02,
        transaction_report_df_07,
    };
    use crate::test_utils::assert_dataframe;

    use super::*;

    #[rstest]
    fn given_trx_in_both_frames_when_asserting_we_should_not_fail(
        transaction_report_df_01: DataFrame,
        sales_report_df_01: DataFrame,
    ) {
        let result = fail_on_missing_trx(&transaction_report_df_01, &sales_report_df_01);
        assert!(result.is_ok(), "trx should be present in both data frames");
    }

    #[rstest]
    fn given_trx_not_in_sales_report_when_asserting_we_should_fail(
        transaction_report_df_01: DataFrame,
        sales_report_df_02: DataFrame,
    ) {
        let result = fail_on_missing_trx(&transaction_report_df_01, &sales_report_df_02);
        assert!(result.is_err(), "trx should not be found in sales report");
    }

    #[rstest]
    fn given_trx_not_in_trx_report_when_asserting_we_should_fail(
        transaction_report_df_02: DataFrame,
        sales_report_df_01: DataFrame,
    ) {
        let result = fail_on_missing_trx(&transaction_report_df_02, &sales_report_df_01);
        assert!(result.is_err(), "trx should not be found in sales report");
    }

    #[rstest]
    fn test_combine_input_dfs(
        sales_report_df_01: DataFrame,
        transaction_report_df_01: DataFrame,
        intermediate_df_01: DataFrame,
    ) {
        let out = combine_input_dfs(&sales_report_df_01, &transaction_report_df_01)
            .expect("should be able to combine input dfs");
        assert_dataframe(&out, &intermediate_df_01);
    }

    #[rstest]
    fn test_combine_input_dfs_with_multiple_changes_of_shift(
        sales_report_df_07: DataFrame,
        transaction_report_df_07: DataFrame,
        intermediate_df_07: DataFrame,
    ) {
        let out = combine_input_dfs(&sales_report_df_07, &transaction_report_df_07)
            .expect("should be able to combine input dfs");
        assert_dataframe(&out, &intermediate_df_07);
    }

    #[fixture]
    fn topic_sample_df() -> DataFrame {
        let rtd = "Recircle Tupper Depot";
        df! [
            "Beschreibung" => [
                "X", "X",
                "X", "X",
                "X", "X",
                "X", "X",
                "X", "X",
                "X", "X",
                "X", "X",
                "X (PO)", "X (PO)", "X (PO) x" ,"X (PO)",
                rtd, rtd, rtd, rtd, rtd, rtd, rtd, rtd,
                "Miete", "Miete",
                "Kerze 4", "Kerze 2", "Kerze klein",
            ],
            "Time" => [
                "00:00:00", "05:59:59",
                "06:00:00", "14:15:00",
                "06:00:00", "14:15:00",
                "14:15:01", "17:59:59",
                "14:28:01", "14:30:00",
                "14:15:01", "17:59:59",
                "18:00:00", "23:59:59",
                "00:00:00", "05:59:59", "18:00:00", "23:59:59",
                "00:00:00", "05:59:59", "06:00:00", "14:15:00", "14:15:01", "17:59:59", "18:00:00", "23:59:59",
                "00:00:00", "12:00:00",
                "09:00:00", "14:00:00", "18:00:00",
            ],
            "ChangeOfShift" => [
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00",
                "14:29:00", "14:30:00",
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00", "14:15:00", "14:15:00",
                "14:15:00", "14:15:00", "14:15:00", "14:15:00", "14:15:00", "14:15:00", "14:15:00", "14:15:00",
                "14:15:00", "14:15:00",
                "14:15:00", "14:15:00", "14:15:00",
            ],
            "is_weekend" => [
                false, false,
                false, false,
                true, true,
                false, false,
                false, false,
                true, true,
                true, true,
                false, false, true, true,
                false, false, false, false, false, true, true, false,
                false, true,
                false, true, false,
            ],
        ]
        .unwrap()
    }

    #[fixture]
    fn topic_expected_df() -> DataFrame {
        let culture = Topic::Culture.to_string();
        let miti = Topic::MiTi.to_string();
        let cafe = Topic::Cafe.to_string();
        let pkg = Topic::Packaging.to_string();
        let rental = Topic::Rental.to_string();
        df![
           "Topic" => [
               culture.clone(), culture.clone(),
               miti.clone(), miti.clone(),
               culture.clone(), culture.clone(),
               cafe.clone(), cafe,
               miti.clone(), miti,
               culture.clone(), culture.clone(),
               culture.clone(), culture.clone(),
               culture.clone(), culture.clone(), culture.clone(), culture.clone(),
               pkg.clone(), pkg.clone(), pkg.clone(), pkg.clone(), pkg.clone(), pkg.clone(), pkg.clone(), pkg,
               rental.clone(), rental,
               culture.clone(), culture.clone(), culture,
           ]
       ].unwrap()
    }

    #[rstest]
    fn test_infer_topic(topic_sample_df: DataFrame, topic_expected_df: DataFrame) {
        let time_format = StrptimeOptions {
            format: Some("%H:%M:%S".into()),
            strict: true,
            exact: true,
            ..Default::default()
        };
        let result = topic_sample_df
            .lazy()
            .with_column(infer_topic(&time_format).alias("Topic"))
            .select([col("Topic")])
            .collect()
            .unwrap();
        assert_eq!(result, topic_expected_df);
    }
}
