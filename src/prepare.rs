use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use polars::datatypes::DataType;
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars::io::{SerReader, SerWriter};
use polars::prelude::LiteralValue::Null;
use polars::prelude::{
    col, lit, when, CsvReader, CsvWriter, Expr, IntoLazy, JoinType, LazyFrame, LiteralValue,
    StrptimeOptions,
};
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
) -> Result<(), Box<dyn Error>> {
    let mut df = process_input(sales_report, transaction_report)?.sort(
        ["Date", "Time", "Transaction ID", "Description"],
        false,
        true,
    )?;
    let iowtr: Box<dyn Write> = Box::new(File::create(output_path)?);
    CsvWriter::new(iowtr)
        .with_separator(b';')
        .include_header(true)
        .with_date_format(Some("%d.%m.%Y".into()))
        .with_time_format(Some("%H:%M:%S".into()))
        .finish(&mut df)?;
    Ok(())
}

/// Reads the two input csv files from filesystem, processes the data returns it as dataframe.
fn process_input(
    sales_report: &Path,
    transaction_report: &Path,
) -> Result<DataFrame, Box<dyn Error>> {
    let sr_df = CsvReader::from_path(sales_report)?
        .has_header(true)
        .with_separator(b',')
        .infer_schema(Some(1500))
        .finish()?;
    let txr_df = CsvReader::from_path(transaction_report)?
        .has_header(true)
        .with_separator(b',')
        .finish()?;
    combine_input_dfs(&sr_df, &txr_df)
}

/// Combines the sales report and transaction report dataframes into the summary dataframe.
#[allow(clippy::too_many_lines)]
fn combine_input_dfs(sr_df: &DataFrame, txr_df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
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

    let commission_df = txr_df
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

    let (refunded_ids_1, refunded_ids_2) = refunded_id_dfs(sr_df);

    let add_tips_df = txr_df
        .clone()
        .lazy()
        .filter(
            col("Zahlungsart")
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
            [col("Transaction ID")],
            [col("Transaktions-ID")],
            JoinType::Inner.into(),
        )
        .filter(col("TG").gt(lit(0.0)))
        .select([
            col("Account"),
            col("Date"),
            col("Time"),
            col("Type"),
            col("Transaction ID"),
            col("Receipt Number"),
            col("Payment Method"),
            lit(1_i64).alias("Quantity"),
            lit("Trinkgeld").alias("Description"),
            col("Currency"),
            col("TG").alias("Price (Gross)"),
            col("TG").alias("Price (Net)"),
            lit(0.0).alias("Tax"),
            lit("").alias("Tax rate"),
            lit("").alias("Transaction refunded"),
        ])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?;

    let union_df = sr_df.vstack(&additional_tip_df)?;

    let df = union_df
        .lazy()
        .with_column(
            col("Date")
                .str()
                .strptime(DataType::Date, raw_date_format, Expr::default())
                .alias("Date"),
        )
        .with_column(
            (col("Time") + lit(":00"))
                .str()
                .to_time(time_format.clone())
                .alias("Time"),
        )
        .with_column(
            col("Description")
                .str()
                .strip_chars(lit(Null))
                .alias("Description"),
        )
        .with_column(infer_topic(time_format).alias("Topic"))
        .join(
            refunded_ids_1,
            [col("Transaction ID")],
            [col("TransactionId")],
            JoinType::Left.into(),
        )
        .join(
            refunded_ids_2,
            [col("Transaction refunded")],
            [col("TransactionId")],
            JoinType::Left.into(),
        )
        .join(
            commission_df,
            [col("Transaction ID")],
            [col("Transaktions-ID")],
            JoinType::Left.into(),
        )
        .with_column(
            (col("Price (Gross)") / col("Commissioned Total") * col("Commission"))
                .round(4)
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
            Expr::Literal(LiteralValue::Null).alias("Comment"),
        ])
        .collect()?;
    Ok(df)
}

/// returns two `LazyFrames` with transaction ids of refunded transactions.
/// the first `LazyFrame` ahs columns `TransactionId` and "refunded1" (with static literal 'x' as values)
/// the second `LazyFrame` ahs columns `TransactionId` and "refunded2" (with static literal 'x' as values)
fn refunded_id_dfs(sr_df: &DataFrame) -> (LazyFrame, LazyFrame) {
    let all_transaction_ids = sr_df.clone().lazy().select([col("Transaction ID")]);
    let ids_of_refunded = sr_df
        .clone()
        .lazy()
        .filter(col("Transaction refunded").is_not_null())
        .select([col("Transaction refunded")])
        .join(
            all_transaction_ids,
            [col("Transaction refunded")],
            [col("Transaction ID")],
            JoinType::Inner.into(),
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

/// Infers the `Topic` from the time of sale:
/// before 14:15 -> `MiTi`
/// between 14:15 and 18:00 -> `Cafe`
/// after 18:00 -> `Vermiet`
/// If the description starts with "Recircle Tupper Depot", the topic will be `Packaging` regardless of time od day.
fn infer_topic(time_options: StrptimeOptions) -> Expr {
    when(
        col("Description")
            .str()
            .starts_with(lit("Recircle Tupper Depot")),
    )
    .then(lit(Topic::Packaging.to_string()))
    .when(col("Time").lt(lit("14:15:00").str().to_time(time_options.clone())))
    .then(lit(Topic::MiTi.to_string()))
    .when(col("Time").gt(lit("18:00:00").str().to_time(time_options)))
    .then(lit(Topic::Verm.to_string()))
    .otherwise(lit(Topic::Cafe.to_string()))
}

/// Infers the `Owner` from `Topic` and `Description`:
/// - if `Topic` is not `MiTi`, the `Owner` will be blank
/// - for `Topic::MiTi`, the Owner is `MiTi` if the description matches one of a hardcoded list of Strings.
///   Otherwise the owner is `LoLa`.
fn infer_owner() -> Expr {
    when(col("Topic").neq(lit(Topic::MiTi.to_string())))
        .then(Expr::Literal(LiteralValue::Null))
        .when(
            col("Description")
                .str()
                .contains(lit("Hauptgang"), true)
                .or(col("Description").str().contains(lit("Kinderteller"), true))
                .or(col("Description").str().contains(lit("Kindermenü"), true))
                .or(col("Description").str().contains(lit("Menü"), true))
                .or(col("Description").str().contains(lit("Dessert"), true))
                .or(col("Description").str().contains(lit("Praktik"), true))
                .or(col("Description").str().contains(lit("Vorspeise"), true))
                .or(col("Description").str().contains(lit("Hauptspeise"), true))
                .or(col("Description").str().contains(lit("Trinkgeld"), true))
                .or(col("Description")
                    .str()
                    .strip_chars(lit(Null))
                    .eq(lit(""))
                    .and(col("Price (Gross)").gt_eq(5.0))),
        )
        .then(lit(Owner::MiTi.to_string()))
        .otherwise(lit(Owner::LoLa.to_string()))
}

/// Infers the purpose based on the `Description`: Description "Trinkgeld" leads to `Tip`,
/// `Consumption` otherwise.
fn infer_purpose() -> Expr {
    when(col("Description").str().contains(lit("Trinkgeld"), true))
        .then(lit(Purpose::Tip.to_string()))
        .otherwise(lit(Purpose::Consumption.to_string()))
}

/// Record Type as defined in the sumup sales report
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display)]
enum RecordType {
    Sales,
    Refund,
}

/// Payment method as defined in the sumup sales report
#[derive(Debug, Deserialize, Serialize, PartialEq, EnumString, Display)]
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
    /// Afternoon café, managed by LoLa
    Cafe,
    /// ("Vermietung") Room rentals by third parties, managed by LoLa
    Verm,
    /// ("Sommerfest" or summer party) Items sold during summer party
    SoFe,
    /// Key deposit
    Deposit,
    /// Rental fee
    Rental,
    /// Cultural Payments
    Culture,
    /// Paid out to external party in cash
    PaidOut,
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
    /// LoLa as main owner of sales by Mittags-Tisch
    LoLa,
    /// Mittags-Tisch as main owner of sales by Mittags-Tisch.
    MiTi,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::test_fixtures::{intermediate_df_01, sales_report_df_01, transaction_report_df_01};
    use crate::test_utils::assert_dataframe;

    use super::*;

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
}
