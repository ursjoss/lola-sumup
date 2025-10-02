use super::posting::Posting;
use crate::prepare::{PaymentMethod, Topic};
use polars::prelude::*;
use std::error::Error;

/// Finds the descriptions for the different posting types
fn get_description(col: &Column) -> PolarsResult<Column> {
    let accounts = col.str()?;
    Ok(accounts
        .into_iter()
        .map(|a| {
            a.map(|a| Posting::from_alias(a).map(|p| format!("SU {}", p.description)))
                .or(None)?
        })
        .collect::<StringChunked>()
        .into_column())
}

/// Produces the accounting dataframe from the accounting [df] for import into banana
pub fn gather_df_banana(df_acct: &DataFrame, month: &str) -> PolarsResult<DataFrame> {
    let date_format = StrptimeOptions {
        format: Some("%d.%m.%Y".into()),
        strict: false,
        exact: true,
        ..Default::default()
    };
    let last_of_month = get_last_of_month(month).expect("should be able to get last of month");
    df_acct
        .clone()
        .lazy()
        .filter(col("Date").is_null())
        .with_column(lit(0.0).alias("Date"))
        .collect()?
        .transpose(Some("KtSoll/KtHaben"), None)?
        .lazy()
        .filter(col("KtSoll/KtHaben").str().contains(lit("/"), false))
        .with_column(
            when(col("KtSoll/KtHaben").eq(lit(Posting::PAYMENT_TO_MITI.alias)))
                .then(lit("").str().to_date(date_format.clone()))
                .otherwise(lit(last_of_month).str().to_date(date_format))
                .alias("Datum"),
        )
        .with_column(
            col("KtSoll/KtHaben")
                .map(
                    |kti| get_description(&kti),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
                )
                .alias("Beschreibung"),
        )
        .with_column(col("KtSoll/KtHaben").str().head(lit(5)).alias("KtSoll"))
        .with_column(col("KtSoll/KtHaben").str().tail(lit(5)).alias("KtHaben"))
        .with_column(
            col("column_0")
                .round(2, RoundMode::HalfToEven)
                .alias("Betrag CHF"),
        )
        .filter(col("Betrag CHF").neq(lit(0.0)))
        .select([
            col("Datum"),
            lit("").alias("Beleg"),
            lit("").alias("Rechnung"),
            col("Beschreibung"),
            col("KtSoll"),
            col("KtHaben"),
            lit("").alias("Anzahl"),
            lit("").alias("Einheit"),
            lit("").alias("Preis/Einheit"),
            col("Betrag CHF"),
        ])
        .collect()
}

fn get_last_of_month(month: &str) -> Result<String, Box<dyn Error>> {
    let year = month.get(0..4).unwrap();
    let year = year.parse::<i32>()?;
    let month = month.get(4..6).unwrap();
    let month = month.parse::<u32>()?;
    let last_of_month = -chrono::NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or("Invalid month 1")?
        .signed_duration_since(if month < 12 {
            chrono::NaiveDate::from_ymd_opt(year, month + 1, 1).ok_or("Invalid month 2")?
        } else {
            chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1).ok_or("Invalid month 3")?
        })
        .num_days();
    Ok(format!("{last_of_month}.{month:02}.{year}"))
}

/// Gathers the detail transactions that need to be reported individually in the banana export
/// - Rental postings to be posted into 31000
pub fn gather_df_banana_details(raw_df: &DataFrame) -> PolarsResult<DataFrame> {
    raw_df
        .clone()
        .lazy()
        .filter(col("Topic").eq(lit(Topic::Rental.to_string())))
        .with_column(
            when(col("Payment Method").eq(lit(PaymentMethod::Card.to_string())))
                .then(lit("10920"))
                .otherwise(lit("10000"))
                .alias("KtSoll"),
        )
        .select([
            col("Date").alias("Datum"),
            lit("").alias("Beleg"),
            lit("").alias("Rechnung"),
            col("Description").alias("Beschreibung"),
            col("KtSoll"),
            lit("31000").alias("KtHaben"),
            lit("").alias("Anzahl"),
            lit("").alias("Einheit"),
            lit("").alias("Preis/Einheit"),
            col("Price (Net)").alias("Betrag CHF"),
        ])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{accounting_df_06, banana_df_06};
    use crate::test_utils::assert_dataframe;
    use rstest::rstest;

    #[rstest]
    #[case("202411", "30.11.2024")]
    #[case("202501", "31.01.2025")]
    #[case("202502", "28.02.2025")]
    fn test_get_last_of_moth(#[case] month: &str, #[case] expected: &str) {
        let lom = get_last_of_month(month).expect("should be able to get last of month");
        assert_eq!(lom, expected);
    }

    #[rstest]
    fn test_gather_df_banana06(accounting_df_06: DataFrame, banana_df_06: DataFrame) {
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
        let out =
            gather_df_banana(&acct_df_ext, "202303").expect("should be able to collect banana_df");
        assert_dataframe(&out, &banana_df_06);
    }
}
