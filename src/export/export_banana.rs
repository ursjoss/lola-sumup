use polars::prelude::*;
use std::error::Error;

use super::posting::Posting;

/// Finds the descriptions for the different posting types
fn get_description(col: &Column) -> PolarsResult<Option<Column>> {
    let accounts = col.str()?;
    Ok(Some(
        accounts
            .into_iter()
            .map(|a| {
                a.map(|a| Posting::from_alias(a).map(|p| format!("SU {}", p.description)))
                    .or(None)?
            })
            .collect::<StringChunked>()
            .into_column(),
    ))
}

/// Produces the accounting dataframe from the summary [df] for import into banana
pub fn gather_df_banana(df_acct: &DataFrame, month: &str) -> PolarsResult<DataFrame> {
    let last_of_month = get_last_of_month(month).expect("should be able to get last of month");
    df_acct
        .clone()
        .lazy()
        .filter(col("Date").is_null())
        .collect()?
        .transpose(Some("KtSoll/KtHaben"), None)?
        .lazy()
        .filter(col("KtSoll/KtHaben").str().contains(lit("/"), false))
        .with_column(
            when(col("KtSoll/KtHaben").eq(lit(Posting::PAYMENT_TO_MITI.alias)))
                .then(lit(""))
                .otherwise(lit(last_of_month))
                .alias("Datum"),
        )
        .with_column(
            col("KtSoll/KtHaben")
                .apply(
                    |kti| get_description(&kti),
                    GetOutput::from_type(DataType::String),
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
            lit("").alias("Rechnung"),
            col("Beschreibung"),
            col("KtSoll"),
            col("KtHaben"),
            lit("").alias("Anzahl"),
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("202411", "30.11.2024")]
    #[case("202501", "31.01.2025")]
    #[case("202502", "28.02.2025")]
    fn test_get_last_of_moth(#[case] month: &str, #[case] expected: &str) {
        let lom = get_last_of_month(month).expect("should be able to get last of month");
        assert_eq!(lom, expected);
    }
}
