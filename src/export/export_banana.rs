use polars::prelude::*;
use std::error::Error;

/// Produces the accounting dataframe from the summary [df] for import into banana
pub fn gather_df_banana(df: &DataFrame, month: &str) -> PolarsResult<DataFrame> {
    let last_of_month = get_last_of_month(month).expect("should be able to get last of month");
    let transposed = df
        .clone()
        .lazy()
        .filter(col("Date").is_null())
        .with_column(
            (col("Net Card Total").fill_null(0.0) + col("Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Payment SumUp"),
        )
        .with_column(
            (col("Net Card MiTi").fill_null(0.0) + col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Net Card Total MiTi"),
        )
        .with_column(
            (col("Tips_Card").fill_null(0.0) - col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Tips Card LoLa"),
        )
        .with_column(
            (col("Gross Cash").fill_null(0.0)
                - col("MiTi_Cash").fill_null(0.0)
                - col("PaidOut Total").fill_null(0.0))
            .round(2)
            .alias("Total Cash Debit"),
        )
        .with_column(
            (col("Gross Card LoLa").fill_null(0.0) + col("Tips_Card").fill_null(0.0)
                - col("MiTi_Tips_Card").fill_null(0.0))
            .round(2)
            .alias("Total Card Debit"),
        )
        .select([
            col("Deposit_Cash").alias("10000/23050"),
            col("Cafe_Cash").alias("10000/30200"),
            col("Verm_Cash").alias("10000/30700"),
            col("SoFe_Cash").alias("10000/30810"),
            col("Rental_Cash").alias("10000/31000"),
            col("Culture_Cash").alias("10000/32000"),
            col("Packaging_Cash").alias("10000/46000"),
            col("PaidOut_Card").alias("10920/10000"),
            col("Deposit_Card").alias("10920/23050"),
            col("Cafe_Card").alias("10920/30200"),
            col("Verm_Card").alias("10920/30700"),
            col("SoFe_Card").alias("10920/30810"),
            col("Rental_Card").alias("10920/31000"),
            col("Culture_Card").alias("10920/32000"),
            col("Packaging_Card").alias("10920/46000"),
            col("Net Card Total MiTi").alias("10920/20051"),
            col("Tips Card LoLa").alias("10920/10910"),
            col("LoLa_Commission").alias("68450/10920"),
            col("Sponsored Reductions").alias("59991/20051"),
            col("Total Praktikum").alias("59991/20120"),
            col("Debt to MiTi").alias("20051/10930"),
            col("Income LoLa MiTi").alias("20051/30500"),
            col("Debt to MiTi").alias("10930/10100"),
        ])
        .collect()?
        .transpose(Some("KtSoll/KtHaben"), None);
    transposed?
        .lazy()
        .with_column(
            when(col("KtSoll/KtHaben").eq(lit("10930/10100")))
                .then(lit(""))
                .otherwise(lit(last_of_month))
                .alias("Datum"),
        )
        .with_column(lit("Sumup").alias("Beschreibung"))
        .with_column(col("KtSoll/KtHaben").str().head(lit(5)).alias("KtSoll"))
        .with_column(col("KtSoll/KtHaben").str().tail(lit(5)).alias("KtHaben"))
        .with_column(col("column_0").round(2).alias("Betrag CHF"))
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
