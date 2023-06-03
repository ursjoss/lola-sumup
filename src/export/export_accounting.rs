use polars::prelude::*;

/// Produces the Accounting dataframe from the summary [df]
pub fn gather_df_accounting(df: &DataFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
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
        .select([
            col("Date"),
            col("Gross Card LoLa"),
            col("Net Card Total MiTi"),
            col("Payment SumUp"),
            col("LoLa_Commission").alias("Commission LoLa"),
        ])
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_gather_df_accounting() {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df_summary = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[Some(112.0)],
            "MiTi_Card" => &[Some(191.0 )],
            "MiTi Total" => &[303.0],
            "Cafe_Cash" => &[Some(29.5)],
            "Cafe_Card" => &[Some(20)],
            "Cafe Total" => &[49.5],
            "Verm_Cash" => &[Some(102.5)],
            "Verm_Card" => &[Some(12.0)],
            "Verm Total" => &[114.5],
            "Gross Cash" => &[244.0],
            "Tips_Cash" => &[Some(4.0)],
            "Sumup Cash" => &[248.0],
            "Gross Card" => &[223.0],
            "Tips_Card" => &[Some(1.0)],
            "Sumup Card" => &[224.0],
            "Gross Total" => &[467.0],
            "Tips Total" => &[5.0],
            "SumUp Total" => &[472.0],
            "Gross Card MiTi" => &[191.0],
            "MiTi_Commission" => &[Some(2.75)],
            "Net Card MiTi" => &[188.25],
            "Gross Card LoLa" => &[32.0],
            "LoLa_Commission" => &[1.04],
            "LoLa_Commission_MiTi" => &[0.44],
            "Net Card LoLa" => &[30.96],
            "Gross Card Total" => &[223],
            "Total Commission" => &[3.79],
            "Net Card Total" => &[219.21],
            "MiTi_Tips_Cash" => &[Some(0.5)],
            "MiTi_Tips_Card" => &[Some(1.0)],
            "MiTi_Tips" => &[Some(1.5)],
            "Cafe_Tips" => &[Some(3.5)],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(250.0)],
            "Gross MiTi (LoLa)" => &[Some(53.0)],
            "Gross MiTi (MiTi) Card" => &[Some(167.0)],
            "Net MiTi (MiTi) Card" => &[164.25],
            "Contribution LoLa" => &[10.51],
            "Credit MiTi" => &[175.76],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "Gross Card LoLa" => &[32.0],
            "Net Card Total MiTi" => &[189.25],
            "Payment SumUp" => &[220.21],
            "Commission LoLa" => &[Some(1.04)],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out =
            gather_df_accounting(&df_summary).expect("should be able to collect accounting_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
