use polars::prelude::*;

/// Produces the Mittagstisch dataframe from the summary [df]
pub fn gather_df_miti(df: &DataFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .with_column(
            (col("MiTi_Cash").fill_null(0.0) + col("MiTi_Tips_Cash").fill_null(0.0))
                .round(2)
                .alias("Total Cash"),
        )
        .with_column(
            (col("MiTi_Card").fill_null(0.0) + col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Total Card"),
        )
        .with_column(
            (col("MiTi_Tips_Cash").fill_null(0.0) + col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Tips Total"),
        )
        .with_column(
            (col("MiTi Total").fill_null(0.0) + col("Tips Total").fill_null(0.0))
                .round(2)
                .alias("Payment Total"),
        )
        .select([
            col("Date"),
            col("MiTi_Cash").alias("Income Cash"),
            col("MiTi_Tips_Cash").alias("Tips Cash"),
            col("Total Cash"),
            col("MiTi_Card").alias("Income Card"),
            col("MiTi_Tips_Card").alias("Tips Card"),
            col("Total Card"),
            col("MiTi Total").alias("Income Total"),
            col("Tips Total"),
            col("Payment Total"),
            col("Gross MiTi (MiTi)").alias("Gross Income MiTi"),
            col("Gross MiTi (LoLa)").alias("Gross Income LoLa"),
            col("Gross MiTi (MiTi) Card").alias("Gross Card MiTi"),
            col("MiTi_Commission").alias("Commission MiTi"),
            col("Net MiTi (MiTi) Card").alias("Net Card MiTi"),
            col("Contribution LoLa"),
            col("Credit MiTi").alias("Credit"),
        ])
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_gather_df_miti() {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df_summary = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[None::<f64>],
            "MiTi_Card" => &[Some(16.0)],
            "MiTi Total" => &[16.0],
            "Cafe_Cash" => &[None::<f64>],
            "Cafe_Card" => &[None::<f64>],
            "Cafe Total" => &[0.0],
            "Verm_Cash" => &[None::<f64>],
            "Verm_Card" => &[None::<f64>],
            "Verm Total" => &[0.0],
            "Gross Cash" => &[0.0],
            "Tips_Cash" => &[None::<f64>],
            "Sumup Cash" => &[0.0],
            "Gross Card" => &[16.0],
            "Tips_Card" => &[None::<f64>],
            "Sumup Card" => &[16.0],
            "Gross Total" => &[16.0],
            "Tips Total" => &[0.0],
            "SumUp Total" => &[16.0],
            "Gross Card MiTi" => &[16.0],
            "MiTi_Commission" => &[Some(0.24)],
            "Net Card MiTi" => &[15.76],
            "Gross Card LoLa" => &[0.0],
            "LoLa_Commission" => &[None::<f64>],
            "LoLa_Commission_MiTi" => &[None::<f64>],
            "Net Card LoLa" => &[0.0],
            "Gross Card Total" => &[16.0],
            "Total Commission" => &[0.24],
            "Net Card Total" => &[15.76],
            "MiTi_Tips_Cash" => &[None::<f64>],
            "MiTi_Tips_Card" => &[None::<f64>],
            "MiTi_Tips" => &[None::<f64>],
            "Cafe_Tips" => &[None::<f64>],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(16.0)],
            "Gross MiTi (LoLa)" => &[None::<f64>],
            "Gross MiTi (MiTi) Card" => &[Some(16.0)],
            "Net MiTi (MiTi) Card" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit MiTi" => &[15.76],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "Income Cash" => &[None::<f64>],
            "Tips Cash" => &[None::<f64>],
            "Total Cash" => &[0.0],
            "Income Card" => &[Some(16.0)],
            "Tips Card" => &[None::<f64>],
            "Total Card" => &[16.0],
            "Income Total" => &[16.0],
            "Tips Total" => &[0.0],
            "Payment Total" => &[16.0],
            "Gross Income MiTi" => &[Some(16.0)],
            "Gross Income LoLa" => &[None::<f64>],
            "Gross Card MiTi" => &[Some(16.0)],
            "Commission MiTi" => &[0.24],
            "Net Card MiTi" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit" => &[15.76],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out = gather_df_miti(&df_summary).expect("should be able to collect miti_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
