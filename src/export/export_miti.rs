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
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) - col("LoLa_Commission_MiTi").fill_null(0.0))
                .round(2)
                .alias("Net Income LoLa"),
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
            col("LoLa_Commission_MiTi").alias("Commission LoLa"),
            col("Net Income LoLa"),
            col("Gross MiTi (MiTi) Card").alias("Gross Card MiTi"),
            col("MiTi_Commission").alias("Commission MiTi"),
            col("Net MiTi (MiTi) Card").alias("Net Card MiTi"),
            col("Contribution LoLa"),
            col("MiTi_Tips_Card").alias("Tips Due"),
            col("Credit MiTi").alias("Total Payment Due"),
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
            "Income Cash" => &[Some(112.0)],
            "Tips Cash" => &[Some(0.5)],
            "Total Cash" => &[112.5],
            "Income Card" => &[Some(191.0)],
            "Tips Card" => &[Some(1.0)],
            "Total Card" => &[192.0],
            "Income Total" => &[303.0],
            "Tips Total" => &[1.5],
            "Payment Total" => &[304.5],
            "Gross Income MiTi" => &[Some(250.0)],
            "Gross Income LoLa" => &[Some(53)],
            "Commission LoLa" => &[0.44],
            "Net Income LoLa" => &[52.56],
            "Gross Card MiTi" => &[Some(167.0)],
            "Commission MiTi" => &[2.75],
            "Net Card MiTi" => &[164.25],
            "Contribution LoLa" => &[10.51],
            "Tips Due" => &[Some(1.0)],
            "Total Payment Due" => &[175.76],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out = gather_df_miti(&df_summary).expect("should be able to collect miti_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
