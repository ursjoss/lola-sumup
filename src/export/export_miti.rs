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
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) * lit(0.8))
                .round(2)
                .alias("Gross MiTi LoLa (LoLa)"),
        )
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) * lit(0.2))
                .round(2)
                .alias("Gross MiTi LoLa (MiTi)"),
        )
        .with_column(
            (col("Net MiTi (LoLA) - Share LoLa").fill_null(0.0) * lit(-1))
                .round(2)
                .alias("Verkauf LoLa (80%)"),
        )
        .select([
            col("Date").alias("Datum"),
            col("MealCount_Regular").alias("Hauptgang"),
            col("MealCount_Children").alias("Kind"),
            col("Gross MiTi (MiTi)").alias("Küche"),
            col("Gross MiTi (LoLa)").alias("Total Bar"),
            col("Gross MiTi LoLa (LoLa)").alias("Anteil LoLa"),
            col("Gross MiTi LoLa (MiTi)").alias("Anteil MiTi"),
            col("Total Cash").alias("Einnahmen barz."),
            col("MiTi_Tips_Cash").alias("davon TG barz."),
            col("Total Card").alias("Einnahmen Karte"),
            col("MiTi_Tips_Card").alias("davon TG Karte"),
            col("MiTi Total").alias("Total Einnahmen (oT)"),
            col("LoLa_Commission_MiTi").alias("Kommission Bar"),
            col("Net Income LoLa").alias("Netto Bar"),
            col("Gross MiTi (MiTi) Card").alias("Karte MiTi"),
            col("MiTi_Commission").alias("Kommission MiTi"),
            col("Net MiTi (MiTi) Card").alias("Netto Karte MiTi"),
            col("Net Payment SumUp MiTi").alias("Net Total Karte"),
            col("Verkauf LoLa (80%)"),
            col("Debt to MiTi").alias("Überweisung"),
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
        let date = NaiveDate::parse_from_str("24.3.2023", "%d.%m.%Y").expect("valid date");
        let df_summary = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[Some(112.0)],
            "MiTi_Card" => &[Some(191.0)],
            "MiTi Total" => &[303.0],
            "Cafe_Cash" => &[Some(29.5)],
            "Cafe_Card" => &[Some(20)],
            "Cafe Total" => &[49.5],
            "Verm_Cash" => &[Some(102.5)],
            "Verm_Card" => &[Some(12.0)],
            "Verm Total" => &[114.5],
            "Deposit_Cash" => &[None::<f64>],
            "Deposit_Card" => &[Some(100.0)],
            "Deposit Total" => &[100.0],
            "Rental_Cash" => &[None::<f64>],
            "Rental_Card" => &[Some(500.0)],
            "Rental Total" => &[500.0],
            "Culture_Cash" => &[None::<f64>],
            "Culture_Card" => &[400.0],
            "Culture Total" => &[400.0],
            "Gross Cash" => &[644.0],
            "Tips_Cash" => &[Some(4.0)],
            "SumUp Cash" => &[248.0],
            "Gross Card" => &[1223.0],
            "Tips_Card" => &[Some(1.0)],
            "SumUp Card" => &[1224.0],
            "Gross Total" => &[1467.0],
            "Tips Total" => &[5.0],
            "SumUp Total" => &[1472.0],
            "Gross Card MiTi" => &[191.0],
            "MiTi_Commission" => &[Some(2.75)],
            "Net Card MiTi" => &[188.25],
            "Gross Card LoLa" => &[1032.0],
            "LoLa_Commission" => &[16.04],
            "LoLa_Commission_MiTi" => &[0.44],
            "Net Card LoLa" => &[129.46],
            "Gross Card Total" => &[1015.56],
            "Total Commission" => &[18.79],
            "Net Card Total" => &[966.77],
            "Net Payment SumUp MiTi" => &[187.81],
            "MiTi_Tips_Cash" => &[Some(0.5)],
            "MiTi_Tips_Card" => &[Some(1.0)],
            "MiTi_Tips" => &[Some(1.5)],
            "Cafe_Tips" => &[Some(3.5)],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(250.0)],
            "Gross MiTi (LoLa)" => &[Some(53.0)],
            "Gross MiTi (MiTi) Card" => &[Some(167.0)],
            "Net MiTi (MiTi) Card" => &[164.25],
            "Net MiTi (LoLa)" => &[52.56],
            "Contribution MiTi" => &[10.51],
            "Net MiTi (LoLA) - Share LoLa" => &[42.05],
            "Debt to MiTi" => &[145.76],
            "Income LoLa MiTi" => &[42.49],
            "MealCount_Regular" => &[14],
            "MealCount_Children" => &[1],
        )
        .expect("valid data frame");
        let expected = df!(
            "Datum" => &[date],
            "Hauptgang" => &[14],
            "Kind" => &[1],
            "Küche" => &[Some(250.0)],
            "Total Bar" => &[Some(53)],
            "Anteil LoLa" => &[Some(42.4)],
            "Anteil MiTi" => &[Some(10.6)],
            "Einnahmen barz." => &[112.5],
            "davon TG barz." => &[Some(0.5)],
            "Einnahmen Karte" => &[192.0],
            "davon TG Karte" => &[Some(1.0)],
            "Total Einnahmen (oT)" => &[303.0],
            "Kommission Bar" => &[0.44],
            "Netto Bar" => &[52.56],
            "Karte MiTi" => &[Some(167.0)],
            "Kommission MiTi" => &[2.75],
            "Netto Karte MiTi" => &[164.25],
            "Net Total Karte" => &[187.81],
            "Verkauf LoLa (80%)" => &[-42.05],
            "Überweisung" => &[145.76],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out = gather_df_miti(&df_summary).expect("should be able to collect miti_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
