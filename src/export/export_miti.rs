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

    use crate::test_fixtures::{sample_date, summary_df_04};
    use crate::test_utils::assert_dataframe;

    use super::*;

    #[rstest]
    fn test_gather_df_miti(summary_df_04: DataFrame, sample_date: NaiveDate) -> PolarsResult<()> {
        let expected = df!(
            "Datum" => &[sample_date],
            "Hauptgang" => &[14],
            "Kind" => &[1],
            "Küche" => &[Some(250.0)],
            "Total Bar" => &[Some(53.0)],
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
        )?;
        let out = gather_df_miti(&summary_df_04).expect("should be able to collect miti_df");

        assert_dataframe(&out, &expected);

        Ok(())
    }
}
