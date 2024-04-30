use polars::prelude::*;

/// Produces the Mittagstisch dataframe from the summary [df]
pub fn gather_df_miti(df: &DataFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .with_column(
            (col("MealCount_Reduced").fill_null(0) + col("MealCount_Praktikum").fill_null(0))
                .alias("MealCount_ReducedPraktikum"),
        )
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
            col("MealCount_ReducedPraktikum").alias("Reduziert"),
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
            col("Sponsored Reductions").alias("Gesponsort"),
            col("Debt to MiTi").alias("Überweisung"),
        ])
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::test_fixtures::{miti_df_03, summary_df_03};
    use crate::test_utils::assert_dataframe;

    use super::*;

    #[rstest]
    fn test_gather_df_miti(summary_df_03: DataFrame, miti_df_03: DataFrame) {
        let out = gather_df_miti(&summary_df_03).expect("should be able to collect miti_df");
        assert_dataframe(&out, &miti_df_03);
    }
}
