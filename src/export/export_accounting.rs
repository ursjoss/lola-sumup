use std::error::Error;

use polars::prelude::*;

use crate::export::posting::Posting;

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
                .alias(Posting::NET_CARD_TOTAL_MITI.column_name),
        )
        .with_column(
            (col("Tips_Card").fill_null(0.0) - col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias(Posting::TIPS_CARD_LOLA.column_name),
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
            col("Date"),
            col("Payment SumUp"),
            col("Total Cash Debit"),
            col("Total Card Debit"),
            col(Posting::DEPOSIT_CASH.column_name).alias(Posting::DEPOSIT_CASH.alias),
            col(Posting::CAFE_CASH.column_name).alias(Posting::CAFE_CASH.alias),
            col(Posting::VERM_CASH.column_name).alias(Posting::VERM_CASH.alias),
            col(Posting::SOFE_CASH.column_name).alias(Posting::SOFE_CASH.alias),
            col(Posting::RENTAL_CASH.column_name).alias(Posting::RENTAL_CASH.alias),
            col(Posting::CULTURE_CASH.column_name).alias(Posting::CULTURE_CASH.alias),
            col(Posting::PACKAGING_CASH.column_name).alias(Posting::PACKAGING_CASH.alias),
            col(Posting::PAIDOUT_CARD.column_name).alias(Posting::PAIDOUT_CARD.alias),
            col(Posting::DEPOSIT_CARD.column_name).alias(Posting::DEPOSIT_CARD.alias),
            col(Posting::CAFE_CARD.column_name).alias(Posting::CAFE_CARD.alias),
            col(Posting::VERM_CARD.column_name).alias(Posting::VERM_CARD.alias),
            col(Posting::SOFE_CARD.column_name).alias(Posting::SOFE_CARD.alias),
            col(Posting::RENTAL_CARD.column_name).alias(Posting::RENTAL_CARD.alias),
            col(Posting::CULTURE_CARD.column_name).alias(Posting::CULTURE_CARD.alias),
            col(Posting::PACKAGING_CARD.column_name).alias(Posting::PACKAGING_CARD.alias),
            col(Posting::NET_CARD_TOTAL_MITI.column_name).alias(Posting::NET_CARD_TOTAL_MITI.alias),
            col(Posting::TIPS_CARD_LOLA.column_name).alias(Posting::TIPS_CARD_LOLA.alias),
            col(Posting::LOLA_COMMISSION.column_name).alias(Posting::LOLA_COMMISSION.alias),
            col(Posting::SPONSORED_REDUCTIONS.column_name)
                .alias(Posting::SPONSORED_REDUCTIONS.alias),
            col(Posting::TOTAL_PRAKTIKUM.column_name).alias(Posting::TOTAL_PRAKTIKUM.alias),
            col(Posting::DEBT_TO_MITI.column_name).alias(Posting::DEBT_TO_MITI.alias),
            col(Posting::INCOME_LOLA_MITI.column_name).alias(Posting::INCOME_LOLA_MITI.alias),
            col(Posting::PAYMENT_TO_MITI.column_name).alias(Posting::PAYMENT_TO_MITI.alias),
        ])
        .collect()
}

pub fn validate_acc_constraint(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    validate_acc_constraint_10920(df_acc)?;
    validate_acc_constraint_20051(df_acc)
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_10920(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr = col(Posting::CAFE_CARD.alias)
        + col(Posting::VERM_CARD.alias)
        + col(Posting::SOFE_CARD.alias)
        + col(Posting::DEPOSIT_CARD.alias)
        + col(Posting::RENTAL_CARD.alias)
        + col(Posting::CULTURE_CARD.alias)
        + col(Posting::PACKAGING_CARD.alias)
        + col(Posting::NET_CARD_TOTAL_MITI.alias)
        + col(Posting::PAIDOUT_CARD.alias)
        + col(Posting::TIPS_CARD_LOLA.alias)
        - col("Payment SumUp")
        - col(Posting::LOLA_COMMISSION.alias);
    validate_constraint(df_acc, net_expr, "10920")?
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_20051(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr = col(Posting::NET_CARD_TOTAL_MITI.alias)
        - col(Posting::DEBT_TO_MITI.alias)
        - col(Posting::INCOME_LOLA_MITI.alias)
        + col(Posting::SPONSORED_REDUCTIONS.alias);
    validate_constraint(df_acc, net_expr, "20051")?
}

fn validate_constraint(
    df_acc: &DataFrame,
    net_expr: Expr,
    account: &str,
) -> Result<Result<(), Box<dyn Error>>, Box<dyn Error>> {
    let violations = df_acc
        .clone()
        .lazy()
        .with_column(net_expr.alias("Net"))
        .filter(col("Net").round(2).abs().gt(lit(0.05)))
        .collect()?;
    Ok(if violations.shape().0 > 0 {
        let row_vec = violations.get_row(0).unwrap().0;
        let date = row_vec.first().unwrap().clone();
        let net = row_vec.last().unwrap().clone();
        Err(format!("Constraint violation for accounting export on {date}: net value of account {account} is {net} instead of 0.0").into())
    } else {
        Ok(())
    })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use crate::test_fixtures::{accounting_df_03, summary_df_03};
    use crate::test_utils::assert_dataframe;

    use super::*;

    #[rstest]
    fn test_gather_df_accounting(summary_df_03: DataFrame, accounting_df_03: DataFrame) {
        let out =
            gather_df_accounting(&summary_df_03).expect("should be able to collect accounting_df");
        assert_dataframe(&out, &accounting_df_03);
    }

    #[rstest]
    // fully valid case
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    // still valid, as we accept a deviation of 0.05/-0.05
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 19.98, 12.0, 20.0, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.02, 12.0, 20.0, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.01, 20.0, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 11.99, 20.0, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.01, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 19.99, 10.0,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.01,
        100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 9.99, 100.0,
        500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0,
        100.01, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 99.99,
        500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.01, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        499.99, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.01, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 399.99, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.0, 100.01, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.0, 99.99, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.49, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.48, None, ""
    )]
    #[case(
        1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0,
        500.0, 400.0, 100.0, 189.25, 1.0, 17.99, 2.00, 0.0, 148.76, 42.50, None, ""
    )]
    // violations of account 10920
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.1,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.1,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.1,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.1,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.1,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.1,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.1,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.1,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0, 500.0, 400.0, 100.0, 188.15, 1.0, 17.99,
        2.0, 0.0, 148.76, 42.49, Some(-1.1), "10920")]
    #[case(1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0, 500.0, 400.0, 100.0, 189.25, 0.5, 17.99,
        2.0, 0.0, 148.76, 42.49, Some(-0.5), "10920")]
    #[case(
        1234.06,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.49,
        Some(100.2),
        "10920"
    )]
    #[case(1343.36, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99,
    2.0, 0.0, 148.76, 42.49, Some(-9.1), "10920")]
    // violations of account 20051
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.70,
        42.49,
        Some(0.06),
        "20051"
    )]
    #[case(
        1334.26,
        552.0,
        1162.0,
        0.0,
        0.0,
        10.0,
        20.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        10.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.99,
        2.0,
        0.0,
        148.76,
        42.43,
        Some(0.06),
        "20051"
    )]
    #[case(1334.26, 552.0, 1162.0, 0.0, 0.0, 10.0, 20.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 10.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.99,
    2.0, 0.0, 148.76, 42.55, Some(-0.06), "20051")]
    fn test_violations(
        #[case] payment_sumup: f64,
        #[case] total_cash_debit: f64,
        #[case] total_card_debit: f64,
        #[case] cafe_cash: f64,
        #[case] verm_cash: f64,
        #[case] sofe_cash: f64,
        #[case] deposit_cash: f64,
        #[case] rental_cash: f64,
        #[case] packaging_cash: f64,
        #[case] culture_cash: f64,
        #[case] cafe_card: f64,
        #[case] verm_card: f64,
        #[case] sofe_card: f64,
        #[case] deposit_card: f64,
        #[case] rental_card: f64,
        #[case] packaging_card: f64,
        #[case] culture_card: f64,
        #[case] paid_out_card: f64,
        #[case] net_card_income_plus_tips_miti_card: f64,
        #[case] tips_lola_paid_via_card: f64,
        #[case] commission_lola: f64,
        #[case] sponsored_reductions: f64,
        #[case] total_praktikum: f64,
        #[case] debt_to_miti: f64,
        #[case] income_lola_miti: f64,
        #[case] delta: Option<f64>,
        #[case] account: &str,
    ) -> PolarsResult<()> {
        let date = NaiveDate::parse_from_str("17.4.23", "%d.%m.%y").expect("valid date");
        let df = df!(
            "Date" => &[date],
            "Payment SumUp" => &[payment_sumup],
            "Total Cash Debit" => &[total_cash_debit],
            "Total Card Debit" => &[total_card_debit],
            "10000/23050" => &[deposit_cash],
            "10000/30200" => &[cafe_cash],
            "10000/30700" => &[verm_cash],
            "10000/30810" => &[sofe_cash],
            "10000/31000" => &[rental_cash],
            "10000/32000" => &[culture_cash],
            "10000/46000" => &[packaging_cash],
            "10920/10000" => &[paid_out_card],
            "10920/23050" => &[deposit_card],
            "10920/30200" => &[cafe_card],
            "10920/30700" => &[verm_card],
            "10920/30810" => &[sofe_card],
            "10920/31000" => &[rental_card],
            "10920/32000" => &[culture_card],
            "10920/46000" => &[packaging_card],
            "10920/20051" => &[net_card_income_plus_tips_miti_card],
            "10920/10910" => &[tips_lola_paid_via_card],
            "68450/10920" => &[Some(commission_lola)],
            "59991/20051" => &[sponsored_reductions],
            "59991/20120" => &[total_praktikum],
            "20051/10930" => &[debt_to_miti],
            "20051/30500" => &[income_lola_miti],
            "10930/10100" => &[debt_to_miti],
        )?;
        match validate_acc_constraint(&df) {
            Ok(()) => assert!(delta.is_none(), "Would not have expected delta {} on {date}.", delta.unwrap()),
            Err(e) => match delta {
                Some(d) => assert_eq!(
                    e.to_string(),
                    format!("Constraint violation for accounting export on {date}: net value of account {account} is {d} instead of 0.0")
                ),
                None => panic!("Would have expected delta on {date} but not in fixture: {e}"),
            },
        }

        Ok(())
    }
}
