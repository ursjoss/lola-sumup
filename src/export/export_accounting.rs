use std::error::Error;

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
        .with_column(
            (col("PaidOut_Card").fill_null(0.0) + col("Tips_Card").fill_null(0.0)
                - col("MiTi_Tips_Card").fill_null(0.0))
            .round(2)
            .alias("PaidOut + Tips Card LoLa"),
        )
        .select([
            col("Date"),
            col("Cafe_Cash").alias("10000/30200"),
            col("Verm_Cash").alias("10000/30700"),
            col("SoFe_Cash").alias("10000/30800"),
            col("Deposit_Cash").alias("10000/23050"),
            col("Rental_Cash").alias("10000/31X00"),
            col("Culture_Cash").alias("10000/32000"),
            col("Cafe_Card").alias("10920/30200"),
            col("Verm_Card").alias("10920/30700"),
            col("SoFe_Card").alias("10920/30800"),
            col("Deposit_Card").alias("10920/23050"),
            col("Rental_Card").alias("10920/31X00"),
            col("Culture_Card").alias("10920/32000"),
            col("Net Card Total MiTi").alias("10920/20051"),
            col("PaidOut + Tips Card LoLa").alias("10920/10910"),
            col("LoLa_Commission").alias("68450/10920"),
            col("Debt to MiTi").alias("20051/10900"),
            col("Income LoLa MiTi").alias("20051/30500"),
            col("Payment SumUp"),
        ])
        .collect()
}

pub fn validate_acc_constraint(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    validate_acc_constraint_10920(df_acc)?;
    validate_acc_constraint_20051(df_acc)
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_10920(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr = col("10920/30200")
        + col("10920/30700")
        + col("10920/30800")
        + col("10920/23050")
        + col("10920/31X00")
        + col("10920/32000")
        + col("10920/20051")
        + col("10920/10910")
        - col("Payment SumUp")
        - col("68450/10920");
    validate_constraint(df_acc, net_expr, "10920")?
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_20051(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr = col("10920/20051") - col("20051/10900") - col("20051/30500");
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
        .filter(col("Net").round(2).abs().gt(lit(0.02)))
        .collect()?;
    Ok(if violations.shape().0 > 0 {
        let row_vec = violations.get_row(0).unwrap().0;
        let date = row_vec.get(0).unwrap().clone();
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
    use pretty_assertions::assert_eq;
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
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    // still valid, as we accept a deviation of 0.02/-0.02
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 19.98, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.02, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.01, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 11.99, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.01, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 19.99, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.01, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 99.99, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.01, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 499.99, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.01, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 399.99, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.01, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 99.99, 189.25, 1.0,
        17.84, 146.76, 42.49, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.48, 1324.41, None, ""
    )]
    #[case(
        0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0,
        17.84, 146.76, 42.50, 1324.41, None, ""
    )]
    // violations of account 10920
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.1,
        12.0,
        20.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.1,
        20.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.1,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.1,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.1,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.0,
        400.1,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.0,
        400.0,
        100.1,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1324.41,
        Some(0.1),
        "10920"
    )]
    #[case(0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 188.15, 1.0, 17.84, 146.76, 42.49, 1324.41, Some(-1.1), "10920")]
    #[case(0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 0.5, 17.84, 146.76, 42.49, 1324.41, Some(-0.5), "10920")]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.49,
        1224.21,
        Some(100.2),
        "10920"
    )]
    #[case(0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.84, 146.76, 42.49, 1333.51, Some(-9.1), "10920")]
    // violations of account 20051
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.73,
        42.49,
        1324.41,
        Some(0.03),
        "20051"
    )]
    #[case(
        0.0,
        0.0,
        10.0,
        0.0,
        0.0,
        0.0,
        20.0,
        12.0,
        20.0,
        100.0,
        500.0,
        400.0,
        100.0,
        189.25,
        1.0,
        17.84,
        146.76,
        42.46,
        1324.41,
        Some(0.03),
        "20051"
    )]
    #[case(0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 20.0, 12.0, 20.0, 100.0, 500.0, 400.0, 100.0, 189.25, 1.0, 17.84, 146.76, 42.52, 1324.41, Some(-0.03), "20051")]
    fn test_violations(
        #[case] cafe_cash: f64,
        #[case] verm_cash: f64,
        #[case] sofe_cash: f64,
        #[case] deposit_cash: f64,
        #[case] rental_cash: f64,
        #[case] culture_cash: f64,
        #[case] cafe_card: f64,
        #[case] verm_card: f64,
        #[case] sofe_card: f64,
        #[case] deposit_card: f64,
        #[case] rental_card: f64,
        #[case] culture_card: f64,
        #[case] paid_out_card: f64,
        #[case] net_card_income_plus_tips_miti_card: f64,
        #[case] tips_lola_paid_via_card: f64,
        #[case] commission_lola: f64,
        #[case] debt_to_miti: f64,
        #[case] income_lola_miti: f64,
        #[case] payment_sumup: f64,
        #[case] delta: Option<f64>,
        #[case] account: &str,
    ) -> PolarsResult<()> {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df = df!(
            "Date" => &[date],
            "10000/30200" => &[cafe_cash],
            "10000/30700" => &[verm_cash],
            "10000/30800" => &[sofe_cash],
            "10000/23050" => &[deposit_cash],
            "10000/31X00" => &[rental_cash],
            "10000/32000" => &[culture_cash],
            "10920/30200" => &[cafe_card],
            "10920/30700" => &[verm_card],
            "10920/30800" => &[sofe_card],
            "10920/23050" => &[deposit_card],
            "10920/31X00" => &[rental_card],
            "10920/32000" => &[culture_card],
            "10920/20051" => &[net_card_income_plus_tips_miti_card],
            "10920/10910" => &[paid_out_card + tips_lola_paid_via_card],
            "68450/10920" => &[Some(commission_lola)],
            "20051/10900" => &[debt_to_miti],
            "20051/30500" => &[income_lola_miti],
            "Payment SumUp" => &[payment_sumup],
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
