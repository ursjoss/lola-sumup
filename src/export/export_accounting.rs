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
            (col("Tips_Card").fill_null(0.0) - col("MiTi_Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Tips Card LoLa"),
        )
        .select([
            col("Date"),
            col("Cafe_Card").alias("10920/30200"),
            col("Verm_Card").alias("10920/30700"),
            col("Deposit_Card").alias("10920/23050"),
            col("Net Card Total MiTi").alias("10920/20051"),
            col("Tips Card LoLa").alias("10920/10910"),
            col("Payment SumUp"),
            col("LoLa_Commission").alias("68450/10920"),
            col("Debt to MiTi").alias("20051/10900"),
            col("Income LoLa MiTi").alias("20051/30500"),
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
        + col("10920/23050")
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
        .filter(col("Net").round(2).abs().gt(lit(0.01)))
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
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_gather_df_accounting() {
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
            "Gross Cash" => &[244.0],
            "Tips_Cash" => &[Some(4.0)],
            "SumUp Cash" => &[248.0],
            "Gross Card" => &[323.0],
            "Tips_Card" => &[Some(1.0)],
            "SumUp Card" => &[324.0],
            "Gross Total" => &[567.0],
            "Tips Total" => &[5.0],
            "SumUp Total" => &[572.0],
            "Gross Card MiTi" => &[191.0],
            "MiTi_Commission" => &[Some(2.75)],
            "Net Card MiTi" => &[188.25],
            "Gross Card LoLa" => &[132.0],
            "LoLa_Commission" => &[2.54],
            "LoLa_Commission_MiTi" => &[0.44],
            "Net Card LoLa" => &[129.46],
            "Gross Card Total" => &[323],
            "Total Commission" => &[5.29],
            "Net Card Total" => &[219.21],
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
            "Net MiTi (LoLA) - Share LoLa" => &[52.56],
            "Debt to MiTi" => &[145.76],
            "Income LoLa MiTi" => &[42.49],
            "MealCount_Regular" => &[14],
            "MealCount_Children" => &[1],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "10920/30200" => &[20.0],
            "10920/30700" => &[12.0],
            "10920/23050" => &[100.0],
            "10920/20051" => &[189.25],
            "10920/10910" => &[0.0],
            "Payment SumUp" => &[220.21],
            "68450/10920" => &[Some(2.54)],
            "20051/10900" => &[145.76],
            "20051/30500" => &[42.49],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out =
            gather_df_accounting(&df_summary).expect("should be able to collect accounting_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }

    #[rstest]
    // fully valid case
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.49, None, "")]
    // still valid, as we accept a deviation of 0.1/-0.1
    #[case(19.99, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.49, None, "")]
    #[case(20.01, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.49, None, "")]
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.48, None, "")]
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.50, None, "")]
    // violations of account 10920
    #[case(
        20.1,
        12.0,
        100.0,
        189.25,
        1.0,
        319.71,
        2.54,
        146.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        20.0,
        12.1,
        100.0,
        189.25,
        1.0,
        319.71,
        2.54,
        146.76,
        42.49,
        Some(0.1),
        "10920"
    )]
    #[case(20.0, 12.0, 100.0, 188.15, 1.0, 319.71, 2.54, 146.76, 42.49, Some(-1.1), "10920")]
    #[case(20.0, 12.0, 100.0, 189.25, 0.5, 319.71, 2.54, 146.76, 42.49, Some(-0.5), "10920")]
    #[case(
        20.0,
        12.0,
        100.0,
        189.25,
        1.0,
        219.51,
        2.54,
        146.76,
        42.49,
        Some(100.2),
        "10920"
    )]
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 11.64, 146.76, 42.49, Some(-9.1), "10920")]
    // violations of account 20051
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.49, None, "")]
    #[case(
        20.0,
        12.0,
        100.0,
        189.25,
        1.0,
        319.71,
        2.54,
        146.74,
        42.49,
        Some(0.02),
        "20051"
    )]
    #[case(
        20.0,
        12.0,
        100.0,
        189.25,
        1.0,
        319.71,
        2.54,
        146.76,
        42.47,
        Some(0.02),
        "20051"
    )]
    #[case(20.0, 12.0, 100.0, 189.25, 1.0, 319.71, 2.54, 146.76, 42.51, Some(-0.02), "20051")]
    fn test_violations(
        #[case] cc: f64,
        #[case] vc: f64,
        #[case] dc: f64,
        #[case] nctm: f64,
        #[case] tcl: f64,
        #[case] psu: f64,
        #[case] cl: f64,
        #[case] dtm: f64,
        #[case] ilm: f64,
        #[case] delta: Option<f64>,
        #[case] account: &str,
    ) {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df = df!(
            "Date" => &[date],
            "10920/30200" => &[cc],
            "10920/30700" => &[vc],
            "10920/23050" => &[dc],
            "10920/20051" => &[nctm],
            "10920/10910" => &[tcl],
            "Payment SumUp" => &[psu],
            "68450/10920" => &[Some(cl)],
            "20051/10900" => &[dtm],
            "20051/30500" => &[ilm],
        )
        .expect("valid data frame");
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
    }
}
