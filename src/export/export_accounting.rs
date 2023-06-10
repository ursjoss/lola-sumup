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
            col("Net Card Total MiTi").alias("10920/20051"),
            col("Tips Card LoLa").alias("10920/10910"),
            col("Payment SumUp"),
            col("LoLa_Commission").alias("68450/10920"),
            col("Debt to MiTi").alias("20051/10900"),
            col("Income LoLa MiTi").alias("20051/30200"),
        ])
        .collect()
}

pub fn validate_acc_constraint(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    validate_acc_constraint_10920(df_acc)?;
    validate_acc_constraint_20051(df_acc)
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_10920(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr =
        col("10920/30200") + col("10920/30700") + col("10920/20051") + col("10920/10910")
            - col("Payment SumUp")
            - col("68450/10920");
    validate_constraint(df_acc, net_expr, "10920")?
}

/// validates the transitory account 10920 nets to 0
fn validate_acc_constraint_20051(df_acc: &DataFrame) -> Result<(), Box<dyn Error>> {
    let net_expr = col("10920/20051") - col("20051/10900") - col("20051/30200");
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
        .filter(col("Net").round(2).neq(lit(0.0)))
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
            "Net MiTi (LoLa) Card" => &[24.0],
            "Net MiTi (LoLa)" => &[52.56],
            "Contribution LoLa" => &[42.04],
            "Contribution MiTi" => &[10.51],
            "Debt to MiTi" => &[175.76],
            "Income LoLa MiTi" => &[13.49],
            "MealCount_Regular" => &[14],
            "MealCount_Children" => &[3],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "10920/30200" => &[20.0],
            "10920/30700" => &[12.0],
            "10920/20051" => &[189.25],
            "10920/10910" => &[0.0],
            "Payment SumUp" => &[220.21],
            "68450/10920" => &[Some(1.04)],
            "20051/10900" => &[175.76],
            "20051/30200" => &[13.49],
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
    #[case(20.0, 12.0, 189.25, 1.0, 221.21, 1.04, 174.76, 14.49, None, "")]
    // violations of account 10920
    #[case(
        20.1,
        12.0,
        189.25,
        1.0,
        221.21,
        1.04,
        174.76,
        14.49,
        Some(0.1),
        "10920"
    )]
    #[case(
        20.0,
        12.1,
        189.25,
        1.0,
        221.21,
        1.04,
        174.76,
        14.49,
        Some(0.1),
        "10920"
    )]
    #[case(20.0, 12.0, 188.15, 1.0, 221.21, 1.04, 174.76, 14.49, Some(-1.1), "10920")]
    #[case(20.0, 12.0, 189.25, 0.5, 221.21, 1.04, 174.76, 14.49, Some(-0.5), "10920")]
    #[case(
        20.0,
        12.0,
        189.25,
        1.0,
        121.01,
        1.04,
        174.76,
        14.49,
        Some(100.2),
        "10920"
    )]
    #[case(20.0, 12.0, 189.25, 1.0, 221.21, 10.14, 174.76, 14.49, Some(-9.1), "10920")]
    // violations of account 20051
    #[case(
        20.0,
        12.0,
        189.25,
        1.0,
        221.21,
        1.04,
        174.75,
        14.49,
        Some(0.01),
        "20051"
    )]
    #[case(20.0, 12.0, 189.25, 1.0, 221.21, 1.04, 174.77, 14.49, Some(-0.01), "20051")]
    #[case(
        20.0,
        12.0,
        189.25,
        1.0,
        221.21,
        1.04,
        174.76,
        14.48,
        Some(0.01),
        "20051"
    )]
    #[case(20.0, 12.0, 189.25, 1.0, 221.21, 1.04, 174.76, 14.50, Some(-0.01), "20051")]
    fn test_violations(
        #[case] cc: f64,
        #[case] vc: f64,
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
            "10920/20051" => &[nctm],
            "10920/10910" => &[tcl],
            "Payment SumUp" => &[psu],
            "68450/10920" => &[Some(cl)],
            "20051/10900" => &[dtm],
            "20051/30200" => &[ilm],
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
