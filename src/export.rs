use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::prepare::{PaymentMethod, Purpose, Topic};

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;
    let mut df = collect_data(raw_df)?;
    df.extend(&df.sum())?;

    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(&mut df)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn collect_data(raw_df: DataFrame) -> PolarsResult<DataFrame> {
    let dt_options = StrptimeOptions {
        format: Some("%d.%m.%Y".into()),
        strict: true,
        exact: true,
        ..Default::default()
    };
    let ldf = raw_df
        .lazy()
        .with_column(col("Date").str().strptime(DataType::Date, dt_options));
    let all_dates = ldf
        .clone()
        .select([col("Date")])
        .unique(None, UniqueKeepStrategy::First)
        .sort(
            "Date",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: false,
            },
        );
    let lola_cash = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::LoLa, &PaymentMethod::Cash),
    );
    let lola_card = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::LoLa, &PaymentMethod::Card),
    );
    let miti_cash = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::MiTi, &PaymentMethod::Cash),
    );
    let miti_card = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::MiTi, &PaymentMethod::Card),
    );
    let verm_cash = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::Verm, &PaymentMethod::Cash),
    );
    let verm_card = collect_by(
        ldf.clone(),
        cons_pred_and_alias(&Topic::Verm, &PaymentMethod::Card),
    );
    let lola_tips = collect_by(ldf.clone(), tip_pred_and_alias(&Topic::LoLa));
    let miti_tips = collect_by(ldf.clone(), tip_pred_and_alias(&Topic::MiTi));
    let verm_tips = collect_by(ldf, tip_pred_and_alias(&Topic::Verm));
    let comb1 = all_dates.join(lola_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb2 = comb1.join(lola_card, [col("Date")], [col("Date")], JoinType::Left);
    let comb3 = comb2.join(miti_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb4 = comb3.join(miti_card, [col("Date")], [col("Date")], JoinType::Left);
    let comb5 = comb4.join(verm_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb6 = comb5.join(verm_card, [col("Date")], [col("Date")], JoinType::Left);
    let comb7 = comb6.join(lola_tips, [col("Date")], [col("Date")], JoinType::Left);
    let comb8 = comb7.join(miti_tips, [col("Date")], [col("Date")], JoinType::Left);
    let comb9 = comb8.join(verm_tips, [col("Date")], [col("Date")], JoinType::Left);
    comb9
        .with_column(
            (col("LoLa_Bar").fill_null(0.0) + col("LoLa_Card").fill_null(0.0)).alias("LoLa Total"),
        )
        .with_column(
            (col("MiTi_Bar").fill_null(0.0) + col("MiTi_Card").fill_null(0.0)).alias("MiTi Total"),
        )
        .with_column(
            (col("Verm_Bar").fill_null(0.0) + col("Verm_Card").fill_null(0.0)).alias("Verm Total"),
        )
        .with_column(
            (col("LoLa_Bar").fill_null(0.0)
                + col("MiTi_Bar").fill_null(0.0)
                + col("Verm_Bar").fill_null(0.0))
            .alias("Cash Total"),
        )
        .with_column(
            (col("LoLa_Card").fill_null(0.0)
                + col("MiTi_Card").fill_null(0.0)
                + col("Verm_Card").fill_null(0.0))
            .alias("Card Total"),
        )
        .with_column(
            (col("LoLa Total").fill_null(0.0)
                + col("MiTi Total").fill_null(0.0)
                + col("Verm Total").fill_null(0.0))
            .alias("Total"),
        )
        .with_column(
            (col("LoLa_Tips").fill_null(0.0)
                + col("MiTi_Tips").fill_null(0.0)
                + col("Verm_Tips").fill_null(0.0))
            .alias("Total Tips"),
        )
        .with_column(
            (col("LoLa Total").fill_null(0.0)
                + col("MiTi Total").fill_null(0.0)
                + col("Verm Total").fill_null(0.0)
                + col("Total Tips").fill_null(0.0))
            .alias("Total SumUp"),
        )
        .select([
            col("Date"),
            col("MiTi_Bar"),
            col("MiTi_Card"),
            col("MiTi Total"),
            col("LoLa_Bar"),
            col("LoLa_Card"),
            col("LoLa Total"),
            col("Verm_Bar"),
            col("Verm_Card"),
            col("Verm Total"),
            col("Cash Total"),
            col("Card Total"),
            col("Total"),
            col("MiTi_Tips"),
            col("LoLa_Tips"),
            col("Verm_Tips"),
            col("Total Tips"),
            col("Total SumUp"),
        ])
        .collect()
}

fn cons_pred_and_alias(topic: &Topic, payment_method: &PaymentMethod) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let payment_method_label = match payment_method {
        PaymentMethod::Cash => "Bar",
        PaymentMethod::Card => "Card",
    };
    let alias = format!("{topic}_{payment_method_label}");
    (expr, alias)
}

fn tip_pred_and_alias(topic: &Topic) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Purpose").eq(lit(Purpose::Tip.to_string())));
    let alias = format!("{topic}_Tips");
    (expr, alias)
}

fn collect_by(ldf: LazyFrame, predicate_and_alias: (Expr, String)) -> LazyFrame {
    let (predicate, alias) = predicate_and_alias;
    ldf.filter(predicate)
        .groupby(["Date"])
        .agg([col("Price (Gross)").sum()])
        .select([
            col("Date"),
            col("Price (Gross)").fill_null(0.0).alias(alias.as_str()),
        ])
        .sort(
            "Date",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: false,
            },
        )
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Topic::LoLa, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["14.03.2023", "16.03.2023", "28.03.2023"],
            "LoLa_Card" => &[1.3, 0.0, 3.6]),
        )
    ]
    #[case(Topic::LoLa, PaymentMethod::Cash, Purpose::Consumption,
        df!(
            "Date" => &["20.03.2023"],
            "LoLa_Bar" => &[4.7]),
        )
    ]
    #[case(Topic::MiTi, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["15.03.2023"],
            "MiTi_Card" => &[5.2]),
        )
    ]
    #[case(Topic::LoLa, PaymentMethod::Cash, Purpose::Tip,
        df!(
            "Date" => &["14.03.2023"],
            "LoLa_Tips" => &[0.5]),
        )
    ]
    fn test_collect_by(
        #[case] topic: Topic,
        #[case] payment_method: PaymentMethod,
        #[case] purpose: Purpose,
        #[case] expected: PolarsResult<DataFrame>,
    ) {
        let df_in = df!(
            "Date" => &["16.03.2023", "14.03.2023", "15.03.2023", "28.03.2023", "20.03.2023", "14.03.2023"],
            "Price (Gross)" => &[None, Some(1.3), Some(5.2), Some(3.6), Some(4.7), Some(0.5)],
            "Topic" => &["LoLa", "LoLa", "MiTi", "LoLa", "LoLa", "LoLa"],
            "Payment Method" => &["Card", "Card", "Card", "Card", "Cash", "Cash"],
            "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Tip"],
        )
        .expect("Misconfigured dataframe");
        let paa = match purpose {
            Purpose::Consumption => cons_pred_and_alias(&topic, &payment_method),
            Purpose::Tip => tip_pred_and_alias(&topic),
        };
        let out = collect_by(df_in.lazy(), paa)
            .collect()
            .expect("Unable to collect result");
        assert_eq!(out, expected.expect("Misconfigured expected df"));
    }

    #[rstest]
    fn test_collect_data() {
        let df = df!(
            "Account" => &["a@b.ch"],
            "Date" => &["17.04.2023"],
            "Time" => &["12:32:00"],
            "Type" => &["Sales"],
            "Transaction ID" => &["TEGUCXAGDE"],
            "Receipt Number" => &["S20230000303"],
            "Payment Method" => &["Cash"],
            "Quantity" => &[1],
            "Description" => &["foo"],
            "Currency" => &["CHF"],
            "Price (Gross)" => &[16.0],
            "Price (Net)" => &[16.0],
            "Tax" => &["0.0%"],
            "Tax rate" => &[""],
            "Transaction refunded" => &[""],
            "Topic" => &["MiTi"],
            "Purpose" => &["Consumption"],
            "Comment" => &[""],
        )
        .expect("Misconfigured test data frame");
        let out = collect_data(df).expect("should be able to collect the data");
        let date0 = NaiveDate::parse_from_str("1.4.2023", "%d.%m.%Y").expect("valid date");
        let date1 = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let expected = df!(
            "Date" => &[date0, date1],
            "MiTi_Bar" => &[Some(0.0), Some(16.0)],
            "MiTi_Card" => &[Some(0.0),  None],
            "MiTi Total" => &[0.0, 16.0],
            "LoLa_Bar" => &[Some(0.0), None],
            "LoLa_Card" => &[Some(0.0), None],
            "LoLa Total" => &[0.0, 0.0],
            "Verm_Bar" => &[Some(0.0), None],
            "Verm_Card" => &[Some(0.0), None],
            "Verm Total" => &[0.0, 0.0],
            "Cash Total" => &[0.0, 16.0],
            "Card Total" => &[0.0, 0.0],
            "Total" => &[0.0, 16.0],
            "MiTi_Tips" => &[Some(0.0), None],
            "LoLa_Tips" => &[Some(0.0), None],
            "Verm_Tips" => &[Some(0.0), None],
            "Total Tips" => &[0.0, 0.0],
            "Total SumUp" => &[0.0, 16.0],
        )
        .expect("valid data frame")
        .lazy()
        .filter(col("Total SumUp").neq(lit(0.0)))
        .collect();
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
