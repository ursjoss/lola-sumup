use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::prepare::{Owner, PaymentMethod, Purpose, Topic};

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;
    validate_topic_owner_constraint(&raw_df)?;

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

fn validate_topic_owner_constraint(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::MiTiWithoutOwner,
        col("Topic")
            .eq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").is_null()),
    )?;
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::OtherWithOwner,
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").is_not_null()),
    )?;
    Ok(())
}

enum TopicOwnerConstraintViolationError {
    MiTiWithoutOwner(DataFrame),
    OtherWithOwner(DataFrame),
}

impl Display for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicOwnerConstraintViolationError::MiTiWithoutOwner(df) => {
                write!(f, "Row with topic 'MiTi' must have an Owner! {df}")
            }
            TopicOwnerConstraintViolationError::OtherWithOwner(df) => {
                write!(
                    f,
                    "Row with topic other than 'MiTi' must not have an Owner! {df}"
                )
            }
        }
    }
}

impl Debug for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}:?")
    }
}

impl Error for TopicOwnerConstraintViolationError {}

fn topic_owner_constraint(
    raw_df: &DataFrame,
    error: fn(DataFrame) -> TopicOwnerConstraintViolationError,
    predicate: Expr,
) -> Result<(), Box<dyn Error>> {
    let df = raw_df
        .clone()
        .lazy()
        .with_row_count("Row-No", Some(2))
        .filter(predicate)
        .select([
            col("Row-No"),
            col("Date"),
            col("Time"),
            col("Transaction ID"),
            col("Description"),
            col("Price (Gross)"),
            col("Topic"),
            col("Owner"),
            col("Purpose"),
            col("Comment"),
        ])
        .collect()?;
    if df.shape().0 > 0 {
        Err(Box::try_from(error(df)).unwrap())
    } else {
        Ok(())
    }
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
    let lola_cash = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::LoLa, &PaymentMethod::Cash),
    );
    let lola_card = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::LoLa, &PaymentMethod::Card),
    );
    let miti_cash = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::MiTi, &PaymentMethod::Cash),
    );
    let miti_card = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::MiTi, &PaymentMethod::Card),
    );
    let verm_cash = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::Verm, &PaymentMethod::Cash),
    );
    let verm_card = collect_by_price(
        ldf.clone(),
        cons_pred_and_alias(&Topic::Verm, &PaymentMethod::Card),
    );
    let lola_tips = collect_by_price(ldf.clone(), tip_pred_and_alias(&Topic::LoLa));
    let miti_tips = collect_by_price(ldf.clone(), tip_pred_and_alias(&Topic::MiTi));
    let verm_tips = collect_by_price(ldf.clone(), tip_pred_and_alias(&Topic::Verm));
    let miti_comm = collect_by_commission(ldf.clone(), commission_pred_and_alias(&Owner::MiTi));
    let lola_comm = collect_by_commission(ldf.clone(), commission_pred_and_alias(&Owner::LoLa));
    let miti_miti = collect_by_price(ldf.clone(), miti_pred_and_alias(&Owner::MiTi));
    let miti_lola = collect_by_price(ldf, miti_pred_and_alias(&Owner::LoLa));

    let with_lola_cash = all_dates.join(lola_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_lola_card =
        with_lola_cash.join(lola_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_cash =
        with_lola_card.join(miti_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_card =
        with_miti_cash.join(miti_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_verm_cash =
        with_miti_card.join(verm_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_verm_card =
        with_verm_cash.join(verm_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_lola_tips =
        with_verm_card.join(lola_tips, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_tips =
        with_lola_tips.join(miti_tips, [col("Date")], [col("Date")], JoinType::Left);
    let with_verm_tips =
        with_miti_tips.join(verm_tips, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_miti =
        with_verm_tips.join(miti_miti, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_lola =
        with_miti_miti.join(miti_lola, [col("Date")], [col("Date")], JoinType::Left);
    let with_comm_miti =
        with_miti_lola.join(miti_comm, [col("Date")], [col("Date")], JoinType::Left);
    let with_comm_lola =
        with_comm_miti.join(lola_comm, [col("Date")], [col("Date")], JoinType::Left);

    with_comm_lola
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
        .with_column(
            (col("MiTi_Commission").fill_null(0.0) + col("LoLa_Commission").fill_null(0.0))
                .alias("Total Commission"),
        )
        .with_column(
            (col("MiTi_MiTi").fill_null(0.0) + col("MiTi_LoLa").fill_null(0.0)).alias("Total MiTi"),
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
            col("MiTi_Commission"),
            col("LoLa_Commission"),
            col("Total Commission"),
            col("MiTi_MiTi"),
            col("MiTi_LoLa"),
            col("Total MiTi"),
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

fn miti_pred_and_alias(owner: &Owner) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(Topic::MiTi.to_string())))
        .and(col("Owner").eq(lit(owner.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("MiTi_{owner}");
    (expr, alias)
}

fn commission_pred_and_alias(owner: &Owner) -> (Expr, String) {
    let expr = match owner {
        Owner::MiTi => (col("Topic").eq(lit(Topic::MiTi.to_string())))
            .and(col("Owner").eq(lit(owner.to_string()))),
        Owner::LoLa => (col("Topic").neq(lit(Topic::MiTi.to_string())))
            .or(col("Owner").eq(lit(owner.to_string()))),
    };
    let alias = format!("{owner}_Commission");
    (expr, alias)
}

fn collect_by_price(ldf: LazyFrame, predicate_and_alias: (Expr, String)) -> LazyFrame {
    collect_by_key_figure(ldf, "Price (Gross)", predicate_and_alias)
}

fn collect_by_commission(ldf: LazyFrame, predicate_and_alias: (Expr, String)) -> LazyFrame {
    collect_by_key_figure(ldf, "Commission", predicate_and_alias)
}

fn collect_by_key_figure(
    ldf: LazyFrame,
    key_figure: &str,
    predicate_and_alias: (Expr, String),
) -> LazyFrame {
    let (predicate, alias) = predicate_and_alias;
    ldf.filter(predicate)
        .groupby(["Date"])
        .agg([col(key_figure).sum()])
        .select([
            col("Date"),
            col(key_figure)
                .fill_null(0.0)
                .round(2)
                .alias(alias.as_str()),
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
        let out = collect_by_price(df_in.lazy(), paa)
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
            "Commission" =>[0.24123],
            "Topic" => &["MiTi"],
            "Owner" => &["MiTi"],
            "Purpose" => &["Consumption"],
            "Comment" => &[""],
        )
        .expect("Misconfigured test data frame");
        let out = collect_data(df).expect("should be able to collect the data");
        let date0 = NaiveDate::parse_from_str("1.4.2023", "%d.%m.%Y").expect("valid date");
        let date1 = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        // the first record is a silly workaround to get typed values into each slice. It's filtered out below.
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
            "MiTi_Commission" => &[Some(0.0), Some(0.24)],
            "LoLa_Commission" => &[Some(0.0), None],
            "Total Commission" => &[0.0, 0.24],
            "MiTi_MiTi" => &[Some(0.0), Some(16.0)],
            "MiTi_LoLa" => &[Some(0.0), None],
            "Total MiTi" => &[0.0, 16.0],
        )
        .expect("valid data frame")
        .lazy()
        .filter(col("Total SumUp").neq(lit(0.0)))
        .collect();
        assert_eq!(out, expected.expect("valid data frame"));
    }

    #[rstest]
    // MiTi must have an owner
    #[case(Topic::MiTi, Some("LoLa"), true, None)]
    #[case(Topic::MiTi, Some("MiTi"), true, None)]
    // LoLa/Verm must have no owner
    #[case(Topic::LoLa, None, true, None)]
    #[case(Topic::Verm, None, true, None)]
    // MiTi w/o or non-MiTi with owner should fail
    #[case(
        Topic::MiTi,
        None,
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::LoLa,
        Some("LoLa"),
        false,
        Some("Row with topic other than 'MiTi' must not have an Owner!")
    )]
    #[case(
        Topic::Verm,
        Some("MiTi"),
        false,
        Some("Row with topic other than 'MiTi' must not have an Owner!")
    )]
    fn test_constraints(
        #[case] topic: Topic,
        #[case] owner: Option<&str>,
        #[case] expected_valid: bool,
        #[case] error_msg: Option<&str>,
    ) {
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
            "Commission" => &[0.24],
            "Topic" => &[topic.to_string()],
            "Owner" => &[owner],
            "Purpose" => &["Consumption"],
            "Comment" => &[""],
        )
        .expect("single record df");
        match validate_topic_owner_constraint(&df) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                let msg = error_msg.expect("should have an error message");
                assert!(e.to_string().starts_with(msg));
            }
        }
    }
}
