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

    write_summary(output_path, &mut df.clone())?;
    write_additional_file(&mut gather_df_miti(&df)?, "miti.csv")?;
    write_additional_file(&mut gather_df_accounting(&df)?, "accounting.csv")
}

fn gather_df_miti(df: &DataFrame) -> PolarsResult<DataFrame> {
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
            col("Gross MiTi (MiTi) Card").alias("Gross Card MiTi"),
            col("MiTi_Commission").alias("Commission MiTi"),
            col("Net MiTi (MiTi) Card").alias("Net Card MiTi"),
            col("Contribution LoLa"),
            col("Credit MiTi").alias("Credit"),
        ])
        .collect()
}

fn gather_df_accounting(df: &DataFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select([
            col("Date"),
            col("Gross Card LoLa"),
            col("Net Card MiTi"),
            col("Net Card Total"),
            col("LoLa_Commission").alias("Commission LoLa"),
        ])
        .collect()
}

fn validate_topic_owner_constraint(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::MiTiWithoutOwner,
        col("Topic")
            .eq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).eq(lit(""))),
    )?;
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::LoLaWithOwner,
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).neq(lit(""))),
    )?;
    Ok(())
}

enum TopicOwnerConstraintViolationError {
    MiTiWithoutOwner(DataFrame),
    LoLaWithOwner(DataFrame),
}

impl Display for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicOwnerConstraintViolationError::MiTiWithoutOwner(df) => {
                write!(f, "Row with topic 'MiTi' must have an Owner! {df}")
            }
            TopicOwnerConstraintViolationError::LoLaWithOwner(df) => {
                write!(
                    f,
                    "Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner! {df}"
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
    let cafe_cash = price_by_date_for(
        consumption_of(&Topic::Cafe, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let cafe_card = price_by_date_for(
        consumption_of(&Topic::Cafe, &PaymentMethod::Card),
        ldf.clone(),
    );
    let miti_cash = price_by_date_for(
        consumption_of(&Topic::MiTi, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let miti_card = price_by_date_for(
        consumption_of(&Topic::MiTi, &PaymentMethod::Card),
        ldf.clone(),
    );
    let verm_cash = price_by_date_for(
        consumption_of(&Topic::Verm, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let verm_card = price_by_date_for(
        consumption_of(&Topic::Verm, &PaymentMethod::Card),
        ldf.clone(),
    );
    let cafe_tips = price_by_date_for(tips_of_topic(&Topic::Cafe), ldf.clone());
    let miti_tips_cash = price_by_date_for(
        tips_of_topic_by_payment_method(&Topic::MiTi, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let miti_tips_card = price_by_date_for(
        tips_of_topic_by_payment_method(&Topic::MiTi, &PaymentMethod::Card),
        ldf.clone(),
    );
    let miti_tips = price_by_date_for(tips_of_topic(&Topic::MiTi), ldf.clone());
    let verm_tips = price_by_date_for(tips_of_topic(&Topic::Verm), ldf.clone());
    let tips_cash = price_by_date_for(tips_by_payment_method(&PaymentMethod::Cash), ldf.clone());
    let tips_card = price_by_date_for(tips_by_payment_method(&PaymentMethod::Card), ldf.clone());
    let miti_comm = commission_by_date_for(commission_by(&Owner::MiTi, None), ldf.clone());
    let lola_comm = commission_by_date_for(commission_by(&Owner::LoLa, Some(false)), ldf.clone());
    let lola_comm_miti =
        commission_by_date_for(commission_by(&Owner::LoLa, Some(true)), ldf.clone());
    let miti_miti = price_by_date_for(miti_by(&Owner::MiTi), ldf.clone());
    let miti_lola = price_by_date_for(miti_by(&Owner::LoLa), ldf.clone());
    let gross_miti_miti_card = price_by_date_for(
        owned_consumption_of(&Topic::MiTi, &Owner::MiTi, &PaymentMethod::Card),
        ldf,
    );

    let with_cafe_cash = all_dates.join(cafe_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_cafe_card =
        with_cafe_cash.join(cafe_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_cash =
        with_cafe_card.join(miti_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_card =
        with_miti_cash.join(miti_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_verm_cash =
        with_miti_card.join(verm_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_verm_card =
        with_verm_cash.join(verm_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_cafe_tips =
        with_verm_card.join(cafe_tips, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_tips_cash =
        with_cafe_tips.join(miti_tips_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_tips_card =
        with_miti_tips_cash.join(miti_tips_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_miti_tips =
        with_miti_tips_card.join(miti_tips, [col("Date")], [col("Date")], JoinType::Left);
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
    let with_tips_cash =
        with_comm_lola.join(tips_cash, [col("Date")], [col("Date")], JoinType::Left);
    let with_tips_card =
        with_tips_cash.join(tips_card, [col("Date")], [col("Date")], JoinType::Left);
    let with_lola_comm_miti =
        with_tips_card.join(lola_comm_miti, [col("Date")], [col("Date")], JoinType::Left);
    let with_gross_miti_miti_card = with_lola_comm_miti.join(
        gross_miti_miti_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left,
    );

    with_gross_miti_miti_card
        .with_column(
            (col("MiTi_Cash").fill_null(0.0) + col("MiTi_Card").fill_null(0.0))
                .round(2)
                .alias("MiTi Total"),
        )
        .with_column(
            (col("Cafe_Cash").fill_null(0.0) + col("Cafe_Card").fill_null(0.0))
                .round(2)
                .alias("Cafe Total"),
        )
        .with_column(
            (col("Verm_Cash").fill_null(0.0) + col("Verm_Card").fill_null(0.0))
                .round(2)
                .alias("Verm Total"),
        )
        .with_column(
            (col("MiTi_Cash").fill_null(0.0)
                + col("Cafe_Cash").fill_null(0.0)
                + col("Verm_Cash").fill_null(0.0))
            .round(2)
            .alias("Gross Cash"),
        )
        .with_column(
            (col("MiTi_Card").fill_null(0.0)
                + col("Cafe_Card").fill_null(0.0)
                + col("Verm_Card").fill_null(0.0))
            .round(2)
            .alias("Gross Card"),
        )
        .with_column(
            col("MiTi_Card")
                .fill_null(0.0)
                .round(2)
                .alias("Gross Card MiTi"),
        )
        .with_column(
            (col("Cafe_Card").fill_null(0.0) + col("Verm_Card").fill_null(0.0))
                .round(2)
                .alias("Gross Card LoLa"),
        )
        .with_column(
            (col("MiTi Total").fill_null(0.0)
                + col("Cafe Total").fill_null(0.0)
                + col("Verm Total").fill_null(0.0))
            .round(2)
            .alias("Gross Total"),
        )
        .with_column(
            (col("Tips_Cash").fill_null(0.0) + col("Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Tips Total"),
        )
        .with_column(
            (col("Gross Cash").fill_null(0.0) + col("Tips_Cash").fill_null(0.0))
                .round(2)
                .alias("Sumup Cash"),
        )
        .with_column(
            (col("Gross Card").fill_null(0.0) + col("Tips_Card").fill_null(0.0))
                .round(2)
                .alias("Sumup Card"),
        )
        .with_column(
            (col("Cafe Total").fill_null(0.0)
                + col("MiTi Total").fill_null(0.0)
                + col("Verm Total").fill_null(0.0)
                + col("Tips Total").fill_null(0.0))
            .round(2)
            .alias("SumUp Total"),
        )
        .with_column(
            (col("MiTi_Commission").fill_null(0.0) + col("LoLa_Commission").fill_null(0.0))
                .round(2)
                .alias("Total Commission"),
        )
        .with_column(
            (col("Gross Card MiTi").fill_null(0.0) - col("MiTi_Commission").fill_null(0.0))
                .round(2)
                .alias("Net Card MiTi"),
        )
        .with_column(
            (col("Gross Card LoLa").fill_null(0.0) - col("LoLa_Commission").fill_null(0.0))
                .round(2)
                .alias("Net Card LoLa"),
        )
        .with_column(
            (col("Gross Card").fill_null(0.0) - col("Total Commission").fill_null(0.0))
                .round(2)
                .alias("Net Card Total"),
        )
        .with_column(
            (col("Gross MiTi (MiTi) Card").fill_null(0.0) - col("MiTi_Commission").fill_null(0.0))
                .round(2)
                .alias("Net MiTi (MiTi) Card"),
        )
        .with_column(
            (lit(0.2)
                * (col("Gross MiTi (LoLa)").fill_null(0.0)
                    - col("LoLa_Commission_MiTi").fill_null(0.0)))
            .round(2)
            .alias("Contribution LoLa"),
        )
        .with_column(
            (col("Net MiTi (MiTi) Card").fill_null(0.0) + col("Contribution LoLa").fill_null(0.0))
                .round(2)
                .alias("Credit MiTi"),
        )
        .select([
            col("Date"),
            col("MiTi_Cash"),
            col("MiTi_Card"),
            col("MiTi Total"),
            col("Cafe_Cash"),
            col("Cafe_Card"),
            col("Cafe Total"),
            col("Verm_Cash"),
            col("Verm_Card"),
            col("Verm Total"),
            col("Gross Cash"),
            col("Tips_Cash"),
            col("Sumup Cash"),
            col("Gross Card"),
            col("Tips_Card"),
            col("Sumup Card"),
            col("Gross Total"),
            col("Tips Total"),
            col("SumUp Total"),
            col("Gross Card MiTi"),
            col("MiTi_Commission"),
            col("Net Card MiTi"),
            col("Gross Card LoLa"),
            col("LoLa_Commission"),
            col("LoLa_Commission_MiTi"),
            col("Net Card LoLa"),
            col("Gross Card").alias("Gross Card Total"),
            col("Total Commission"),
            col("Net Card Total"),
            col("MiTi_Tips_Cash"),
            col("MiTi_Tips_Card"),
            col("MiTi_Tips"),
            col("Cafe_Tips"),
            col("Verm_Tips"),
            col("Gross MiTi (MiTi)"),
            col("Gross MiTi (LoLa)"),
            col("Gross MiTi (MiTi) Card"),
            col("Net MiTi (MiTi) Card"),
            col("Contribution LoLa"),
            col("Credit MiTi"),
        ])
        .collect()
}

/// Predicate and alias for Consumption, selected for `Topic` and `PaymentMethod`
fn consumption_of(topic: &Topic, payment_method: &PaymentMethod) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("{topic}_{payment_method}");
    (expr, alias)
}

fn owned_consumption_of(
    topic: &Topic,
    owner: &Owner,
    payment_method: &PaymentMethod,
) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Owner").eq(lit(owner.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("Gross {topic} ({owner}) {payment_method}");
    (expr, alias)
}

/// Predicate and alias for Tips, selected by `Topic`
fn tips_of_topic(topic: &Topic) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Purpose").eq(lit(Purpose::Tip.to_string())));
    let alias = format!("{topic}_Tips");
    (expr, alias)
}

/// Predicate and alias for Tips, selected by `Topic` and `PaymentMethod`
fn tips_of_topic_by_payment_method(
    topic: &Topic,
    payment_method: &PaymentMethod,
) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").eq(lit(Purpose::Tip.to_string())));
    let alias = format!("{topic}_Tips_{payment_method}");
    (expr, alias)
}

/// Predicate and alias for Tips, selected by `Topic`
fn tips_by_payment_method(payment_method: &PaymentMethod) -> (Expr, String) {
    let expr = (col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").eq(lit(Purpose::Tip.to_string())));
    let alias = format!("Tips_{payment_method}");
    (expr, alias)
}

/// Predicate and alias for Miti by `Owner`
fn miti_by(owner: &Owner) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(Topic::MiTi.to_string())))
        .and(col("Owner").eq(lit(owner.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("Gross MiTi ({owner})");
    (expr, alias)
}

/// Predicate and alias for commission by `Owner`
fn commission_by(owner: &Owner, miti_only: Option<bool>) -> (Expr, String) {
    let expr = match owner {
        Owner::MiTi => (col("Topic").eq(lit(Topic::MiTi.to_string())))
            .and(col("Owner").eq(lit(Owner::MiTi.to_string()))),
        Owner::LoLa => match miti_only {
            None => panic!("Configured wrongly, we need [miti_only] for owner LoLa"),
            Some(false) => (col("Topic").neq(lit(Topic::MiTi.to_string())))
                .or(col("Owner").eq(lit(Owner::LoLa.to_string()))),
            Some(true) => (col("Topic").eq(lit(Topic::MiTi.to_string())))
                .and(col("Owner").eq(lit(Owner::LoLa.to_string()))),
        },
    };
    let alias = match miti_only {
        None | Some(false) => format!("{owner}_Commission"),
        Some(true) => format!("{owner}_Commission_MiTi"),
    };
    (expr, alias)
}

/// Aggregates daily consumption
fn price_by_date_for(predicate_and_alias: (Expr, String), ldf: LazyFrame) -> LazyFrame {
    key_figure_by_date_for("Price (Gross)", predicate_and_alias, ldf)
}

/// Aggregates daily commission
fn commission_by_date_for(predicate_and_alias: (Expr, String), ldf: LazyFrame) -> LazyFrame {
    key_figure_by_date_for("Commission", predicate_and_alias, ldf)
}

/// Aggregates daily values for a particular key figure, rounded to a two decimals
fn key_figure_by_date_for(
    key_figure: &str,
    predicate_and_alias: (Expr, String),
    ldf: LazyFrame,
) -> LazyFrame {
    let (predicate, alias) = predicate_and_alias;
    ldf.filter(predicate)
        .groupby(["Date"])
        .agg([col(key_figure).sum()])
        .select([
            col("Date"),
            col(key_figure)
                .round(2)
                .fill_null(0.0)
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

fn write_summary(output_path: &Option<PathBuf>, df: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(df)?;
    Ok(())
}

fn write_additional_file(df: &mut DataFrame, file_name: &str) -> Result<(), Box<dyn Error>> {
    let output_path = Some(PathBuf::from(file_name));
    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Topic::Cafe, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["14.03.2023", "16.03.2023", "28.03.2023"],
            "Cafe_Card" => &[1.3, 0.0, 3.6]),
        )
    ]
    #[case(Topic::Cafe, PaymentMethod::Cash, Purpose::Consumption,
        df!(
            "Date" => &["20.03.2023"],
            "Cafe_Cash" => &[4.7]),
        )
    ]
    #[case(Topic::MiTi, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["15.03.2023"],
            "MiTi_Card" => &[5.2]),
        )
    ]
    #[case(Topic::Cafe, PaymentMethod::Cash, Purpose::Tip,
        df!(
            "Date" => &["14.03.2023"],
            "Cafe_Tips" => &[0.5]),
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
            "Topic" => &["Cafe", "Cafe", "MiTi", "Cafe", "Cafe", "Cafe"],
            "Payment Method" => &["Card", "Card", "Card", "Card", "Cash", "Cash"],
            "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Tip"],
        )
        .expect("Misconfigured dataframe");
        let paa = match purpose {
            Purpose::Consumption => consumption_of(&topic, &payment_method),
            Purpose::Tip => tips_of_topic(&topic),
        };
        let out = price_by_date_for(paa, df_in.lazy())
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
            "Payment Method" => &["Card"],
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
            "Comment" => &[None::<String>],
        )
        .expect("Misconfigured test data frame");
        let out = collect_data(df).expect("should be able to collect the data");
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let expected = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[None::<f64>],
            "MiTi_Card" => &[Some(16.0)],
            "MiTi Total" => &[16.0],
            "Cafe_Cash" => &[None::<f64>],
            "Cafe_Card" => &[None::<f64>],
            "Cafe Total" => &[0.0],
            "Verm_Cash" => &[None::<f64>],
            "Verm_Card" => &[None::<f64>],
            "Verm Total" => &[0.0],
            "Gross Cash" => &[0.0],
            "Tips_Cash" => &[None::<f64>],
            "Sumup Cash" => &[0.0],
            "Gross Card" => &[16.0],
            "Tips_Card" => &[None::<f64>],
            "Sumup Card" => &[16.0],
            "Gross Total" => &[16.0],
            "Tips Total" => &[0.0],
            "SumUp Total" => &[16.0],
            "Gross Card MiTi" => &[16.0],
            "MiTi_Commission" => &[Some(0.24)],
            "Net Card MiTi" => &[15.76],
            "Gross Card LoLa" => &[0.0],
            "LoLa_Commission" => &[None::<f64>],
            "LoLa_Commission_MiTi" => &[None::<f64>],
            "Net Card LoLa" => &[0.0],
            "Gross Card Total" => &[16.0],
            "Total Commission" => &[0.24],
            "Net Card Total" => &[15.76],
            "MiTi_Tips_Cash" => &[None::<f64>],
            "MiTi_Tips_Card" => &[None::<f64>],
            "MiTi_Tips" => &[None::<f64>],
            "Cafe_Tips" => &[None::<f64>],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(16.0)],
            "Gross MiTi (LoLa)" => &[None::<f64>],
            "Gross MiTi (MiTi) Card" => &[Some(16.0)],
            "Net MiTi (MiTi) Card" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit MiTi" => &[15.76],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        assert_eq!(out, expected.expect("valid data frame"));
    }

    #[rstest]
    // MiTi must have an owner
    #[case(Topic::MiTi, Some("LoLa"), true, None)]
    #[case(Topic::MiTi, Some("MiTi"), true, None)]
    // LoLa (Cafe/Verm) must have no owner
    #[case(Topic::Cafe, Some(""), true, None)]
    #[case(Topic::Verm, Some(""), true, None)]
    #[case(Topic::Verm, None, true, None)]
    // MiTi w/o or non-MiTi with owner should fail
    #[case(
        Topic::MiTi,
        Some(""),
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::MiTi,
        None,
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::Cafe,
        Some("Cafe"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
    )]
    #[case(
        Topic::Verm,
        Some("MiTi"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
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
            "Comment" => &[None::<String>],
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

    #[rstest]
    fn test_gather_df_miti() {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df_summary = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[None::<f64>],
            "MiTi_Card" => &[Some(16.0)],
            "MiTi Total" => &[16.0],
            "Cafe_Cash" => &[None::<f64>],
            "Cafe_Card" => &[None::<f64>],
            "Cafe Total" => &[0.0],
            "Verm_Cash" => &[None::<f64>],
            "Verm_Card" => &[None::<f64>],
            "Verm Total" => &[0.0],
            "Gross Cash" => &[0.0],
            "Tips_Cash" => &[None::<f64>],
            "Sumup Cash" => &[0.0],
            "Gross Card" => &[16.0],
            "Tips_Card" => &[None::<f64>],
            "Sumup Card" => &[16.0],
            "Gross Total" => &[16.0],
            "Tips Total" => &[0.0],
            "SumUp Total" => &[16.0],
            "Gross Card MiTi" => &[16.0],
            "MiTi_Commission" => &[Some(0.24)],
            "Net Card MiTi" => &[15.76],
            "Gross Card LoLa" => &[0.0],
            "LoLa_Commission" => &[None::<f64>],
            "LoLa_Commission_MiTi" => &[None::<f64>],
            "Net Card LoLa" => &[0.0],
            "Gross Card Total" => &[16.0],
            "Total Commission" => &[0.24],
            "Net Card Total" => &[15.76],
            "MiTi_Tips_Cash" => &[None::<f64>],
            "MiTi_Tips_Card" => &[None::<f64>],
            "MiTi_Tips" => &[None::<f64>],
            "Cafe_Tips" => &[None::<f64>],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(16.0)],
            "Gross MiTi (LoLa)" => &[None::<f64>],
            "Gross MiTi (MiTi) Card" => &[Some(16.0)],
            "Net MiTi (MiTi) Card" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit MiTi" => &[15.76],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "Income Cash" => &[None::<f64>],
            "Tips Cash" => &[None::<f64>],
            "Total Cash" => &[0.0],
            "Income Card" => &[Some(16.0)],
            "Tips Card" => &[None::<f64>],
            "Total Card" => &[16.0],
            "Income Total" => &[16.0],
            "Tips Total" => &[0.0],
            "Payment Total" => &[16.0],
            "Gross Income MiTi" => &[Some(16.0)],
            "Gross Income LoLa" => &[None::<f64>],
            "Gross Card MiTi" => &[Some(16.0)],
            "Commission MiTi" => &[0.24],
            "Net Card MiTi" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit" => &[15.76],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out = gather_df_miti(&df_summary).expect("should be able to collect miti_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }

    #[rstest]
    fn test_gather_df_accounting() {
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let df_summary = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[None::<f64>],
            "MiTi_Card" => &[Some(16.0)],
            "MiTi Total" => &[16.0],
            "Cafe_Cash" => &[None::<f64>],
            "Cafe_Card" => &[None::<f64>],
            "Cafe Total" => &[0.0],
            "Verm_Cash" => &[None::<f64>],
            "Verm_Card" => &[None::<f64>],
            "Verm Total" => &[0.0],
            "Gross Cash" => &[0.0],
            "Tips_Cash" => &[None::<f64>],
            "Sumup Cash" => &[0.0],
            "Gross Card" => &[16.0],
            "Tips_Card" => &[None::<f64>],
            "Sumup Card" => &[16.0],
            "Gross Total" => &[16.0],
            "Tips Total" => &[0.0],
            "SumUp Total" => &[16.0],
            "Gross Card MiTi" => &[16.0],
            "MiTi_Commission" => &[Some(0.24)],
            "Net Card MiTi" => &[15.76],
            "Gross Card LoLa" => &[0.0],
            "LoLa_Commission" => &[None::<f64>],
            "LoLa_Commission_MiTi" => &[None::<f64>],
            "Net Card LoLa" => &[0.0],
            "Gross Card Total" => &[16.0],
            "Total Commission" => &[0.24],
            "Net Card Total" => &[15.76],
            "MiTi_Tips_Cash" => &[None::<f64>],
            "MiTi_Tips_Card" => &[None::<f64>],
            "MiTi_Tips" => &[None::<f64>],
            "Cafe_Tips" => &[None::<f64>],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(16.0)],
            "Gross MiTi (LoLa)" => &[None::<f64>],
            "Gross MiTi (MiTi) Card" => &[Some(16.0)],
            "Net MiTi (MiTi) Card" => &[15.76],
            "Contribution LoLa" => &[0.0],
            "Credit MiTi" => &[15.76],
        )
        .expect("valid data frame");
        let expected = df!(
            "Date" => &[date],
            "Gross Card LoLa" => &[0.0],
            "Net Card MiTi" => &[15.76],
            "Net Card Total" => &[15.76],
            "Commission LoLa" => &[None::<f64>],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        let out =
            gather_df_accounting(&df_summary).expect("should be able to collect accounting_df");
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
