use polars::prelude::*;

use crate::export::export_summary::MitiMealType::{Children, Regular};
use crate::prepare::{Owner, PaymentMethod, Purpose, Topic};

/// Produces the Summary dataframe from the `raw_df` read from the file
#[allow(clippy::too_many_lines)]
pub fn collect_data(raw_df: DataFrame) -> PolarsResult<DataFrame> {
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
        ldf.clone(),
    );
    let meal_count_regular = meal_count(for_meals_of_type(&Regular), ldf.clone());
    let meal_count_children = meal_count(for_meals_of_type(&Children), ldf);

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
    let with_meal_count_regular = with_gross_miti_miti_card.join(
        meal_count_regular,
        [col("Date")],
        [col("Date")],
        JoinType::Left,
    );
    let with_meal_count_children = with_meal_count_regular.join(
        meal_count_children,
        [col("Date")],
        [col("Date")],
        JoinType::Left,
    );

    with_meal_count_children
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
            (col("Net Card MiTi").fill_null(0.0) - col("Net MiTi (MiTi) Card").fill_null(0.0))
                .round(2)
                .alias("Net MiTi (LoLa) Card"),
        )
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) - col("LoLa_Commission_MiTi").fill_null(0.0))
                .round(2)
                .alias("Net MiTi (LoLa)"),
        )
        .with_column(
            (lit(0.8) * col("Net MiTi (LoLa)").fill_null(0.0))
                .round(2)
                .alias("Contribution LoLa"),
        )
        .with_column(
            (lit(0.2) * col("Net MiTi (LoLa)").fill_null(0.0))
                .round(2)
                .alias("Contribution MiTi"),
        )
        .with_column(
            (col("Net MiTi (MiTi) Card").fill_null(0.0)
                + col("Contribution MiTi").fill_null(0.0)
                + col("MiTi_Tips_Card").fill_null(0.0))
            .round(2)
            .alias("Debt to MiTi"),
        )
        .with_column(
            (col("Net MiTi (LoLa) Card").fill_null(0.0) - col("Contribution MiTi").fill_null(0.0))
                .round(2)
                .alias("Income LoLa MiTi"),
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
            col("Net MiTi (LoLa) Card"),
            col("Net MiTi (LoLa)"),
            col("Contribution LoLa"),
            col("Contribution MiTi"),
            col("Debt to MiTi"),
            col("Income LoLa MiTi"),
            col("MealCount_Regular"),
            col("MealCount_Children"),
        ])
        .collect()
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

/// Predicate and alias for Consumption, selected for `Topic` and `PaymentMethod`
fn consumption_of(topic: &Topic, payment_method: &PaymentMethod) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("{topic}_{payment_method}");
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

/// Predicate and alias for Miti by `Owner`
fn miti_by(owner: &Owner) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(Topic::MiTi.to_string())))
        .and(col("Owner").eq(lit(owner.to_string())))
        .and(col("Purpose").neq(lit(Purpose::Tip.to_string())));
    let alias = format!("Gross MiTi ({owner})");
    (expr, alias)
}

/// Predicate and alias for consumption (non-tips) by topic, owner and payment method
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

/// Daily meal count
fn meal_count(predicate_and_alias: (Expr, String), ldf: LazyFrame) -> LazyFrame {
    count_by_date_for(predicate_and_alias, ldf)
}

/// Aggregates daily values for a particular key figure, rounded to a two decimals
fn count_by_date_for(predicate_and_alias: (Expr, String), ldf: LazyFrame) -> LazyFrame {
    let (predicate, alias) = predicate_and_alias;
    ldf.filter(predicate)
        .groupby(["Date"])
        .agg([col("Quantity").sum()])
        .select([
            col("Date"),
            col("Quantity").fill_null(0).alias(alias.as_str()),
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

/// Predicate and alias for `MealCount` of different types of meals
fn for_meals_of_type(meal_type: &MitiMealType) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(Topic::MiTi.to_string())))
        .and(col("Owner").eq(lit(Owner::MiTi.to_string())))
        .and(meal_type.expr());
    let alias = format!("MealCount_{}", meal_type.label());
    (expr, alias)
}

trait FilterExpressionProvider {
    fn expr(&self) -> Expr;
    fn label(&self) -> &str;
}

// Types of Meals for statistics
#[derive(Debug)]
enum MitiMealType {
    /// Regular Menus (Adults)
    Regular,
    /// Kids menus
    Children,
}

impl FilterExpressionProvider for MitiMealType {
    fn expr(&self) -> Expr {
        match self {
            Regular => col("Description")
                .str()
                .starts_with(lit("Hauptgang"))
                .or(col("Description").str().starts_with(lit("Menü")))
                .or(col("Description").str().starts_with(lit("Praktika"))),
            Children => col("Description").str().starts_with(lit("Kinder")),
        }
    }
    fn label(&self) -> &str {
        match self {
            Regular => "Regular",
            Children => "Children",
        }
    }
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
            "Account" => &["a@b.ch", "a@b.ch", "a@b.ch"],
            "Date" => &["17.04.2023", "17.04.2023", "17.04.2023"],
            "Time" => &["12:32:00", "12:33:00", "12:34:00"],
            "Type" => &["Sales", "Sales", "Sales"],
            "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDF", "TEGUCXAGDG"],
            "Receipt Number" => &["S20230000303", "S20230000304", "S20230000305"],
            "Payment Method" => &["Card", "Cash", "Card"],
            "Quantity" => &[1, 1, 4],
            "Description" => &["Hauptgang, normal", "Kaffe", "Cappucino"],
            "Currency" => &["CHF", "CHF", "CHF"],
            "Price (Gross)" => &[16.0, 3.50, 20.0],
            "Price (Net)" => &[16.0, 3.50, 20.0],
            "Tax" => &["0.0%", "0.0%", "0.0%"],
            "Tax rate" => &["", "", ""],
            "Transaction refunded" => &["", "", ""],
            "Commission" =>[0.24123, 0.0, 0.3],
            "Topic" => &["MiTi", "MiTi", "MiTi"],
            "Owner" => &["MiTi", "LoLa", "LoLa"],
            "Purpose" => &["Consumption", "Consumption", "Consumption"],
            "Comment" => &[None::<String>, None::<String>, None::<String>],
        )
        .expect("Misconfigured test data frame");
        let out = collect_data(df).expect("should be able to collect the data");
        let date = NaiveDate::parse_from_str("17.4.2023", "%d.%m.%Y").expect("valid date");
        let expected = df!(
            "Date" => &[date],
            "MiTi_Cash" => &[Some(3.5)],
            "MiTi_Card" => &[Some(36.0)],
            "MiTi Total" => &[39.5],
            "Cafe_Cash" => &[None::<f64>],
            "Cafe_Card" => &[None::<f64>],
            "Cafe Total" => &[0.0],
            "Verm_Cash" => &[None::<f64>],
            "Verm_Card" => &[None::<f64>],
            "Verm Total" => &[0.0],
            "Gross Cash" => &[3.5],
            "Tips_Cash" => &[None::<f64>],
            "Sumup Cash" => &[3.5],
            "Gross Card" => &[36.0],
            "Tips_Card" => &[None::<f64>],
            "Sumup Card" => &[36.0],
            "Gross Total" => &[39.5],
            "Tips Total" => &[0.0],
            "SumUp Total" => &[39.5],
            "Gross Card MiTi" => &[36.0],
            "MiTi_Commission" => &[Some(0.24)],
            "Net Card MiTi" => &[35.76],
            "Gross Card LoLa" => &[0.0],
            "LoLa_Commission" => &[0.3],
            "LoLa_Commission_MiTi" => &[0.3],
            "Net Card LoLa" => &[-0.3],
            "Gross Card Total" => &[36.0],
            "Total Commission" => &[0.54],
            "Net Card Total" => &[35.46],
            "MiTi_Tips_Cash" => &[None::<f64>],
            "MiTi_Tips_Card" => &[None::<f64>],
            "MiTi_Tips" => &[None::<f64>],
            "Cafe_Tips" => &[None::<f64>],
            "Verm_Tips" => &[None::<f64>],
            "Gross MiTi (MiTi)" => &[Some(16.0)],
            "Gross MiTi (LoLa)" => &[Some(23.5)],
            "Gross MiTi (MiTi) Card" => &[Some(16.0)],
            "Net MiTi (MiTi) Card" => &[15.76],
            "Net MiTi (LoLa) Card" => &[20.0],
            "Net MiTi (LoLa)" => &[23.2],
            "Contribution LoLa" => &[18.56],
            "Contribution MiTi" => &[4.64],
            "Debt to MiTi" => &[20.40],
            "Income LoLa MiTi" => &[15.36],
            "MealCount_Regular" => &[1],
            "MealCount_Children" => &[None::<i32>],
        )
        .expect("valid data frame")
        .lazy()
        .collect();
        assert_eq!(out, expected.expect("valid data frame"));
    }
}
