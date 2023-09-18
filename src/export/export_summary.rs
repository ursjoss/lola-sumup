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
    let ldf = raw_df.lazy().with_column(col("Date").str().strptime(
        DataType::Date,
        dt_options,
        Expr::default(),
    ));
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
                maintain_order: true,
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
    let sofe_cash = price_by_date_for(
        consumption_of(&Topic::SoFe, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let sofe_card = price_by_date_for(
        consumption_of(&Topic::SoFe, &PaymentMethod::Card),
        ldf.clone(),
    );
    let deposit_cash = price_by_date_for(
        consumption_of(&Topic::Deposit, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let deposit_card = price_by_date_for(
        consumption_of(&Topic::Deposit, &PaymentMethod::Card),
        ldf.clone(),
    );
    let rental_cash = price_by_date_for(
        consumption_of(&Topic::Rental, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let rental_card = price_by_date_for(
        consumption_of(&Topic::Rental, &PaymentMethod::Card),
        ldf.clone(),
    );
    let culture_cash = price_by_date_for(
        consumption_of(&Topic::Culture, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let culture_card = price_by_date_for(
        consumption_of(&Topic::Culture, &PaymentMethod::Card),
        ldf.clone(),
    );
    let paid_out_cash = price_by_date_for(
        consumption_of(&Topic::PaidOut, &PaymentMethod::Cash),
        ldf.clone(),
    );
    let paid_out_card = price_by_date_for(
        consumption_of(&Topic::PaidOut, &PaymentMethod::Card),
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
    let miti_total_comm = commission_by_date_for(commission_by_topic(&Topic::MiTi), ldf.clone());
    let lola_comm = commission_by_date_for(commission_by(&Owner::LoLa, Some(false)), ldf.clone());
    let lola_comm_miti =
        commission_by_date_for(commission_by(&Owner::LoLa, Some(true)), ldf.clone());
    let deposit_comm = commission_by_date_for(commission_by_topic(&Topic::Deposit), ldf.clone());
    let rental_comm = commission_by_date_for(commission_by_topic(&Topic::Rental), ldf.clone());
    let culture_comm = commission_by_date_for(commission_by_topic(&Topic::Culture), ldf.clone());
    let miti_miti = price_by_date_for(miti_by(&Owner::MiTi), ldf.clone());
    let miti_lola = price_by_date_for(miti_by(&Owner::LoLa), ldf.clone());
    let gross_miti_miti_card = price_by_date_for(
        owned_consumption_of(&Topic::MiTi, &Owner::MiTi, &PaymentMethod::Card),
        ldf.clone(),
    );
    let net_miti_lola_cash = price_by_date_for(
        owned_consumption_of(&Topic::MiTi, &Owner::LoLa, &PaymentMethod::Cash),
        ldf.clone(),
    );

    let meal_count_regular = meal_count(for_meals_of_type(&Regular), ldf.clone());
    let meal_count_children = meal_count(for_meals_of_type(&Children), ldf);

    let with_cafe_cash = all_dates.join(
        cafe_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_cafe_card = with_cafe_cash.join(
        cafe_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_cash = with_cafe_card.join(
        miti_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_card = with_miti_cash.join(
        miti_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_verm_cash = with_miti_card.join(
        verm_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_verm_card = with_verm_cash.join(
        verm_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_sofe_cash = with_verm_card.join(
        sofe_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_sofe_card = with_sofe_cash.join(
        sofe_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_deposit_cash = with_sofe_card.join(
        deposit_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_deposit_card = with_deposit_cash.join(
        deposit_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_rental_cash = with_deposit_card.join(
        rental_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_rental_card = with_rental_cash.join(
        rental_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_culture_cash = with_rental_card.join(
        culture_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_culture_card = with_culture_cash.join(
        culture_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_paid_out_cash = with_culture_card.join(
        paid_out_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_paid_out_card = with_paid_out_cash.join(
        paid_out_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_cafe_tips = with_paid_out_card.join(
        cafe_tips,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_tips_cash = with_cafe_tips.join(
        miti_tips_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_tips_card = with_miti_tips_cash.join(
        miti_tips_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_tips = with_miti_tips_card.join(
        miti_tips,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_verm_tips = with_miti_tips.join(
        verm_tips,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_miti = with_verm_tips.join(
        miti_miti,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_lola = with_miti_miti.join(
        miti_lola,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_comm_miti = with_miti_lola.join(
        miti_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_comm_lola = with_comm_miti.join(
        lola_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_comm_deposit = with_comm_lola.join(
        deposit_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_comm_rental = with_comm_deposit.join(
        rental_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_comm_culture = with_comm_rental.join(
        culture_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_tips_cash = with_comm_culture.join(
        tips_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_tips_card = with_tips_cash.join(
        tips_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_lola_comm_miti = with_tips_card.join(
        lola_comm_miti,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_miti_total_commission = with_lola_comm_miti.join(
        miti_total_comm,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_gross_miti_miti_card = with_miti_total_commission.join(
        gross_miti_miti_card,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_net_miti_lola_cash = with_gross_miti_miti_card.join(
        net_miti_lola_cash,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_meal_count_regular = with_net_miti_lola_cash.join(
        meal_count_regular,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
    );
    let with_meal_count_children = with_meal_count_regular.join(
        meal_count_children,
        [col("Date")],
        [col("Date")],
        JoinType::Left.into(),
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
            (col("SoFe_Cash").fill_null(0.0) + col("SoFe_Card").fill_null(0.0))
                .round(2)
                .alias("SoFe Total"),
        )
        .with_column(
            (col("Deposit_Cash").fill_null(0.0) + col("Deposit_Card").fill_null(0.0))
                .round(2)
                .alias("Deposit Total"),
        )
        .with_column(
            (col("Rental_Cash").fill_null(0.0) + col("Rental_Card").fill_null(0.0))
                .round(2)
                .alias("Rental Total"),
        )
        .with_column(
            (col("Culture_Cash").fill_null(0.0) + col("Culture_Card").fill_null(0.0))
                .round(2)
                .alias("Culture Total"),
        )
        .with_column(
            (col("PaidOut_Cash").fill_null(0.0) + col("PaidOut_Card").fill_null(0.0))
                .round(2)
                .alias("PaidOut Total"),
        )
        .with_column(
            (col("MiTi_Cash").fill_null(0.0)
                + col("Cafe_Cash").fill_null(0.0)
                + col("Verm_Cash").fill_null(0.0)
                + col("SoFe_Cash").fill_null(0.0)
                + col("Deposit_Cash").fill_null(0.0)
                + col("Rental_Cash").fill_null(0.0)
                + col("Culture_Cash").fill_null(0.0)
                + col("PaidOut_Cash").fill_null(0.0))
            .round(2)
            .alias("Gross Cash"),
        )
        .with_column(
            (col("MiTi_Card").fill_null(0.0)
                + col("Cafe_Card").fill_null(0.0)
                + col("Verm_Card").fill_null(0.0)
                + col("SoFe_Card").fill_null(0.0)
                + col("Deposit_Card").fill_null(0.0)
                + col("Rental_Card").fill_null(0.0)
                + col("Culture_Card").fill_null(0.0)
                + col("PaidOut_Card").fill_null(0.0))
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
            (col("Gross Card").fill_null(0.0) - col("MiTi_Card").fill_null(0.0))
                .round(2)
                .alias("Gross Card LoLa"),
        )
        .with_column(
            (col("Gross Cash").fill_null(0.0) + col("Gross Card").fill_null(0.0))
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
                .alias("SumUp Cash"),
        )
        .with_column(
            (col("Gross Card").fill_null(0.0) + col("Tips_Card").fill_null(0.0))
                .round(2)
                .alias("SumUp Card"),
        )
        .with_column(
            (col("SumUp Cash") + col("SumUp Card"))
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
            (col("MiTi_Card").fill_null(0.0) + col("MiTi_Tips_Card").fill_null(0.0)
                - col("MiTi_Total_Commission").fill_null(0.0))
            .round(2)
            .alias("Net Payment SumUp MiTi"),
        )
        .with_column(
            (col("Gross MiTi (MiTi) Card").fill_null(0.0) - col("MiTi_Commission").fill_null(0.0))
                .round(2)
                .alias("Net MiTi (MiTi) Card"),
        )
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) - col("LoLa_Commission_MiTi").fill_null(0.0))
                .round(2)
                .alias("Net MiTi (LoLa)"),
        )
        .with_column(
            (lit(0.2) * col("Net MiTi (LoLa)").fill_null(0.0))
                .round(2)
                .alias("Contribution MiTi"),
        )
        .with_column(
            (col("Gross MiTi (LoLa)").fill_null(0.0) - col("Contribution MiTi").fill_null(0.0))
                .round(2)
                .alias("Income LoLa MiTi"),
        )
        .with_column(
            (col("Net MiTi (LoLa)") * lit(0.8))
                .round(2)
                .alias("Net MiTi (LoLA) - Share LoLa"),
        )
        .with_column(
            (col("Net Payment SumUp MiTi") - col("Net MiTi (LoLA) - Share LoLa"))
                .round(2)
                .alias("Debt to MiTi"),
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
            col("SoFe_Cash"),
            col("SoFe_Card"),
            col("SoFe Total"),
            col("Deposit_Cash"),
            col("Deposit_Card"),
            col("Deposit Total"),
            col("Rental_Cash"),
            col("Rental_Card"),
            col("Rental Total"),
            col("Culture_Cash"),
            col("Culture_Card"),
            col("Culture Total"),
            col("PaidOut_Cash"),
            col("PaidOut_Card"),
            col("PaidOut Total"),
            col("Gross Cash"),
            col("Tips_Cash"),
            col("SumUp Cash"),
            col("Gross Card"),
            col("Tips_Card"),
            col("SumUp Card"),
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
            col("Net Payment SumUp MiTi"),
            col("MiTi_Tips_Cash"),
            col("MiTi_Tips_Card"),
            col("MiTi_Tips"),
            col("Cafe_Tips"),
            col("Verm_Tips"),
            col("Gross MiTi (MiTi)"),
            col("Gross MiTi (LoLa)"),
            col("Gross MiTi (MiTi) Card"),
            col("Net MiTi (MiTi) Card"),
            col("Net MiTi (LoLa)"),
            col("Contribution MiTi"),
            col("Net MiTi (LoLA) - Share LoLa"),
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
        .group_by(["Date"])
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
                maintain_order: true,
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
/// For `Owner::MiTi`: topic and owner `MiTi`
/// For `Owner::LoLa`: Union of commission from `LoLa` sales via Mittagstisch
///                    as well as from non-Mittagstisch related sales
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

/// Total commission for topic
fn commission_by_topic(topic: &Topic) -> (Expr, String) {
    let expr = col("Topic").eq(lit(topic.to_string()));
    let alias = format!("{topic}_Total_Commission");
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
        .group_by(["Date"])
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
                maintain_order: true,
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
    use rstest::rstest;

    use crate::test_fixtures::{intermediate_df_02, summary_df_02};
    use crate::test_utils::assert_dataframe;

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
    #[case(Topic::Deposit, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["20.03.2023"],
            "Deposit_Card" => &[100.0]),
        )
    ]
    #[case(Topic::Culture, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["22.03.2023"],
            "Culture_Card" => &[400.0]),
        )
    ]
    #[case(Topic::Rental, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["23.03.2023"],
            "Rental_Card" => &[500.0]),
        )
    ]
    #[case(Topic::PaidOut, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["24.03.2023"],
            "PaidOut_Card" => &[600.0]),
        )
    ]
    #[case(Topic::SoFe, PaymentMethod::Cash, Purpose::Consumption,
        df!(
            "Date" => &["25.03.2023"],
            "SoFe_Cash" => &[10.0]),
        )
    ]
    #[case(Topic::SoFe, PaymentMethod::Card, Purpose::Consumption,
        df!(
            "Date" => &["25.03.2023"],
            "SoFe_Card" => &[20.0]),
        )
    ]
    fn test_collect_by(
        #[case] topic: Topic,
        #[case] payment_method: PaymentMethod,
        #[case] purpose: Purpose,
        #[case] expected: PolarsResult<DataFrame>,
    ) -> PolarsResult<()> {
        let df_in = df!(
            "Date" => &["16.03.2023", "14.03.2023", "15.03.2023", "28.03.2023", "20.03.2023", "14.03.2023", "20.03.2023", "22.03.2023", "23.03.2023", "24.03.2023", "25.03.2023", "25.03.2023"],
            "Price (Gross)" => &[None, Some(1.3), Some(5.2), Some(3.6), Some(4.7), Some(0.5), Some(100.0), Some(400.0), Some(500.0), Some(600.0), Some(10.0), Some(20.0)],
            "Topic" => &["Cafe", "Cafe", "MiTi", "Cafe", "Cafe", "Cafe", "Deposit", "Culture", "Rental", "PaidOut", "SoFe", "SoFe"],
            "Payment Method" => &["Card", "Card", "Card", "Card", "Cash", "Cash", "Card", "Card", "Card", "Card", "Cash", "Card"],
            "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Tip", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        )?;
        let paa = match purpose {
            Purpose::Consumption => consumption_of(&topic, &payment_method),
            Purpose::Tip => tips_of_topic(&topic),
        };
        let out = price_by_date_for(paa, df_in.lazy()).collect()?;

        assert_dataframe(&out, &expected.expect("Misconfigured expected dataframe"));

        Ok(())
    }

    #[rstest]
    fn test_collect_data(intermediate_df_02: DataFrame, summary_df_02: DataFrame) {
        let out = collect_data(intermediate_df_02).expect("should be able to collect the data");
        assert_dataframe(&out, &summary_df_02);
    }
}
