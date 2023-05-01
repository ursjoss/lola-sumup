use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::prepare::{PaymentMethod, Topic};

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let dt_options = StrpTimeOptions {
        date_dtype: DataType::Date,
        fmt: Some("%d.%m.%Y".into()),
        strict: true,
        exact: true,
        ..Default::default()
    };
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()?;
    let ldf = raw_df
        .lazy()
        .with_column(col("Date").str().strptime(dt_options));
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
        predicate_and_alias(&Topic::LoLa, &PaymentMethod::Cash),
    );
    let lola_card = collect_by(
        ldf.clone(),
        predicate_and_alias(&Topic::LoLa, &PaymentMethod::Card),
    );
    let miti_cash = collect_by(
        ldf.clone(),
        predicate_and_alias(&Topic::MiTi, &PaymentMethod::Cash),
    );
    let miti_card = collect_by(
        ldf.clone(),
        predicate_and_alias(&Topic::MiTi, &PaymentMethod::Card),
    );
    let verm_cash = collect_by(
        ldf.clone(),
        predicate_and_alias(&Topic::Vermietung, &PaymentMethod::Cash),
    );
    let verm_card = collect_by(
        ldf,
        predicate_and_alias(&Topic::Vermietung, &PaymentMethod::Card),
    );
    let comb1 = all_dates.join(lola_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb2 = comb1.join(lola_card, [col("Date")], [col("Date")], JoinType::Left);
    let comb3 = comb2.join(miti_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb4 = comb3.join(miti_card, [col("Date")], [col("Date")], JoinType::Left);
    let comb5 = comb4.join(verm_cash, [col("Date")], [col("Date")], JoinType::Left);
    let comb6 = comb5.join(verm_card, [col("Date")], [col("Date")], JoinType::Left);
    let mut combined = comb6
        .with_column(
            (col("LoLa_Cash").fill_null(0.0) + col("LoLa_Card").fill_null(0.0)).alias("LoLa_Total"),
        )
        .with_column(
            (col("MiTi_Cash").fill_null(0.0) + col("MiTi_Card").fill_null(0.0)).alias("MiTi_Total"),
        )
        .with_column(
            (col("Vermietung_Cash").fill_null(0.0) + col("Vermietung_Card").fill_null(0.0))
                .alias("Vermietung_Total"),
        )
        .with_column(
            (col("LoLa_Cash").fill_null(0.0)
                + col("MiTi_Cash").fill_null(0.0)
                + col("Vermietung_Cash").fill_null(0.0))
            .alias("Cash_Total"),
        )
        .with_column(
            (col("LoLa_Card").fill_null(0.0)
                + col("MiTi_Card").fill_null(0.0)
                + col("Vermietung_Card").fill_null(0.0))
            .alias("Card_Total"),
        )
        .with_column(
            (col("LoLa_Total").fill_null(0.0)
                + col("MiTi_Total").fill_null(0.0)
                + col("Vermietung_Total").fill_null(0.0))
            .alias("Total"),
        )
        .select([
            col("Date"),
            col("LoLa_Cash"),
            col("LoLa_Card"),
            col("LoLa_Total"),
            col("MiTi_Cash"),
            col("MiTi_Card"),
            col("MiTi_Total"),
            col("Vermietung_Cash"),
            col("Vermietung_Card"),
            col("Vermietung_Total"),
            col("Cash_Total"),
            col("Card_Total"),
            col("Total"),
        ])
        .collect()?;

    let iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b',')
        .finish(&mut combined)?;
    Ok(())
}

fn predicate_and_alias(topic: &Topic, payment_method: &PaymentMethod) -> (Expr, String) {
    let expr = (col("Topic").eq(lit(topic.to_string())))
        .and(col("Payment Method").eq(lit(payment_method.to_string())));
    let alias = format!("{topic}_{payment_method}");
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
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Topic::LoLa, PaymentMethod::Card,
        df!(
            "Date" => &["14.03.2023", "16.03.2023", "28.03.2023"],
            "LoLa_Card" => &[1.3, 0.0, 3.6]),
        )
    ]
    #[case(Topic::LoLa, PaymentMethod::Cash,
        df!(
            "Date" => &["20.03.2023"],
            "LoLa_Cash" => &[4.7]),
        )
    ]
    #[case(Topic::MiTi, PaymentMethod::Card,
        df!(
            "Date" => &["15.03.2023"],
            "MiTi_Card" => &[5.2]),
        )
    ]
    fn test(
        #[case] topic: Topic,
        #[case] payment_method: PaymentMethod,
        #[case] expected: PolarsResult<DataFrame>,
    ) {
        let df_in = df!(
            "Date" => &["16.03.2023", "14.03.2023", "15.03.2023", "28.03.2023", "20.03.2023"],
            "Price (Gross)" => &[None, Some(1.3), Some(5.2), Some(3.6), Some(4.7)],
            "Topic" => &["LoLa", "LoLa", "MiTi", "LoLa", "LoLa"],
            "Payment Method" => &["Card", "Card", "Card", "Card", "Cash"]
        )
        .expect("Misconfigured dataframe");
        let paa = predicate_and_alias(&topic, &payment_method);
        let out = collect_by(df_in.lazy(), paa)
            .collect()
            .expect("Unable to collect result");
        assert_eq!(out, expected.expect("Misconfigured expected df"));
    }
}
