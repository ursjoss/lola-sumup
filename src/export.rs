use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::prepare::{PaymentMethod, Topic};

pub fn export(
    input_path: &Path,
    output_path: &Option<PathBuf>,
    verbose: bool,
) -> Result<(), Box<dyn Error>> {
    let _iowtr: Box<dyn Write> = match output_path {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
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
    let df = raw_df
        .lazy()
        .with_column(col("Date").str().strptime(dt_options))
        .collect()?;
    let all_dates = df
        .clone()
        .lazy()
        .select([col("Date")])
        .unique(None, UniqueKeepStrategy::First)
        .sort(
            "Date",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: false,
            },
        )
        .collect()?;
    let lola_cash = collect_by(&df, predicate_and_alias(&Topic::LoLa, &PaymentMethod::Cash))?;
    let lola_card = collect_by(&df, predicate_and_alias(&Topic::LoLa, &PaymentMethod::Card))?;
    let miti_cash = collect_by(&df, predicate_and_alias(&Topic::MiTi, &PaymentMethod::Cash))?;
    let miti_card = collect_by(&df, predicate_and_alias(&Topic::MiTi, &PaymentMethod::Card))?;
    let verm_cash = collect_by(
        &df,
        predicate_and_alias(&Topic::Vermietung, &PaymentMethod::Cash),
    )?;
    let verm_card = collect_by(
        &df,
        predicate_and_alias(&Topic::Vermietung, &PaymentMethod::Card),
    )?;
    let comb1 = all_dates.join(&lola_cash, ["Date"], ["Date"], JoinType::Left, None)?;
    let comb2 = comb1.join(&lola_card, ["Date"], ["Date"], JoinType::Left, None)?;
    let comb3 = comb2.join(&miti_cash, ["Date"], ["Date"], JoinType::Left, None)?;
    let comb4 = comb3.join(&miti_card, ["Date"], ["Date"], JoinType::Left, None)?;
    let comb5 = comb4.join(&verm_cash, ["Date"], ["Date"], JoinType::Left, None)?;
    let comb6 = comb5.join(&verm_card, ["Date"], ["Date"], JoinType::Left, None)?;
    let mut combined = comb6.lazy().collect()?;

    if verbose {
        println!("{df}");
        println!("{all_dates}");
        println!("{lola_cash}");
        println!("{lola_card}");
        println!("{miti_cash}");
        println!("{miti_card}");
        println!("{verm_cash}");
        println!("{verm_card}");
        println!("{combined}");
    }

    let mut file = File::create("overview.csv")?;

    CsvWriter::new(&mut file)
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

fn collect_by(df: &DataFrame, predicate_and_alias: (Expr, String)) -> PolarsResult<DataFrame> {
    let (predicate, alias) = predicate_and_alias;
    df.clone()
        .lazy()
        .filter(predicate)
        .groupby(["Date"])
        .agg([col("Price (Gross)").sum()])
        .select([col("Date"), col("Price (Gross)").alias(alias.as_str())])
        .sort(
            "Date",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: false,
            },
        )
        .collect()
}
