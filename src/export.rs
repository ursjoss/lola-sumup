use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
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
    let df_csv = CsvReader::from_path(input_path)?
        // .with_schema(Arc::new(myschema))
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()?;
    let x = df_csv
        .clone()
        .lazy()
        .filter(col("Topic").eq(lit("LoLa")))
        .with_column(col("Date").str().strptime(dt_options))
        .groupby(["Date"])
        .agg([col("Price (Gross)").sum()])
        .select([col("Date"), col("Price (Gross)")])
        .sort(
            "Date",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: false,
            },
        )
        .collect()?;
    println!("{df_csv}");
    println!("{x}");
    Ok(())
}
