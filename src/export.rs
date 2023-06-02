use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{error::Error, io};

use polars::prelude::*;

use crate::export::constraint::validation_topic_owner;
use crate::export::export_accounting::gather_df_accounting;
use crate::export::export_miti::gather_df_miti;
use crate::export::export_summary::collect_data;

mod constraint;
mod export_accounting;
mod export_miti;
mod export_summary;

pub fn export(input_path: &Path, output_path: &Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;
    validation_topic_owner(&raw_df)?;

    let mut df = collect_data(raw_df)?;
    df.extend(&df.sum())?;

    write_summary(output_path, &mut df)?;
    write_additional_file(&mut gather_df_miti(&df)?, "miti.csv")?;
    write_additional_file(&mut gather_df_accounting(&df)?, "accounting.csv")
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
