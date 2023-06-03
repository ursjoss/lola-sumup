use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::export::constraint::validation_topic_owner;
use crate::export::export_accounting::{gather_df_accounting, validate_acc_constraint};
use crate::export::export_miti::gather_df_miti;
use crate::export::export_summary::collect_data;

mod constraint;
mod export_accounting;
mod export_miti;
mod export_summary;

/// Reads the intermediate files and exports all configured reports.
pub fn export(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let raw_df = CsvReader::from_path(input_path)?
        .has_header(true)
        .with_delimiter(b';')
        .with_try_parse_dates(true)
        .finish()?;
    validation_topic_owner(&raw_df)?;

    let mut df = collect_data(raw_df)?;
    df.extend(&df.sum())?;

    write_to_file(&mut df, &path_with_prefix("summary", month, ts))?;
    write_to_file(
        &mut gather_df_miti(&df)?,
        &path_with_prefix("mittagstisch", month, ts),
    )?;
    let mut df_acc = gather_df_accounting(&df)?;
    validate_acc_constraint(&df_acc)?;
    write_to_file(&mut df_acc, &path_with_prefix("accounting", month, ts))
}

/// Constructs a path for a CSV file from `prefix`, `month` and `ts` (timestamp).
fn path_with_prefix(prefix: &str, month: &str, ts: &str) -> PathBuf {
    PathBuf::from(format!("{prefix}_{month}_{ts}.csv"))
}

/// Writes the dataframe `df` to the file system into path `path`.
fn write_to_file(df: &mut DataFrame, path: &dyn AsRef<Path>) -> Result<(), Box<dyn Error>> {
    let iowtr: Box<dyn Write> = Box::new(File::create(path)?);
    CsvWriter::new(iowtr)
        .has_header(true)
        .with_delimiter(b';')
        .finish(&mut df.clone())?;
    Ok(())
}
