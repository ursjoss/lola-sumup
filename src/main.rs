#![warn(clippy::pedantic)]

use std::env;
use std::error::Error;
use std::path::PathBuf;

use chrono::Local;
use clap::{Parser, Subcommand};

use crate::export::export;
use crate::prepare::prepare;

mod export;
mod prepare;

#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod test_utils;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Prepares an enriched intermediate file from the original `SumUp` sales report CSV and transaction report CSV.
    Prepare {
        /// the month for which transactions are to be processed (`<yyyymm>`, e.g. 202305)
        #[arg(value_parser = month_in_range)]
        month: String,

        /// the sales-report to process
        #[arg(short, long)]
        sales_report: PathBuf,

        /// the transaction-report to process
        #[arg(short, long)]
        transaction_report: PathBuf,
    },
    /// Consumes the (potentially redacted) intermediate file and exports to different special purpose CSV files.
    Export {
        /// the intermediate file to process
        intermediate_file: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    configure_the_environment();
    let cli = Cli::parse();
    let ts = &ts_now();
    match &cli.command {
        Commands::Prepare {
            month,
            sales_report,
            transaction_report,
        } => prepare(
            sales_report,
            transaction_report,
            &intermediate_file(month, ts),
        ),
        Commands::Export { intermediate_file } => {
            let file_name = intermediate_file.as_os_str().to_str();
            let month = derive_month_from(file_name)?;
            export(intermediate_file, &month, ts)
        }
    }
}

pub fn configure_the_environment() {
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1");
    env::set_var("POLARS_FMT_MAX_COLS", "70");
    env::set_var("POLARS_FMT_MAX_ROWS", "100");
    env::set_var("POLARS_FMT_STR_LEN", "50");
}

/// Returns the current timestamp in format `<yyyymmddHHMMSS>`.
fn ts_now() -> String {
    Local::now().format("%Y%m%d%H%M%S").to_string()
}

/// Ensures the validity of the value of the month parameter.
fn month_in_range(input: &str) -> Result<String, String> {
    if input.len() != 6 || input.parse::<i32>().is_err() {
        Err("month must conform with pattern <yyyymm>.".into())
    } else {
        let year = &input[..=3].parse::<i32>().expect("must pass");
        let month_string = &input[4..=5];
        let month = &month_string.parse::<i32>().expect("must pass");
        let valid_years = 2023..=2100;
        let valid_months = 1..=12;
        if !valid_years.contains(year) {
            Err(format!(
                "Unsupported year '{year}' - only years 2023 to 2100 are supported."
            ))
        } else if !valid_months.contains(month) {
            Err(format!("Incorrect month: {month_string}."))
        } else {
            Ok(input.into())
        }
    }
}

/// Provides path for the intermediate file (`intermediate_<yyyymm>_<yyyymmddHHMMSS>.csv`)
/// with `<yyyymm>` standing for the month being processed and
/// `<yyyymmddHHMMSS>` representing the execution timestamp.
fn intermediate_file(month: &String, ts: &String) -> PathBuf {
    PathBuf::from(format!("intermediate_{month}_{ts}.csv"))
}

/// Derives the month from the intermediate filename (e.g. `intermediate_<yyyymm>.csv` -> `<yyyymm>`)
fn derive_month_from(file: Option<&str>) -> Result<String, String> {
    let min = "intermediate_yyyymm.csv";
    let underscore_index = min.find('_').unwrap();
    let static_part = &min[0..=underscore_index];
    let Some(filename) = file else {
        return Err("Unable to derive filename for intermediate file.".into());
    };
    if filename.len() < min.len() {
        Err(format!(
            "Filename '{filename}' should have at least {} characters ({min}), but has only {}.",
            min.len(),
            filename.len()
        ))
    } else if !filename.starts_with(static_part) {
        Err(format!("Filename must start with '{static_part}'."))
    } else {
        let path = std::path::Path::new(filename);
        if path
            .extension()
            .map_or(false, |ext| ext.eq_ignore_ascii_case("csv"))
        {
            let start_index = underscore_index + 1;
            let end_index = start_index + 5;
            Ok(filename[start_index..=end_index].into())
        } else {
            Err("Filename must have extension .csv.".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::*;

    #[test]
    fn verify_cli() {
        use ::clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn test_ts_new() {
        let ts = ts_now();
        println!("timestamp: {ts}");
        assert!(ts.parse::<i64>().is_ok());
    }

    #[rstest]
    #[case("202301", "202301")]
    #[case("202412", "202412")]
    #[case("20231", "month must conform with pattern <yyyymm>.")]
    #[case("2023110", "month must conform with pattern <yyyymm>.")]
    #[case("202x11", "month must conform with pattern <yyyymm>.")]
    #[case(
        "202012",
        "Unsupported year '2020' - only years 2023 to 2100 are supported."
    )]
    #[case("202300", "Incorrect month: 00.")]
    #[case("202313", "Incorrect month: 13.")]
    fn test_month_in_range(#[case] input: String, #[case] expected: String) {
        let result = month_in_range(input.as_str());
        match result {
            Ok(month) => assert_eq!(month, expected),
            Err(msg) => assert_eq!(msg, expected),
        }
    }

    #[rstest]
    #[case(Some("intermediate_202303.csv"), "202303")]
    #[case(Some("intermediate_202312_mod.csv"), "202312")]
    #[case(None, "Unable to derive filename for intermediate file.")]
    #[case(Some("intermediat_202303.csv"), "Filename 'intermediat_202303.csv' should have at least 23 characters (intermediate_yyyymm.csv), but has only 22.")]
    #[case(
        Some("abcdefghijkl_202303.csv"),
        "Filename must start with 'intermediate_'."
    )]
    #[case(Some("intermediate_202303_.cs"), "Filename must have extension .csv.")]
    #[case(Some("intermediate_202303aaaa"), "Filename must have extension .csv.")]
    fn test_derive_month_from(#[case] input: Option<&str>, #[case] expected: String) {
        let result = derive_month_from(input);
        match result {
            Ok(month) => assert_eq!(month, expected),
            Err(msg) => assert_eq!(msg, expected),
        }
    }
}
