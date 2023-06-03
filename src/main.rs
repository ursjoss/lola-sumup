#![warn(clippy::pedantic)]

use chrono::Local;
use std::error::Error;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::export::export;
use crate::prepare::prepare;

mod export;
mod prepare;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Prepares an enriched intermediate file from the original sumup sales report CSV and transaction report CSV.
    Prepare {
        /// the month for which transactions are to be processed (<yyyymm>, e.g. 202305)
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
        /// the intermediate input file to process
        #[arg(short, long)]
        input_file: PathBuf,

        /// the exported
        #[arg(short, long)]
        output_file: Option<PathBuf>,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
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
        Commands::Export {
            input_file,
            output_file,
        } => export(input_file, output_file),
    }
}

fn ts_now() -> String {
    Local::now().format("%Y%m%d%H%M%S").to_string()
}

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

fn intermediate_file(month: &String, ts: &String) -> PathBuf {
    PathBuf::from(format!("intermediate_{month}_{ts}.csv"))
}

#[cfg(test)]
mod tests {
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
}
