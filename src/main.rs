#![warn(clippy::pedantic)]

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
    /// Prepares an enriched working copy from the original sumup sales report CSV file
    Prepare {
        /// the input file to process
        #[arg(short, long)]
        input_file: PathBuf,

        /// the input file with the commissions
        #[arg(short, long)]
        commission_file: PathBuf,

        /// the output file in intermediate format
        #[arg(short, long)]
        output_file: Option<PathBuf>,
    },
    // Exports
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

    match &cli.command {
        Commands::Prepare {
            input_file,
            commission_file,
            output_file,
        } => prepare(input_file, commission_file, output_file),
        Commands::Export {
            input_file,
            output_file,
        } => export(input_file, output_file),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use ::clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
