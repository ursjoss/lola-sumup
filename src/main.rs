#![warn(clippy::pedantic)]

use std::error::Error;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::prepare::prepare;

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
    /// Prepares an enriched working copy from the original sumup CSV export
    Prepare {
        /// the input file to process
        #[arg(short, long)]
        input_file: PathBuf,

        /// the output file
        #[arg(short, long)]
        output_file: Option<PathBuf>,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Prepare {
            input_file,
            output_file,
        } => prepare(input_file, output_file),
    }
}

#[test]
fn verify_cli() {
    use ::clap::CommandFactory;
    Cli::command().debug_assert();
}
