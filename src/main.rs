#![warn(clippy::pedantic)]

use std::error::Error;
use std::path::PathBuf;

use clap::Parser;

use crate::prepare::prepare;

mod prepare;

#[derive(Parser)]
struct Cli {
    source_file: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Cli { source_file } = Cli::parse();

    prepare(&source_file)
}

#[test]
fn verify_cli() {
    use ::clap::CommandFactory;
    Cli::command().debug_assert();
}
