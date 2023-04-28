#![warn(clippy::pedantic)]

use std::error::Error;
use std::path::PathBuf;

use clap::Parser;

use crate::prepare::prepare;

mod prepare;

#[derive(Parser)]
struct Opts {
    source_file: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Opts { source_file } = Opts::parse();

    prepare(&source_file)
}
