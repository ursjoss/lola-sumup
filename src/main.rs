#![warn(clippy::pedantic)]

use std::env;
use std::error::Error;
use std::ffi::OsString;

use crate::prepare::prepare;

mod prepare;

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    prepare(file_path)
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}
