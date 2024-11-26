use std::error::Error;
use std::path::Path;

/// Processes the export from the accounting software, exporting the monthly closing sheet
pub fn do_closing(input_path: &Path, month: &str, ts: &str) -> Result<(), Box<dyn Error>> {
    let file_name = input_path.to_str().unwrap();
    println!("Processing {file_name} for month {month} and ts {ts}");
    Ok(())
}
