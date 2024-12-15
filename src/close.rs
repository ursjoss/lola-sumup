use crate::close::close_xlsx::do_closing_xlsx;
use crate::close::close_xml::do_closing_xml;
use crate::derive_month_from_accounts;

use std::error::Error;
use std::path::Path;

mod close_xlsx;
mod close_xml;

/// Read the file with the accounts information and create the closing file
pub fn close(accounts_file: &Path, ts: &str) -> Result<(), Box<dyn Error>> {
    let file_name = accounts_file.as_os_str().to_str();
    if let Some(extension) = accounts_file.extension().and_then(|e| e.to_str()) {
        let month = derive_month_from_accounts(file_name, extension)?;
        match extension {
            "xlsx" => do_closing_xlsx(accounts_file, &month, ts),
            "xls" => do_closing_xml(accounts_file, &month, ts),
            _ => Err(Box::from(format!(
                "File extension {extension} is not supported."
            ))),
        }
    } else {
        Err("No valid file extension found".into())
    }
}
