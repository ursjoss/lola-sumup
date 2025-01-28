use crate::close::close_xml::do_closing_xml;
use crate::close::close_xml::read_budget_config;
use crate::derive_month_from_accounts;

use std::error::Error;
use std::path::Path;

mod close_xml;

/// Read the file with the accounts information and create the closing file
pub fn close(
    budget_config_file: &Path,
    accounts_file: &Path,
    ts: &str,
) -> Result<(), Box<dyn Error>> {
    let account_file_name = accounts_file.as_os_str().to_str();
    if let Some(extension) = accounts_file.extension().and_then(|e| e.to_str()) {
        let month = derive_month_from_accounts(account_file_name, extension)?;
        let budget = read_budget_config(budget_config_file)?;
        match extension {
            "xls" => {
                let _ = do_closing_xml(accounts_file, budget, &month, ts);
                Ok(())
            }
            _ => Err(Box::from(format!(
                "File extension {extension} is not supported."
            ))),
        }
    } else {
        Err("No valid file extension found".into())
    }
}
