use crate::close::close_xml::do_closing_xml;
use crate::close::close_xml::Budget;
use crate::derive_month_from_accounts;

use std::error::Error;
use std::fs;
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
            "xls" => do_closing_xml(accounts_file, budget, &month, ts),
            _ => Err(Box::from(format!(
                "File extension {extension} is not supported."
            ))),
        }
    } else {
        Err("No valid file extension found".into())
    }
}

fn read_budget_config(budget_config_file: &Path) -> Result<Budget, Box<dyn Error>> {
    let toml_str = fs::read_to_string(budget_config_file)?;
    let budget: Budget = toml::from_str(&toml_str)?;
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::path::PathBuf;

    #[rstest]
    fn can_read_from_sample_config_file() {
        let file_path = "sample_config/budget.toml".to_string();
        let config_file = &PathBuf::from(file_path);
        let config = read_budget_config(config_file);
        let Ok(config) = config else {
            panic!("Invalid config: {config:#?}");
        };
        assert_eq!("LoLa Budget", config._name);
    }
}
