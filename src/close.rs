use crate::close::close_xml::do_closing_xml;
use crate::close::close_xml::read_budget_config;
use crate::derive_month_from_accounts;
use crate::export::path_with_prefix;

use polars::prelude::*;
use polars_excel_writer::PolarsExcelWriter;
use rust_xlsxwriter::{Color, Format, FormatAlign, Workbook};
use std::error::Error;
use std::path::Path;
use std::vec;

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
                let df = do_closing_xml(accounts_file, budget, &month)?;
                write_closing_to_file(&df, "closing", &month, ts)?;
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

/// Creates a dataframe with dummy postings to extend the actual postings from the ledger
/// The purpose is to have at least one real or dummy posting per post.
fn as_dataframe(accounts: Vec<String>, month: &str) -> PolarsResult<DataFrame> {
    let length = accounts.len();
    let year: String = month.chars().take(4).collect();
    let dates = vec![format!("{year}-01-01"); length];
    let descriptions = vec!["Dummy post"; length];
    let amounts = vec![0.0; length];
    df!(
        "Date" => dates,
        "Description" => descriptions,
        "Debit" => accounts.clone(),
        "Credit" => accounts,
        "Amount" => amounts,
    )
}

fn write_closing_to_file(
    df: &DataFrame,
    prefix: &str,
    month: &str,
    ts: &str,
) -> Result<(), Box<dyn Error>> {
    let path = &path_with_prefix(prefix, month, ts);
    let mut excel_writer = PolarsExcelWriter::new();

    excel_writer.set_autofit(true);

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet().set_name(prefix)?;
    excel_writer.set_freeze_panes(1, 1);

    excel_writer.write_dataframe_to_worksheet(df, worksheet, 0, 0)?;

    let last_row = u32::try_from(df.shape().0)? + 2;
    //    let last_col = u16::try_from(df.shape().1)?;

    // Budget groups and posts
    let col = 0;
    let format = Format::new().set_background_color(Color::RGB(0x00B2_B2B2));
    worksheet.set_range_format(0, col, last_row, col, &format)?;
    let format = format.clone().set_bold();
    worksheet.write_with_format(0, col, "", &format)?;
    worksheet.write_with_format(last_row, col, "Rein-Gew./-Verlust", &format)?;

    // Budget
    let col = 1;
    let format = Format::new()
        .set_background_color(Color::RGB(0x00b4_c7dc))
        .set_num_format("#'##0.00");
    worksheet.set_range_format(0, col, last_row, col, &format)?;
    let format = format.clone().set_bold().set_align(FormatAlign::Right);
    worksheet.set_range_format(0, col, 0, col, &format)?;

    // Months
    let first_col = 2;
    let last_col = 2;
    let format = Format::new()
        .set_background_color(Color::RGB(0x00ff_ffd7))
        .set_num_format("#'##0.00");
    worksheet.set_range_format(0, first_col, last_row, last_col, &format)?;
    let format = format.clone().set_bold().set_align(FormatAlign::Right);
    worksheet.set_range_format(0, first_col, 0, last_col, &format)?;

    // Verbleibend
    let col = 3;
    let format = Format::new()
        .set_background_color(Color::RGB(0x00af_d095))
        .set_num_format("#'##0.00");
    worksheet.set_range_format(0, col, last_row, col, &format)?;
    let format = format.clone().set_bold().set_align(FormatAlign::Right);
    worksheet.set_range_format(0, col, 0, col, &format)?;

    workbook.save(path)?;
    Ok(())
}
