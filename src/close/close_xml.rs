use chrono::NaiveDate;
use polars::prelude::*;
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::close::as_dataframe;
use crate::export::get_last_of_month_nd;

/// read account information from xml file with ending .xls
pub fn do_closing_xml(
    input_path: &Path,
    budget: Budget,
    month: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let journal = read_xml(input_path)?;
    let dummy_accounts = as_dataframe(budget.get_first_account_per_post(), month)?;
    let extended = journal.vstack(&dummy_accounts)?;
    let aggregated = aggregate_balances(&extended, budget, month)?;
    Ok(aggregated)
}

/// reads the Excel XML format, extracting columns from sheet Journal:
/// Date, Description, Debit, Credit, Amount.
fn read_xml(input_path: &Path) -> Result<DataFrame, Box<dyn Error>> {
    let file = BufReader::new(File::open(input_path)?);
    let mut reader = Reader::from_reader(file);

    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut in_sheet = false;
    let mut in_cell = false;
    let mut cell_value = String::new();
    let mut index: Option<u32> = None;
    let mut row: HashMap<u32, String> = HashMap::new();
    let mut df = new_empty_frame()?;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"Worksheet" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"ss:Name" {
                            let raw_value = str::from_utf8(attr.value.as_ref())?;
                            let unescaped_value = unescape(raw_value)?;
                            if unescaped_value == Sheet::Journal.name() {
                                in_sheet = true;
                            }
                        }
                    }
                }
                b"Cell" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"ss:Index" {
                            let raw_value = str::from_utf8(attr.value.as_ref())?;
                            let unescaped_value = unescape(raw_value)?;
                            index = Some(unescaped_value.parse::<u32>()?);
                            in_cell = in_sheet;
                        }
                    }
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name().as_ref() {
                b"Worksheet" => {
                    cell_value.clear();
                    in_sheet = false;
                    in_cell = false;
                    row.clear();
                }
                b"Cell" => {
                    if in_cell {
                        if let Some(i) = index {
                            row.insert(i, cell_value.clone());
                        }
                        in_cell = false;
                    }
                }
                b"Row" => {
                    if in_sheet {
                        let date_index = JournalColumn::Date as u32;
                        let description_index = JournalColumn::Description as u32;
                        let debit_index = JournalColumn::Debit as u32;
                        let credit_index = JournalColumn::Credit as u32;
                        let amount_index = JournalColumn::Amount as u32;
                        let mut df_row = new_row(
                            &row.remove(&date_index).unwrap_or("1/1/2100".into()),
                            row.remove(&description_index)
                                .unwrap_or("description missing".into()),
                            &row.remove(&debit_index).unwrap_or("debit missing".into()),
                            &row.remove(&credit_index).unwrap_or("credit missing".into()),
                            &row.remove(&amount_index).unwrap_or("0.0".into()),
                        )?;
                        df_row = df_row.fill_null(FillNullStrategy::Zero)?;
                        let filtered = df_row
                            .clone()
                            .lazy()
                            .filter(
                                col("Date").is_not_null().and(
                                    col("Date")
                                        .str()
                                        .contains(lit(r"^\d{4}-\d{2}-\d{2}$"), false),
                                ),
                            )
                            .select([col("Date")])
                            .collect()
                            .unwrap();
                        if filtered.shape().0 > 0 {
                            df = df.vstack(&df_row)?;
                        }
                        row.clear();
                    }
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if in_sheet && in_cell {
                    cell_value = e.decode()?.into_owned();
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(Box::from(e)),
            _ => {}
        }
        buf.clear();
    }
    Ok(df)
}

/// calculates the first day to be ignored, e.g.
/// returns 2025-08-01 for month 202507
fn cut_off_date(last_valid_month: &str) -> String {
    let year: i32 = last_valid_month[0..4].parse().unwrap();
    let month: u32 = last_valid_month[4..6].parse().unwrap();
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1).map_or("2000-01-01".to_string(), |d| {
        d.format("%Y-%m-%d").to_string()
    })
}

fn new_row(
    date: &str,
    description: String,
    debit: &str,
    credit: &str,
    amount: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let date_trunc = &date[..10.min(date.len())];
    let amount_numeric: f64 = amount.parse().unwrap_or(0.0);
    new_row_with_vecs(
        vec![date_trunc.to_string()],
        vec![description],
        vec![debit.into()],
        vec![credit.into()],
        vec![amount_numeric],
    )
}

fn new_empty_frame() -> Result<DataFrame, Box<dyn Error>> {
    new_row_with_vecs(
        Vec::<String>::new(),
        Vec::<String>::new(),
        Vec::<String>::new(),
        Vec::<String>::new(),
        Vec::<f64>::new(),
    )
}

fn new_row_with_vecs(
    date: Vec<String>,
    description: Vec<String>,
    debit: Vec<String>,
    credit: Vec<String>,
    amount: Vec<f64>,
) -> Result<DataFrame, Box<dyn Error>> {
    let df = DataFrame::new(vec![
        Column::new(
            JournalColumn::Date.name().into(),
            Series::new(JournalColumn::Date.name().into(), date),
        ),
        Column::new(
            JournalColumn::Description.name().into(),
            Series::new(JournalColumn::Description.name().into(), description),
        ),
        Column::new(
            JournalColumn::Debit.name().into(),
            Series::new(JournalColumn::Debit.name().into(), debit),
        ),
        Column::new(
            JournalColumn::Credit.name().into(),
            Series::new(JournalColumn::Credit.name().into(), credit),
        ),
        Column::new(
            JournalColumn::Amount.name().into(),
            Series::new(JournalColumn::Amount.name().into(), amount),
        ),
    ])?;
    Ok(df)
}

/// gets the balances and aggregates them on budget level
fn aggregate_balances(
    journal: &DataFrame,
    budget: Budget,
    month: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let balances = get_balances_from(journal, month)?;
    let aggregated = enrich_and_aggregate(&balances, budget, month)?;
    Ok(aggregated)
}

/// Transforms the journal into balances for the relevant accounts up to and including the month specified.
fn get_balances_from(journal: &DataFrame, month: &str) -> Result<DataFrame, Box<dyn Error>> {
    let debits = journal
        .clone()
        .lazy()
        .select([col("Debit").alias("Account"), col("Amount"), col("Date")])
        .collect()?;
    let credits = journal
        .clone()
        .lazy()
        .select([col("Credit").alias("Account"), -col("Amount"), col("Date")])
        .collect()?;
    let debits_and_credits = debits.vstack(&credits)?;

    let cut_off_date = cut_off_date(month);
    let balances = debits_and_credits
        .clone()
        .lazy()
        .filter(
            col("Account")
                .str()
                .contains(lit(r"^[3456789]\d{3,4}$"), false)
                .and(col("Date").lt(lit(cut_off_date.clone()))),
        )
        .group_by(["Account"])
        .agg(&[col("Amount").sum().alias("Balance")])
        .collect()?;
    Ok(balances)
}

/// enriches the balances with budget
fn enrich_and_aggregate(
    balances: &DataFrame,
    budget: Budget,
    month: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let year = month.chars().take(4).collect::<String>();
    let budget_alias = format!("Budget {year}");
    let lom = get_last_of_month_nd(month)?;
    let lom_formatted = lom.format("%-d.%-m.%y");
    let month_alias = format!("1.1.-{lom_formatted}");
    let budget = Arc::new(budget);
    let b1 = budget.clone();
    let b2 = budget.clone();
    let b3 = budget.clone();
    let b4 = budget;
    let enriched = balances
        .clone()
        .lazy()
        .with_column(
            col("Account")
                .map(
                    move |a| get_name_of_post(&a, &b1),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
                )
                .alias("Group"),
        )
        .with_column(
            col("Account")
                .map(
                    move |a| get_factor_of_post(&a, &b2),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int8)),
                )
                .alias("Factor"),
        )
        .with_column(
            col("Account")
                .map(
                    move |a| get_sort_of_post(&a, &b3),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int8)),
                )
                .alias("Sort"),
        )
        .with_column(
            col("Account")
                .map(
                    move |a| get_budget_of_post(&a, &b4, &year.clone()),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
                )
                .alias("Budget"),
        )
        .filter(
            // Exceptional accounts that need not be included
            col("Account").neq(lit("8900")),
        )
        .collect()?;

    validate_all_accounts_are_in_budget(&enriched)?;

    //    let x = budget
    //        .join(
    //            enriched.clone(),
    //            [col("Group")],
    //            [col("Account")],
    //            JoinType::Left.into(),
    //        )
    //        .collect()?;

    let aggregated = enriched
        .clone()
        .lazy()
        .with_column((col("Balance").fill_null(lit(0.0)) * col("Factor")).alias("Net"))
        .group_by(["Group", "Sort", "Budget", "Factor"])
        .agg(&[col("Net").sum()])
        .sort(["Sort"], SortMultipleOptions::default())
        .select([
            col("Group"),
            col("Budget").alias(budget_alias),
            col("Net").alias(month_alias),
            ((col("Budget") - col("Net")) * col("Factor")).alias("Verbleibend"),
        ])
        .collect()?;
    Ok(aggregated)
}

// validates the processed data does not contain any accounts that are not in the budget
fn validate_all_accounts_are_in_budget(enriched: &DataFrame) -> Result<(), Box<dyn Error>> {
    let unmatched = enriched
        .clone()
        .lazy()
        .filter(col("Group").is_null())
        .collect()?;
    if unmatched.shape().0 > 0 {
        let row_vec = unmatched.get_row(0).unwrap().0;
        let account = row_vec.first().unwrap().clone();
        println!("{unmatched:?}");
        Err(format!("Account {account} is not considered in the budget definition").into())
    } else {
        Ok(())
    }
}

/// Finds the descriptions of the budget post for given account
fn get_name_of_post(col: &Column, budget: &Budget) -> PolarsResult<Column> {
    let accounts = col.str()?;
    Ok(accounts
        .into_iter()
        .map(|a| {
            a.map(|a| budget.get_post_by_account(a).map(|p| p.name.to_string()))
                .or(None)?
        })
        .collect::<StringChunked>()
        .into_column())
}

/// Finds the budget amount of the budget post for given account and year
fn get_budget_of_post(col: &Column, budget: &Budget, year: &str) -> PolarsResult<Column> {
    let accounts = col.str()?;
    Ok(accounts
        .into_iter()
        .map(|a| a.map(|a| budget.get_budget_amount_by_account(a, year)))
        .collect::<Float64Chunked>()
        .into_column())
}

/// Finds the sort for the post for given account
fn get_sort_of_post(col: &Column, budget: &Budget) -> PolarsResult<Column> {
    get_int_from_post(col, budget, |p| p.sort)
}

/// Finds the factor for the post for given account
fn get_factor_of_post(col: &Column, budget: &Budget) -> PolarsResult<Column> {
    get_int_from_post(col, budget, |p| p.factor)
}

fn get_int_from_post<F>(col: &Column, budget: &Budget, int_extractor: F) -> PolarsResult<Column>
where
    F: Fn(&Post) -> i64 + Copy,
{
    let accounts = col.str()?;
    Ok(accounts
        .into_iter()
        .map(|a| {
            a.map(|a| budget.get_post_by_account(a).map(int_extractor))
                .or(None)?
        })
        .collect::<Int64Chunked>()
        .into_column())
}

/// The worksheets in the workbook
#[derive(Debug, Clone, Copy)]
enum Sheet {
    _Accounts = 0,
    _Totals = 1,
    Journal = 2,
    _FileInfo = 3,
}

impl Sheet {
    fn name(self) -> &'static str {
        match self {
            Sheet::_Accounts => "Accounts",
            Sheet::_Totals => "Totals",
            Sheet::Journal => "Journal",
            Sheet::_FileInfo => "FileInfo",
        }
    }
}

/// The columns in the worksheet Journal, index is one-based
#[derive(Debug, Clone, Copy)]
enum JournalColumn {
    Date = 8,
    Description = 20,
    Debit = 22,
    Credit = 24,
    Amount = 29,
}

impl JournalColumn {
    fn name(self) -> &'static str {
        match self {
            JournalColumn::Date => "Date",
            JournalColumn::Description => "Description",
            JournalColumn::Debit => "Debit",
            JournalColumn::Credit => "Credit",
            JournalColumn::Amount => "Amount",
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Budget {
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "post_groups")]
    _post_groups: HashMap<String, PostGroup>,
    posts: HashMap<String, Post>,
    years: HashMap<String, Year>,
}

#[derive(Deserialize, Debug, Clone)]
struct PostGroup {
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "posts")]
    _posts: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
struct Post {
    pub name: String,
    account_codes: Vec<String>,
    sort: i64,
    factor: i64,
}

#[derive(Deserialize, Debug, Clone)]
struct Year {
    amounts: HashMap<String, f64>,
}

impl Budget {
    /// Get the post by account code
    fn get_post_by_account(&self, account: &str) -> Option<&Post> {
        self.posts
            .values()
            .find(|p| p.account_codes.contains(&account.to_string()))
    }

    fn get_post_key_by_account(&self, account: &str) -> Option<String> {
        self.posts
            .iter()
            .find(|(_, p)| p.account_codes.contains(&account.to_string()))
            .map(|(k, _)| k.to_string())
    }

    /// get the amount for the given budget account and year
    fn get_budget_amount_by_account(&self, account: &str, year: &str) -> f64 {
        let post_key_option = self.get_post_key_by_account(account);
        if let Some(post_key) = post_key_option {
            let x = self.years.get(year);
            x.and_then(|y| y.amounts.get(&post_key))
                .copied()
                .unwrap_or(-0.0)
        } else {
            -0.0
        }
    }

    /// returns the first account per post
    fn get_first_account_per_post(&self) -> Vec<String> {
        self.posts
            .values()
            .filter_map(|post| post.account_codes.first())
            .cloned()
            .collect()
    }
}

pub fn read_budget_config(budget_config_file: &Path) -> Result<Budget, Box<dyn Error>> {
    let toml_str = fs::read_to_string(budget_config_file)?;
    let budget: Budget = toml::from_str(&toml_str)?;
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_fixtures::{aggregated_df_01_202505, aggregated_df_01_202507, journal_df_01},
        test_utils::assert_dataframe,
    };
    use rstest::rstest;
    use std::path::PathBuf;

    fn read_budget_from_samples() -> Budget {
        let file_path = "samples/budget.toml".to_string();
        let config_file = &PathBuf::from(file_path);
        let budget = read_budget_config(config_file);
        let Ok(budget) = budget else {
            panic!("Invalid budget: {budget:#?}");
        };
        budget
    }

    #[rstest]
    fn can_read_from_sample_config_file() {
        let budget = read_budget_from_samples();
        let Some(post) = budget.get_post_by_account("30100") else {
            panic!("Account 30100 not found in budget");
        };
        assert_eq!("Ertrag Restauration", post.name);
    }

    #[rstest]
    fn can_run_closing() {
        let budget = read_budget_from_samples();

        let data = "samples/konten_202412_20250128132200.xls".to_string();
        let data_file = &PathBuf::from(data);
        let _result = do_closing_xml(data_file, budget, "202410")
            .expect("Unable to process sample data file.");
    }

    #[rstest]
    fn can_get_dummy_posts() {
        let budget = read_budget_from_samples();
        let accounts = budget.get_first_account_per_post();
        assert_ne!(accounts.len(), 0);
    }

    #[rstest]
    #[case("202411", "2024-12-01")]
    #[case("202412", "2025-01-01")]
    #[case("202507", "2025-08-01")]
    fn test_cut_off_date(#[case] month: &str, #[case] expected: &str) {
        let actual = cut_off_date(month);
        assert_eq!(actual, expected);
    }

    #[rstest]
    fn test_aggregate_balances_by_202505(
        journal_df_01: DataFrame,
        aggregated_df_01_202505: DataFrame,
    ) {
        test_aggregate_balances_by("202505", journal_df_01, aggregated_df_01_202505);
    }

    #[rstest]
    fn test_aggregate_balances_by_202507(
        journal_df_01: DataFrame,
        aggregated_df_01_202507: DataFrame,
    ) {
        test_aggregate_balances_by("202507", journal_df_01, aggregated_df_01_202507);
    }

    fn test_aggregate_balances_by(month: &str, journal: DataFrame, expected: DataFrame) {
        let budget = read_budget_from_samples();
        let actual = aggregate_balances(&journal, budget, month).expect("can aggregate balances");
        assert_dataframe(&actual, &expected);
    }
}
