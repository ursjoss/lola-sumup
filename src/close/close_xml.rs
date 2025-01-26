use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// read account information from xml file with ending .xls
pub fn do_closing_xml(
    input_path: &Path,
    budget: Budget,
    _month: &str,
    _ts: &str,
) -> Result<(), Box<dyn Error>> {
    let b1 = budget.clone();
    let b2 = budget.clone();
    let balances = read_xml(input_path)?;
    let enriched = balances
        .clone()
        .lazy()
        .with_column(
            col("Account")
                .apply(
                    move |a| get_name_of_post(&a, &b1),
                    GetOutput::from_type(DataType::String),
                )
                .alias("Group"),
        )
        .with_column(
            col("Account")
                .apply(
                    move |a| get_factor_of_post(&a, &b2),
                    GetOutput::from_type(DataType::Int8),
                )
                .alias("Factor"),
        )
        .with_column(
            col("Account")
                .apply(
                    move |a| get_sort_of_post(&a, &budget),
                    GetOutput::from_type(DataType::Int8),
                )
                .alias("Sort"),
        )
        .filter(
            // Exceptional accounts that need not be included
            col("Account")
                .neq(lit("8900"))
                .or(col("Debit").neq(lit(0.0)))
                .or(col("Credit").neq(lit(0.0))),
        )
        .collect()?;

    validate_all_accounts_are_in_budget(&enriched)?;

    let aggregated = enriched
        .clone()
        .lazy()
        .with_column((col("Balance").fill_null(lit(0.0)) * col("Factor")).alias("Net"))
        .group_by(["Group", "Sort"])
        .agg(&[col("Net").sum()])
        .sort(["Sort"], SortMultipleOptions::default())
        .select([col("Group"), col("Net")])
        .collect()?;
    println!("{aggregated:?}");
    Ok(())
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
fn get_name_of_post(col: &Column, budget: &Budget) -> PolarsResult<Option<Column>> {
    let accounts = col.str()?;
    Ok(Some(
        accounts
            .into_iter()
            .map(|a| {
                a.map(|a| budget.get_post_by_account(a).map(|p| p.name.to_string()))
                    .or(None)?
            })
            .collect::<StringChunked>()
            .into_column(),
    ))
}

/// Finds the sort for the post for given account
fn get_sort_of_post(col: &Column, budget: &Budget) -> PolarsResult<Option<Column>> {
    get_int_from_post(col, budget, |p| p.sort)
}

/// Finds the factor for the post for given account
fn get_factor_of_post(col: &Column, budget: &Budget) -> PolarsResult<Option<Column>> {
    get_int_from_post(col, budget, |p| p.factor)
}

fn get_int_from_post<F>(
    col: &Column,
    budget: &Budget,
    int_extractor: F,
) -> PolarsResult<Option<Column>>
where
    F: Fn(&Post) -> i64 + Copy,
{
    let accounts = col.str()?;
    Ok(Some(
        accounts
            .into_iter()
            .map(|a| {
                a.map(|a| budget.get_post_by_account(a).map(int_extractor))
                    .or(None)?
            })
            .collect::<Int64Chunked>()
            .into_column(),
    ))
}

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
                        if attr.key.as_ref() == b"ss:Name"
                            && attr.unescape_value()? == Sheet::Accounts.name()
                        {
                            in_sheet = true;
                        }
                    }
                }
                b"Cell" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"ss:Index" {
                            index = Some(attr.unescape_value()?.parse::<u32>()?);
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
                        let account_index = AccountsColumn::Account as u32;
                        let description_index = AccountsColumn::Description as u32;
                        let debit_index = AccountsColumn::Debit as u32;
                        let credit_index = AccountsColumn::Credit as u32;
                        let balance_index = AccountsColumn::Balance as u32;
                        let mut df_row = new_row(
                            row.remove(&account_index)
                                .unwrap_or("account missing".into()),
                            row.remove(&description_index)
                                .unwrap_or("description missing".into()),
                            &row.remove(&debit_index).unwrap_or("0.0".into()),
                            &row.remove(&credit_index).unwrap_or("0.0".into()),
                            &row.remove(&balance_index).unwrap_or("0.0".into()),
                        )?;
                        df_row = df_row.fill_null(FillNullStrategy::Zero)?;
                        let filtered = df_row
                            .clone()
                            .lazy()
                            .filter(
                                col("Account").is_not_null().and(
                                    col("Account")
                                        .str()
                                        .contains(lit(r"^[3456789]\d{3,4}$"), false),
                                ),
                            )
                            .select([col("Account")])
                            .collect()?;
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
                    cell_value = e.unescape()?.into_owned();
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

fn new_row(
    account: String,
    description: String,
    debit: &str,
    credit: &str,
    balance: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let debit_numeric: f64 = debit.parse().unwrap_or(0.0);
    let credit_numeric: f64 = credit.parse().unwrap_or(0.0);
    let balance_numeric: f64 = balance.parse().unwrap_or(0.0);
    new_row_with_vecs(
        vec![account],
        vec![description],
        vec![debit_numeric],
        vec![credit_numeric],
        vec![balance_numeric],
    )
}

fn new_empty_frame() -> Result<DataFrame, Box<dyn Error>> {
    new_row_with_vecs(
        Vec::<String>::new(),
        Vec::<String>::new(),
        Vec::<f64>::new(),
        Vec::<f64>::new(),
        Vec::<f64>::new(),
    )
}

fn new_row_with_vecs(
    account: Vec<String>,
    description: Vec<String>,
    debit: Vec<f64>,
    credit: Vec<f64>,
    balance: Vec<f64>,
) -> Result<DataFrame, Box<dyn Error>> {
    let df = DataFrame::new(vec![
        Column::new(
            AccountsColumn::Account.name().into(),
            Series::new(AccountsColumn::Account.name().into(), account),
        ),
        Column::new(
            AccountsColumn::Description.name().into(),
            Series::new(AccountsColumn::Description.name().into(), description),
        ),
        Column::new(
            AccountsColumn::Debit.name().into(),
            Series::new(AccountsColumn::Debit.name().into(), debit),
        ),
        Column::new(
            AccountsColumn::Credit.name().into(),
            Series::new(AccountsColumn::Credit.name().into(), credit),
        ),
        Column::new(
            AccountsColumn::Balance.name().into(),
            Series::new(AccountsColumn::Balance.name().into(), balance),
        ),
    ])?;
    Ok(df)
}

/// The worksheets in the workbook
#[derive(Debug, Clone, Copy)]
enum Sheet {
    Accounts = 0,
    _Totals = 1,
    _Journal = 2,
    _FileInfo = 3,
}

impl Sheet {
    fn name(self) -> &'static str {
        match self {
            Sheet::Accounts => "Accounts",
            Sheet::_Totals => "Totals",
            Sheet::_Journal => "Journal",
            Sheet::_FileInfo => "FileInfo",
        }
    }
}

/// The columns in the worksheet Accounts, index is one-based
#[derive(Debug, Clone, Copy)]
enum AccountsColumn {
    Account = 10,
    Description = 11,
    Debit = 21,
    Credit = 22,
    Balance = 23,
}

impl AccountsColumn {
    fn name(self) -> &'static str {
        match self {
            AccountsColumn::Account => "Account",
            AccountsColumn::Description => "Description",
            AccountsColumn::Debit => "Debit",
            AccountsColumn::Credit => "Credit",
            AccountsColumn::Balance => "Balance",
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Budget {
    pub name: String,
    post_groups: HashMap<String, PostGroup>,
    posts: HashMap<String, Post>,
    years: HashMap<String, Year>,
}

#[derive(Deserialize, Debug, Clone)]
struct PostGroup {
    name: String,
    posts: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
struct Post {
    name: String,
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
    /// TODO use this to enrich the `DataFrame` with the post information
    fn get_post_by_account(&self, account: &str) -> Option<&Post> {
        self.posts
            .values()
            .find(|p| p.account_codes.contains(&account.to_string()))
    }
}
