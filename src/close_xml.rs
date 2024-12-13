use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn do_closing_xls(input_path: &Path, _month: &str, _ts: &str) -> Result<(), Box<dyn Error>> {
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
                        let mut df_row = new_row(
                            row.remove(&10).unwrap_or("account missing".into()),
                            row.remove(&11).unwrap_or("description missing".into()),
                            &row.remove(&21).unwrap_or("0.0".into()),
                            &row.remove(&22).unwrap_or("0.0".into()),
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

    println!("{df:?}");
    Ok(())
}

fn new_row(
    account: String,
    description: String,
    debit: &str,
    credit: &str,
) -> Result<DataFrame, Box<dyn Error>> {
    let debit_numeric: f64 = debit.parse().unwrap_or(0.0);
    let credit_numeric: f64 = credit.parse().unwrap_or(0.0);
    new_row_with_vecs(
        vec![account],
        vec![description],
        vec![debit_numeric],
        vec![credit_numeric],
    )
}

fn new_empty_frame() -> Result<DataFrame, Box<dyn Error>> {
    new_row_with_vecs(
        Vec::<String>::new(),
        Vec::<String>::new(),
        Vec::<f64>::new(),
        Vec::<f64>::new(),
    )
}

fn new_row_with_vecs(
    account: Vec<String>,
    description: Vec<String>,
    debit: Vec<f64>,
    credit: Vec<f64>,
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
}

impl AccountsColumn {
    fn name(self) -> &'static str {
        match self {
            AccountsColumn::Account => "Account",
            AccountsColumn::Description => "Description",
            AccountsColumn::Debit => "Debit",
            AccountsColumn::Credit => "Credit",
        }
    }
}
