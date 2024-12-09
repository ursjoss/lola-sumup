use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

//Row, ss:Index
//- 10: Account
//- 11: Description
//- 21: Debit
//- 22: Credit
pub fn do_closing_xls(input_path: &Path, _month: &str, _ts: &str) -> Result<(), Box<dyn Error>> {
    let file = BufReader::new(File::open(input_path)?);
    let mut reader = Reader::from_reader(file);

    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut in_sheet = false;
    let mut in_cell = false;
    let mut cell_value = String::new();
    let mut index: Option<u32> = None;
    let mut row = HashMap::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"Worksheet" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"ss:Name" && attr.unescape_value()? == "Accounts" {
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
                        println!("--- {row:?}");
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

    Ok(())
}
