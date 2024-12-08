use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn do_closing_xls(input_path: &Path, _month: &str, _ts: &str) -> Result<(), Box<dyn Error>> {
    let file = BufReader::new(File::open(input_path)?);
    let mut reader = Reader::from_reader(file);

    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut _in_sheet = false;
    let mut in_cell = false;
    let mut cell_value = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().local_name().into_inner() {
                b"Cell" => in_cell = true,
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name().local_name().into_inner() {
                b"Cell" => {
                    println!("Cell value: {cell_value}");
                    cell_value.clear();
                    in_cell = false;
                }
                b"Row" => println!("---"),
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if in_cell {
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
