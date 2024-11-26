use calamine::{open_workbook, Reader, Xlsx};
use polars::prelude::*;
use std::error::Error;
use std::path::Path;
use std::string::ToString;

/// Processes the export from the accounting software, exporting the monthly closing sheet
pub fn do_closing(input_path: &Path, _month: &str, _ts: &str) -> Result<(), Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(input_path)?;

    if let Ok(range) = workbook.worksheet_range("Accounts") {
        let headers: Vec<String> = range
            .rows()
            .next()
            .expect("No headers found")
            .iter()
            .map(ToString::to_string)
            .collect();

        let columns: Vec<Column> = headers
            .iter()
            .enumerate()
            .map(|(col_idx, header)| {
                let column_data: Vec<AnyValue> = range
                    .rows()
                    .skip(11) // Skip the header rows
                    .map(|row| {
                        row.get(col_idx).map_or(AnyValue::Null, |cell| match cell {
                            calamine::Data::Int(i) => AnyValue::Int64(*i),
                            calamine::Data::Float(f) => AnyValue::Float64(*f),
                            calamine::Data::String(s) => AnyValue::String(s),
                            calamine::Data::Bool(b) => AnyValue::Boolean(*b),
                            _ => AnyValue::Null,
                        })
                    })
                    .collect();

                // Create a Series from the column data
                let series = Series::new(header.into(), column_data);

                // Convert Series to Column
                series.into()
            })
            .collect();

        let full_df = DataFrame::new(columns).expect("Failed to create DataFrame");
        let df = full_df
            .lazy()
            .filter(col("Account").is_not_null())
            .select([
                col("Account"),
                col("Description"),
                col("Balance").fill_null(0.0),
            ])
            .collect()?;
        println!("df: {df}");
    }
    Ok(())
}
