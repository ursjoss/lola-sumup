use polars::df;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use rstest::fixture;

/// Asserts two dataframes by first checking the shape, then by comparing row by row
/// and last but not least applying a `frame_equal_missing`.
pub fn assert_dataframe(actual: &DataFrame, expected: &DataFrame) {
    assert_eq!(
        actual.shape(),
        expected.shape(),
        "shape of the dataframes is not matching!"
    );
    for n in 0..actual.shape().0 {
        assert_eq!(
            actual.get_row(n).unwrap(),
            expected.get_row(n).unwrap(),
            "row {n} is not matching the expectation!",
        );
    }
    assert!(actual.frame_equal_missing(expected));
}

/// Sample record matching the structure of the sumup sales report csv file
#[fixture]
pub fn sales_report_df_01() -> DataFrame {
    df!(
        "Account" => &["a@b.ch"],
        "Date" => &["17.04.23"],
        "Time" => &["12:32"],
        "Type" => &["Sales"],
        "Transaction ID" => &["TEGUCXAGDE"],
        "Receipt Number" => &["S20230000303"],
        "Payment Method" => &["Card"],
        "Quantity" => &[1_i64],
        "Description" => &[" foo "],
        "Currency" => &["CHF"],
        "Price (Gross)" => &[16.0],
        "Price (Net)" => &[16.0],
        "Tax" => &[0.0],
        "Tax rate" => &[""],
        "Transaction refunded" => &[""],
    )
    .expect("valid dataframe")
}

/// Sample record matching the structure of the sumup transaction report csv file
#[fixture]
pub fn transaction_report_df_01() -> DataFrame {
    df!(
        "Transaktions-ID" => &["TEGUCXAGDE"],
        "Zahlungsart" => &["Umsatz"],
        "Status" => &["Erfolgreich"],
        "Betrag inkl. MwSt." => &[17.0],
        "Trinkgeldbetrag" => &[1.0],
        "Gebühr" => &[0.24],
    )
    .expect("valid dataframe")
}
