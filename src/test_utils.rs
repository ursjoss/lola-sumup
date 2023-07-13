use chrono::NaiveDate;
use polars::df;
use polars::frame::DataFrame;
use polars::prelude::{AnyValue, NamedFrom};
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
    .expect("valid dataframe sales report data frame 01")
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
    .expect("valid dataframe transaction report data frame 01")
}

/// Sample record matching the structure of the intermediate csv file
#[fixture]
pub fn intermediate_df_01() -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch"],
        "Date" => &["17.4.2023", "17.4.2023"],
        "Time" => &["12:32:00", "12:32:00"],
        "Type" => &["Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDE"],
        "Receipt Number" => &["S20230000303", "S20230000303"],
        "Payment Method" => &["Card", "Card"],
        "Quantity" => &[1_i64, 1_i64],
        "Description" => &["foo", "Trinkgeld"],
        "Currency" => &["CHF", "CHF"],
        "Price (Gross)" => &[16.0, 1.0],
        "Price (Net)" => &[16.0, 1.0],
        "Tax" => &[0.0, 0.0],
        "Tax rate" => &["", ""],
        "Transaction refunded" => &["", ""],
        "Commission" => &[0.2259, 0.0141],
        "Topic" => &["MiTi", "MiTi"],
        "Owner" => &["LoLa", "MiTi"],
        "Purpose" => &["Consumption", "Tip"],
        "Comment" => &[AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 01")
}

#[fixture]
pub fn sample_date_01() -> NaiveDate {
    NaiveDate::parse_from_str("24.3.2023", "%d.%m.%Y").expect("valid date")
}

/// Sample record matching the summary dataframe created from the intermediate csv file
#[fixture]
pub fn summary_df_01(sample_date_01: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date_01],
        "MiTi_Cash" => &[Some(112.0)],
        "MiTi_Card" => &[Some(191.0)],
        "MiTi Total" => &[303.0],
        "Cafe_Cash" => &[Some(29.5)],
        "Cafe_Card" => &[Some(20.0)],
        "Cafe Total" => &[49.5],
        "Verm_Cash" => &[Some(102.5)],
        "Verm_Card" => &[Some(12.0)],
        "Verm Total" => &[114.5],
        "Deposit_Cash" => &[None::<f64>],
        "Deposit_Card" => &[Some(100.0)],
        "Deposit Total" => &[100.0],
        "Rental_Cash" => &[None::<f64>],
        "Rental_Card" => &[Some(500.0)],
        "Rental Total" => &[500.0],
        "Culture_Cash" => &[None::<f64>],
        "Culture_Card" => &[400.0],
        "Culture Total" => &[400.0],
        "PaidOut_Cash" => &[None::<f64>],
        "PaidOut_Card" => &[100.0],
        "PaidOut Total" => &[100.0],
        "Gross Cash" => &[644.0],
        "Tips_Cash" => &[Some(4.0)],
        "SumUp Cash" => &[248.0],
        "Gross Card" => &[1323.0],
        "Tips_Card" => &[Some(1.0)],
        "SumUp Card" => &[1324.0],
        "Gross Total" => &[1567.0],
        "Tips Total" => &[12.5],
        "SumUp Total" => &[1554.5],
        "Gross Card MiTi" => &[191.0],
        "MiTi_Commission" => &[Some(2.75)],
        "Net Card MiTi" => &[188.25],
        "Gross Card LoLa" => &[1132.0],
        "LoLa_Commission" => &[17.54],
        "LoLa_Commission_MiTi" => &[0.44],
        "Net Card LoLa" => &[1114.46],
        "Gross Card Total" => &[1323],
        "Total Commission" => &[19.79],
        "Net Card Total" => &[1303.21],
        "Net Payment SumUp MiTi" => &[187.81],
        "MiTi_Tips_Cash" => &[Some(0.5)],
        "MiTi_Tips_Card" => &[Some(1.0)],
        "MiTi_Tips" => &[Some(1.5)],
        "Cafe_Tips" => &[Some(3.5)],
        "Verm_Tips" => &[None::<f64>],
        "Gross MiTi (MiTi)" => &[Some(250.0)],
        "Gross MiTi (LoLa)" => &[Some(53.0)],
        "Gross MiTi (MiTi) Card" => &[Some(167.0)],
        "Net MiTi (MiTi) Card" => &[164.25],
        "Net MiTi (LoLa)" => &[52.56],
        "Contribution MiTi" => &[10.51],
        "Net MiTi (LoLA) - Share LoLa" => &[52.56],
        "Debt to MiTi" => &[145.76],
        "Income LoLa MiTi" => &[42.49],
        "MealCount_Regular" => &[14],
        "MealCount_Children" => &[1],
    )
    .expect("valid summary dataframe 01")
}
