use chrono::{NaiveDate, NaiveTime};
use polars::df;
use polars::frame::DataFrame;
use polars::prelude::{AnyValue, NamedFrom};
use rstest::fixture;

#[fixture]
pub fn sample_date() -> NaiveDate {
    NaiveDate::parse_from_str("24.3.2023", "%d.%m.%Y").expect("valid date")
}

#[fixture]
pub fn sample_time() -> NaiveTime {
    NaiveTime::parse_from_str("12:32", "%H:%M").expect("valid time")
}

/// Sample record 01 matching the structure of the sumup sales report csv file
#[fixture]
pub fn sales_report_df_01(sample_date: NaiveDate, sample_time: NaiveTime) -> DataFrame {
    let date = sample_date.format("%d.%m.%y").to_string();
    let time = sample_time.format("%H:%M").to_string();

    println!("date: ${date}");
    println!("time: ${time}");
    df!(
        "Account" => &["a@b.ch"],
        "Date" => &[date],
        "Time" => &[time],
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

/// Sample record 01 matching the structure of the sumup transaction report csv file
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

/// Sample record 01 matching the structure of the intermediate csv file,
/// It is the result of processing sales_report_df_01 and transaction_report_df_01
#[fixture]
pub fn intermediate_df_01(sample_date: NaiveDate, sample_time: NaiveTime) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch"],
        "Date" => &[sample_date, sample_date],
        "Time" => &[sample_time, sample_time],
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

/// Sample record matching the structure of the intermediate csv file
#[fixture]
pub fn intermediate_df_03(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@B.ch", "a@B.ch"],
        "Date" => &[date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date],
        "Time" => &["12:32:00", "12:33:00", "12:34:00", "12:35:00", "12:36:00", "12:37:00", "12:40:00"],
        "Type" => &["Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDF", "TEGUCXAGDG", "TEGUCXAGDH", "TEGUCXAGDI", "TEGUCXAGDJ", "EGUCXAGDK"],
        "Receipt Number" => &["S20230000303", "S20230000304", "S20230000305", "S20230000306", "S20230000307", "S20230000308", "S20230000309"],
        "Payment Method" => &["Card", "Cash", "Card", "Card", "Card", "Card", "Card"],
        "Quantity" => &[1, 1, 4, 1, 1, 1, 1],
        "Description" => &["Hauptgang, normal", "Kaffee", "Cappuccino", "Schlüsseldepot", "Kulturevent", "Rental fee", "Sold by renter, paid out in cash"],
        "Currency" => &["CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF"],
        "Price (Gross)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0],
        "Price (Net)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0],
        "Tax" => &["0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%"],
        "Tax rate" => &["", "", "", "", "", "", ""],
        "Transaction refunded" => &["", "", "", "", "", "", ""],
        "Commission" =>[0.24123, 0.0, 0.3, 1.5, 6.0, 7.5, 1.5],
        "Topic" => &["MiTi", "MiTi", "MiTi", "Deposit", "Culture", "Rental", "PaidOut"],
        "Owner" => &["MiTi", "LoLa", "LoLa", "", "", "", ""],
        "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        "Comment" => &[None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>],
    )
    .expect("valid intermediate dataframe 03")
}

/// Sample record matching the summary df, created from `intermediate_df_03`
/// summary
#[fixture]
pub fn summary_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
        "MiTi_Cash" => &[Some(3.5)],
        "MiTi_Card" => &[Some(36.0)],
        "MiTi Total" => &[39.5],
        "Cafe_Cash" => &[None::<f64>],
        "Cafe_Card" => &[None::<f64>],
        "Cafe Total" => &[0.0],
        "Verm_Cash" => &[None::<f64>],
        "Verm_Card" => &[None::<f64>],
        "Verm Total" => &[0.0],
        "Deposit_Cash" => &[None::<f64>],
        "Deposit_Card" => &[100.0],
        "Deposit Total" => &[100.0],
        "Rental_Cash" => &[None::<f64>],
        "Rental_Card" => &[500.0],
        "Rental Total" => &[500.0],
        "Culture_Cash" => &[None::<f64>],
        "Culture_Card" => &[400.0],
        "Culture Total" => &[400.0],
        "PaidOut_Cash" => &[None::<f64>],
        "PaidOut_Card" => &[100.0],
        "PaidOut Total" => &[100.0],
        "Gross Cash" => &[3.5],
        "Tips_Cash" => &[None::<f64>],
        "SumUp Cash" => &[3.5],
        "Gross Card" => &[1136.0],
        "Tips_Card" => &[None::<f64>],
        "SumUp Card" => &[1136.0],
        "Gross Total" => &[1139.5],
        "Tips Total" => &[0.0],
        "SumUp Total" => &[1139.5],
        "Gross Card MiTi" => &[36.0],
        "MiTi_Commission" => &[Some(0.24)],
        "Net Card MiTi" => &[35.76],
        "Gross Card LoLa" => &[1100.0],
        "LoLa_Commission" => &[16.8],
        "LoLa_Commission_MiTi" => &[0.3],
        "Net Card LoLa" => &[1083.2],
        "Gross Card Total" => &[1136.0],
        "Total Commission" => &[17.04],
        "Net Card Total" => &[1118.96],
        "Net Payment SumUp MiTi" => &[35.46],
        "MiTi_Tips_Cash" => &[None::<f64>],
        "MiTi_Tips_Card" => &[None::<f64>],
        "MiTi_Tips" => &[None::<f64>],
        "Cafe_Tips" => &[None::<f64>],
        "Verm_Tips" => &[None::<f64>],
        "Gross MiTi (MiTi)" => &[Some(16.0)],
        "Gross MiTi (LoLa)" => &[Some(23.5)],
        "Gross MiTi (MiTi) Card" => &[Some(16.0)],
        "Net MiTi (MiTi) Card" => &[15.76],
        "Net MiTi (LoLa)" => &[23.2],
        "Contribution MiTi" => &[4.64],
        "Net MiTi (LoLA) - Share LoLa" => &[18.56],
        "Debt to MiTi" => &[16.9],
        "Income LoLa MiTi" => &[18.86],
        "MealCount_Regular" => &[1],
        "MealCount_Children" => &[None::<i32>],
    )
    .expect("valid summary dataframe 03")
}

/// Sample record 01 matching the summary dataframe created from the intermediate csv file
/// accounting
#[fixture]
pub fn summary_df_01(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
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

/// Sample record 02 matching the summary dataframe created from the intermediate csv file
/// miti
#[fixture]
pub fn summary_df_04(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
        "MiTi_Cash" => &[Some(112.0)],
        "MiTi_Card" => &[Some(191.0)],
        "MiTi Total" => &[303.0],
        "Cafe_Cash" => &[Some(29.5)],
        "Cafe_Card" => &[Some(20)],
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
        "Tips Total" => &[5.0],
        "SumUp Total" => &[1572.0],
        "Gross Card MiTi" => &[191.0],
        "MiTi_Commission" => &[Some(2.75)],
        "Net Card MiTi" => &[188.25],
        "Gross Card LoLa" => &[1132.0],
        "LoLa_Commission" => &[17.54],
        "LoLa_Commission_MiTi" => &[0.44],
        "Net Card LoLa" => &[218.96],
        "Gross Card Total" => &[1115.56],
        "Total Commission" => &[20.29],
        "Net Card Total" => &[1065.27],
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
        "Net MiTi (LoLA) - Share LoLa" => &[42.05],
        "Debt to MiTi" => &[145.76],
        "Income LoLa MiTi" => &[42.49],
        "MealCount_Regular" => &[14],
        "MealCount_Children" => &[1],
    )
    .expect("valid summary dataframe 01")
}
