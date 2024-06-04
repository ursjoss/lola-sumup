use chrono::{NaiveDate, NaiveTime};
use polars::df;
use polars::frame::DataFrame;
use polars::prelude::AnyValue;
use rstest::fixture;

#[fixture]
pub fn sample_date() -> NaiveDate {
    NaiveDate::parse_from_str("24.3.2023", "%d.%m.%Y").expect("valid date")
}

#[fixture]
pub fn sample_time() -> NaiveTime {
    NaiveTime::parse_from_str("12:32", "%H:%M").expect("valid time")
}

//region::01 - from raw files to intermediate

/// Sample record 01 matching the structure of the sumup sales report csv file
#[fixture]
pub fn sales_report_df_01(sample_date: NaiveDate, sample_time: NaiveTime) -> DataFrame {
    sales_report_with_trx_id("TEGUCXAGDE", sample_date, sample_time)
}

#[fixture]
pub fn sales_report_df_02(sample_date: NaiveDate, sample_time: NaiveTime) -> DataFrame {
    sales_report_with_trx_id("other", sample_date, sample_time)
}

fn sales_report_with_trx_id(
    trx_id: &str,
    sample_date: NaiveDate,
    sample_time: NaiveTime,
) -> DataFrame {
    let date = sample_date.format("%d.%m.%y").to_string();
    let time = sample_time.format("%H:%M").to_string();
    df!(
        "Account" => &["a@b.ch"],
        "Date" => &[date],
        "Time" => &[time],
        "Type" => &["Sales"],
        "Transaction ID" => &[trx_id],
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
    .expect("valid dataframe sales report data frame")
}

/// Sample record 01 matching the structure of the sumup transaction report csv file
#[fixture]
pub fn transaction_report_df_01() -> DataFrame {
    transaction_report_with_trx_id("TEGUCXAGDE")
}

#[fixture]
pub fn transaction_report_df_02() -> DataFrame {
    transaction_report_with_trx_id("other")
}

fn transaction_report_with_trx_id(trx_id: &str) -> DataFrame {
    df!(
        "Transaktions-ID" => &[trx_id],
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

//endregion

//region:02 - from intermediate to summary

/// Sample record 02 matching the structure of the intermediate csv file
#[fixture]
pub fn intermediate_df_02(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch"],
        "Date" => &[date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date],
        "Time" => &["12:32:00", "12:33:00", "12:34:00", "12:35:00", "12:36:00", "12:37:00", "12:40:00", "13:50:00", "13:51:00", "13:52:00", "13:53:00", "13:54:00"],
        "Type" => &["Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDF", "TEGUCXAGDG", "TEGUCXAGDH", "TEGUCXAGDI", "TEGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDI", "EGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDL", "EGUCXAGDM"],
        "Receipt Number" => &["S20230000303", "S20230000304", "S20230000305", "S20230000306", "S20230000307", "S20230000308", "S20230000309", "S20230000310", "S20230000311", "S20230000312", "S20230000313", "S20230000314"],
        "Payment Method" => &["Card", "Cash", "Card", "Card", "Card", "Card", "Card", "Cash", "Card", "Card", "Cash", "Cash"],
        "Quantity" => &[1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "Description" => &["Hauptgang, normal", "Kaffee", "Cappuccino", "Schlüsseldepot", "Kulturevent", "Rental fee", "Sold by renter, paid out in cash", "SoFe 1", "SoFe 2", "Recircle Tupper Depot", "Recircle Tupper Depot", "Hauptgang Fleisch  Reduziert"],
        "Currency" => &["CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF"],
        "Price (Gross)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0, 13.0],
        "Price (Net)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0, 13.0],
        "Tax" => &["0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%"],
        "Tax rate" => &["", "", "", "", "", "", "", "", "", "", "", ""],
        "Transaction refunded" => &["", "", "", "", "", "", "", "", "", "", "", ""],
        "Commission" =>[0.24123, 0.0, 0.3, 1.5, 6.0, 7.5, 1.5, 0.0, 0.3, 0.15, 0.0, 0.0],
        "Topic" => &["MiTi", "MiTi", "MiTi", "Deposit", "Culture", "Rental", "PaidOut", "SoFe", "SoFe", "Packaging", "Packaging", "MiTi"],
        "Owner" => &["MiTi", "LoLa", "LoLa", "", "", "", "", "", "", "", "", "MiTi"],
        "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        "Comment" => &[None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>],
    )
        .expect("valid intermediate dataframe 03")
}

/// Sample record 02 matching the summary df, created from `intermediate_df_02`
/// summary
#[fixture]
pub fn summary_df_02(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Date" => &[date],
        "MiTi_Cash" => &[Some(16.5)],
        "MiTi_Card" => &[Some(36.0)],
        "MiTi Total" => &[52.5],
        "Cafe_Cash" => &[None::<f64>],
        "Cafe_Card" => &[None::<f64>],
        "Cafe Total" => &[0.0],
        "Verm_Cash" => &[None::<f64>],
        "Verm_Card" => &[None::<f64>],
        "Verm Total" => &[0.0],
        "SoFe_Cash" => &[Some(10.0)],
        "SoFe_Card" => &[Some(20.0)],
        "SoFe Total" => &[30.0],
        "Deposit_Cash" => &[None::<f64>],
        "Deposit_Card" => &[Some(100.0)],
        "Deposit Total" => &[100.0],
        "Packaging_Cash" => &[Some(20.0)],
        "Packaging_Card" => &[Some(10.0)],
        "Packaging Total" => &[Some(30.0)],
        "Rental_Cash" => &[None::<f64>],
        "Rental_Card" => &[500.0],
        "Rental Total" => &[500.0],
        "Culture_Cash" => &[None::<f64>],
        "Culture_Card" => &[400.0],
        "Culture Total" => &[400.0],
        "PaidOut_Cash" => &[None::<f64>],
        "PaidOut_Card" => &[100.0],
        "PaidOut Total" => &[100.0],
        "Gross Cash" => &[Some(46.5)],
        "Tips_Cash" => &[None::<f64>],
        "SumUp Cash" => &[Some(46.5)],
        "Gross Card" => &[Some(1166.0)],
        "Tips_Card" => &[None::<f64>],
        "SumUp Card" => &[Some(1166.0)],
        "Gross Total" => &[Some(1212.5)],
        "Tips Total" => &[0.0],
        "SumUp Total" => &[Some(1212.5)],
        "Gross Card MiTi" => &[36.0],
        "MiTi_Commission" => &[Some(0.24)],
        "Net Card MiTi" => &[35.76],
        "Gross Card LoLa" => &[Some(1130.0)],
        "LoLa_Commission" => &[Some(17.25)],
        "LoLa_Commission_MiTi" => &[0.3],
        "Net Card LoLa" => &[Some(1112.75)],
        "Gross Card Total" => &[Some(1166.0)],
        "Total Commission" => &[Some(17.49)],
        "Net Card Total" => &[Some(1148.51)],
        "Net Payment SumUp MiTi" => &[35.46],
        "MiTi_Tips_Cash" => &[None::<f64>],
        "MiTi_Tips_Card" => &[None::<f64>],
        "MiTi_Tips" => &[None::<f64>],
        "Cafe_Tips" => &[None::<f64>],
        "Verm_Tips" => &[None::<f64>],
        "Gross MiTi (MiTi)" => &[Some(29.0)],
        "Gross MiTi (LoLa)" => &[Some(23.5)],
        "Gross MiTi (MiTi) Card" => &[Some(16.0)],
        "Net MiTi (MiTi) Card" => &[15.76],
        "Net MiTi (LoLa)" => &[23.2],
        "Contribution MiTi" => &[4.64],
        "Net MiTi (LoLA) - Share LoLa" => &[18.56],
        "Sponsored Reductions" => &[2.0],
        "Debt to MiTi" => &[18.9],
        "Income LoLa MiTi" => &[18.86],
        "MealCount_Regular" => &[1_i64],
        "MealCount_Reduced" => &[1_i64],
        "MealCount_Praktikum" => &[None::<i64>],
        "MealCount_Children" => &[None::<i64>],
    )
    .expect("valid summary dataframe 02")
}

//endregion

//region:03 - from summary to accounting / miti

/// Sample record 03 matching the summary dataframe created from the intermediate csv file
/// accounting
/// miti
#[fixture]
pub fn summary_df_03(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Date" => &[date],
        "MiTi_Cash" => &[112.0],
        "MiTi_Card" => &[191.0],
        "MiTi Total" => &[303.0],
        "Cafe_Cash" => &[29.5],
        "Cafe_Card" => &[20.0],
        "Cafe Total" => &[49.5],
        "Verm_Cash" => &[102.5],
        "Verm_Card" => &[12.0],
        "Verm Total" => &[114.5],
        "SoFe_Cash" => &[10.0],
        "SoFe_Card" => &[20.0],
        "SoFe Total" => &[30.0],
        "Deposit_Cash" => &[0.0],
        "Deposit_Card" => &[100.0],
        "Deposit Total" => &[100.0],
        "Rental_Cash" => &[0.0],
        "Rental_Card" => &[500.0],
        "Rental Total" => &[500.0],
        "Packaging_Cash" => &[20.0],
        "Packaging_Card" => &[10.0],
        "Packaging Total" => &[30.0],
        "Culture_Cash" => &[0.0],
        "Culture_Card" => &[400.0],
        "Culture Total" => &[400.0],
        "PaidOut_Cash" => &[0.0],
        "PaidOut_Card" => &[100.0],
        "PaidOut Total" => &[100.0],
        "Gross Cash" => &[674.0],
        "Tips_Cash" => &[4.0],
        "SumUp Cash" => &[278.0],
        "Gross Card" => &[1353.0],
        "Tips_Card" => &[1.0],
        "SumUp Card" => &[1354.0],
        "Gross Total" => &[1617.0],
        "Tips Total" => &[5.0],
        "SumUp Total" => &[1632.0],
        "Gross Card MiTi" => &[191.0],
        "MiTi_Commission" => &[2.75],
        "Net Card MiTi" => &[188.25],
        "Gross Card LoLa" => &[1162.0],
        "LoLa_Commission" => &[17.99],
        "LoLa_Commission_MiTi" => &[0.44],
        "Net Card LoLa" => &[1144.01],
        "Gross Card Total" => &[1353],
        "Total Commission" => &[20.24],
        "Net Card Total" => &[1412.76],
        "Net Payment SumUp MiTi" => &[187.81],
        "MiTi_Tips_Cash" => &[0.5],
        "MiTi_Tips_Card" => &[1.0],
        "MiTi_Tips" => &[1.5],
        "Cafe_Tips" => &[3.5],
        "Verm_Tips" => &[0.0],
        "Gross MiTi (MiTi)" => &[250.0],
        "Gross MiTi (LoLa)" => &[53.0],
        "Gross MiTi (MiTi) Card" => &[167.0],
        "Net MiTi (MiTi) Card" => &[164.25],
        "Net MiTi (LoLa)" => &[52.56],
        "Contribution MiTi" => &[10.51],
        "Net MiTi (LoLA) - Share LoLa" => &[42.05],
        "Sponsored Reductions" => &[0.0],
        "Debt to MiTi" => &[145.76],
        "Income LoLa MiTi" => &[42.49],
        "MealCount_Regular" => &[14],
        "MealCount_Reduced" => &[0],
        "MealCount_Praktikum" => &[0],
        "MealCount_Children" => &[1],
    )
    .expect("valid summary dataframe 02")
}

/// Sample record 03 matching the accounting dataframe created from summary_df_03
#[fixture]
pub fn accounting_df_03(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Date" => &[date],
        "Payment SumUp" => &[1413.76],
        "Total Cash Debit" => &[462.0],
        "Total Card Debit" => &[1162.0],
        "10000/23050" => &[0.0],
        "10000/30200" => &[29.5],
        "10000/30700" => &[102.5],
        "10000/30800" => &[10.0],
        "10000/31000" => &[0.0],
        "10000/32000" => &[0.0],
        "10000/46000" => &[20.0],
        "10920/10000" => &[100.0],
        "10920/23050" => &[100.0],
        "10920/30200" => &[20.0],
        "10920/30700" => &[12.0],
        "10920/30800" => &[20.0],
        "10920/31000" => &[500.0],
        "10920/32000" => &[400.0],
        "10920/46000" => &[10.0],
        "10920/20051" => &[189.25],
        "10920/10910" => &[0.0],
        "68450/10920" => &[17.99],
        "59991/20051" => &[0.0],
        "20051/10930" => &[145.76],
        "20051/30500" => &[42.49],
        "10930/10100" => &[145.76],
    )
    .expect("Valid accounting df 03")
}

/// Sample record 03 matching the miti dataframe created from summary_df_03
#[fixture]
pub fn miti_df_03(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Datum" => &[date],
        "Hauptgang" => &[14],
        "Reduziert" => &[0],
        "Kind" => &[1],
        "Küche" => &[Some(250.0)],
        "Total Bar" => &[Some(53.0)],
        "Anteil LoLa" => &[Some(42.4)],
        "Anteil MiTi" => &[Some(10.6)],
        "Einnahmen barz." => &[112.5],
        "davon TG barz." => &[Some(0.5)],
        "Einnahmen Karte" => &[192.0],
        "davon TG Karte" => &[Some(1.0)],
        "Total Einnahmen (oT)" => &[303.0],
        "Kommission Bar" => &[0.44],
        "Netto Bar" => &[52.56],
        "Karte MiTi" => &[Some(167.0)],
        "Kommission MiTi" => &[2.75],
        "Netto Karte MiTi" => &[164.25],
        "Net Total Karte" => &[187.81],
        "Verkauf LoLa (80%)" => &[-42.05],
        "Gesponsort" => &[0.0],
        "Überweisung" => &[145.76],
    )
    .expect("Valid miti df 03")
}

//endregion

//region:04 - from intermediate to summary - validating total row

#[fixture]
pub fn sample_date2() -> NaiveDate {
    NaiveDate::parse_from_str("25.3.2023", "%d.%m.%Y").expect("valid date")
}

#[fixture]
pub fn intermediate_df_04(sample_date: NaiveDate, sample_date2: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    let date2 = sample_date2.format("%d.%m.%Y").to_string();
    df!(
        "Account" => &["a@b.ch", "a@b.ch"],
        "Date" => &[date, date2],
        "Time" => &["21:00:00", "13:47:00"],
        "Type" => &["Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDE"],
        "Receipt Number" => &["S20230000303", "S20230000303"],
        "Payment Method" => &["Cash", "Card"],
        "Quantity" => &[1_i64, 3_i64],
        "Description" => &["foo", "Kindermenü"],
        "Currency" => &["CHF", "CHF"],
        "Price (Gross)" => &[489.1, 20.0],
        "Price (Net)" => &[489.1, 20.0],
        "Tax" => &[0.0, 0.0],
        "Tax rate" => &["", ""],
        "Transaction refunded" => &["", ""],
        "Commission" => &[0.0, 0.3],
        "Topic" => &["Culture", "MiTi"],
        "Owner" => &["", "MiTi"],
        "Purpose" => &["Consumption", "Consumption"],
        "Comment" => &[AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 03")
}

#[fixture]
pub fn summary_df_04(sample_date: NaiveDate, sample_date2: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    let date2 = sample_date2.format("%d.%m.%Y").to_string();
    df!(
        "Date" => &[Some(date), Some(date2), None],
        "MiTi_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "MiTi_Card" => &[None::<f64>, Some(20.0), Some(20.0)],
        "MiTi Total" =>&[Some(0.0), Some(20.0), Some(20.0)],
        "Cafe_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Cafe_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Cafe Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Verm_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Verm_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Verm Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "SoFe_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "SoFe_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "SoFe Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Deposit_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Deposit_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Deposit Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Packaging_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Packaging_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Packaging Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Rental_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Rental_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Rental Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Culture_Cash" => &[Some(489.1), None::<f64>, Some(489.1)],
        "Culture_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Culture Total" => &[Some(489.1), Some(0.0), Some(489.1)],
        "PaidOut_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "PaidOut_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "PaidOut Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Gross Cash" => &[Some(489.1), Some(0.0), Some(489.1)],
        "Tips_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "SumUp Cash" => &[Some(489.1), Some(0.0), Some(489.1)],
        "Gross Card" => &[Some(0.0), Some(20.0), Some(20.0)],
        "Tips_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "SumUp Card" => &[Some(0.0), Some(20.0), Some(20.0)],
        "Gross Total" => &[Some(489.1), Some(20.0), Some(509.1)],
        "Tips Total" => &[Some(0.0), Some(0.0), Some(0.0)],
        "SumUp Total" => &[Some(489.1), Some(20.0), Some(509.1)],
        "Gross Card MiTi" => &[Some(0.0), Some(20.0), Some(20.0)],
        "MiTi_Commission" => &[None::<f64>, Some(0.3), Some(0.3)],
        "Net Card MiTi" => &[Some(0.0), Some(19.7), Some(19.7)],
        "Gross Card LoLa" => &[Some(0.0), Some(0.0), Some(0.0)],
        "LoLa_Commission" => &[Some(0.0), None::<f64>, Some(0.0)],
        "LoLa_Commission_MiTi" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Net Card LoLa" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Gross Card Total" => &[Some(0.0), Some(20.0), Some(20.0)],
        "Total Commission" => &[Some(0.0), Some(0.3), Some(0.3)],
        "Net Card Total" => &[Some(0.0), Some(19.7), Some(19.7)],
        "Net Payment SumUp MiTi" => &[Some(0.0), Some(19.7), Some(19.7)],
        "MiTi_Tips_Cash" => &[None::<f64>, None::<f64>, Some(0.0)],
        "MiTi_Tips_Card" => &[None::<f64>, None::<f64>, Some(0.0)],
        "MiTi_Tips" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Cafe_Tips" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Verm_Tips" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Gross MiTi (MiTi)" => &[None::<f64>, Some(20.0), Some(20.0)],
        "Gross MiTi (LoLa)" => &[None::<f64>, None::<f64>, Some(0.0)],
        "Gross MiTi (MiTi) Card" => &[None::<f64>, Some(20.0), Some(20.0)],
        "Net MiTi (MiTi) Card" => &[Some(0.0), Some(19.7), Some(19.7)],
        "Net MiTi (LoLa)" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Contribution MiTi" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Net MiTi (LoLA) - Share LoLa" => &[Some(0.0), Some(0.0), Some(0.0)],
        "Sponsored Reductions" =>[Some(0.0), Some(0.0), Some(0.0)],
        "Debt to MiTi" => &[Some(0.0), Some(19.7), Some(19.7)],
        "Income LoLa MiTi" => &[Some(0.0), Some(0.0), Some(0.0)],
        "MealCount_Regular" => &[None::<i64>, None::<i64>, Some(0)],
        "MealCount_Reduced" => &[None::<i64>, None::<i64>, Some(0)],
        "MealCount_Praktikum" => &[None::<i64>, None::<i64>, Some(0)],
        "MealCount_Children" => &[None::<i64>, Some(3), Some(3)],
    )
    .expect("valid summary dataframe 04")
}

//end region
