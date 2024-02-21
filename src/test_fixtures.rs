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

//region::01 - from raw files to intermediate

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

//endregion

//region:02 - from intermediate to summary

/// Sample record 02 matching the structure of the intermediate csv file
#[fixture]
pub fn intermediate_df_02(sample_date: NaiveDate) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch"],
        "Date" => &[date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date.clone(), date],
        "Time" => &["12:32:00", "12:33:00", "12:34:00", "12:35:00", "12:36:00", "12:37:00", "12:40:00", "13:50:00", "13:51:00", "13:52:00", "13:53:00"],
        "Type" => &["Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDF", "TEGUCXAGDG", "TEGUCXAGDH", "TEGUCXAGDI", "TEGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDI", "EGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDL"],
        "Receipt Number" => &["S20230000303", "S20230000304", "S20230000305", "S20230000306", "S20230000307", "S20230000308", "S20230000309", "S20230000310", "S20230000311", "S20230000312", "S20230000313"],
        "Payment Method" => &["Card", "Cash", "Card", "Card", "Card", "Card", "Card", "Cash", "Card", "Card", "Cash"],
        "Quantity" => &[1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1],
        "Description" => &["Hauptgang, normal", "Kaffee", "Cappuccino", "Schlüsseldepot", "Kulturevent", "Rental fee", "Sold by renter, paid out in cash", "SoFe 1", "SoFe 2", "Recircle Tupper Deopt", "Recircle Tupper Deopt"],
        "Currency" => &["CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF"],
        "Price (Gross)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0],
        "Price (Net)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0],
        "Tax" => &["0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%", "0.0%"],
        "Tax rate" => &["", "", "", "", "", "", "", "", "", "", ""],
        "Transaction refunded" => &["", "", "", "", "", "", "", "", "", "", ""],
        "Commission" =>[0.24123, 0.0, 0.3, 1.5, 6.0, 7.5, 1.5, 0.0, 0.3, 0.15, 0.0],
        "Topic" => &["MiTi", "MiTi", "MiTi", "Deposit", "Culture", "Rental", "PaidOut", "SoFe", "SoFe", "Packaging", "Packaging"],
        "Owner" => &["MiTi", "LoLa", "LoLa", "", "", "", "", "", "", "", ""],
        "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        "Comment" => &[None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>],
    )
        .expect("valid intermediate dataframe 03")
}

/// Sample record 02 matching the summary df, created from `intermediate_df_02`
/// summary
#[fixture]
pub fn summary_df_02(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
        "MiTi_Cash" => &[3.5],
        "MiTi_Card" => &[36.0],
        "MiTi Total" => &[39.5],
        "Cafe_Cash" => &[0.0],
        "Cafe_Card" => &[0.0],
        "Cafe Total" => &[0.0],
        "Verm_Cash" => &[0.0],
        "Verm_Card" => &[0.0],
        "Verm Total" => &[0.0],
        "SoFe_Cash" => &[10.0],
        "SoFe_Card" => &[20.0],
        "SoFe Total" => &[30.0],
        "Deposit_Cash" => &[0.0],
        "Deposit_Card" => &[100.0],
        "Deposit Total" => &[100.0],
        "Packaging_Cash" => &[20.0],
        "Packaging_Card" => &[10.0],
        "Packaging Total" => &[30.0],
        "Rental_Cash" => &[0.0],
        "Rental_Card" => &[500.0],
        "Rental Total" => &[500.0],
        "Culture_Cash" => &[0.0],
        "Culture_Card" => &[400.0],
        "Culture Total" => &[400.0],
        "PaidOut_Cash" => &[0.0],
        "PaidOut_Card" => &[100.0],
        "PaidOut Total" => &[100.0],
        "Gross Cash" => &[33.5],
        "Tips_Cash" => &[0.0],
        "SumUp Cash" => &[33.5],
        "Gross Card" => &[1166.0],
        "Tips_Card" => &[0.0],
        "SumUp Card" => &[1166.0],
        "Gross Total" => &[1199.5],
        "Tips Total" => &[0.0],
        "SumUp Total" => &[1199.5],
        "Gross Card MiTi" => &[36.0],
        "MiTi_Commission" => &[0.24],
        "Net Card MiTi" => &[35.76],
        "Gross Card LoLa" => &[1130.0],
        "LoLa_Commission" => &[17.25],
        "LoLa_Commission_MiTi" => &[0.3],
        "Net Card LoLa" => &[1112.75],
        "Gross Card Total" => &[1166.0],
        "Total Commission" => &[17.49],
        "Net Card Total" => &[1148.51],
        "Net Payment SumUp MiTi" => &[35.46],
        "MiTi_Tips_Cash" => &[0.0],
        "MiTi_Tips_Card" => &[0.0],
        "MiTi_Tips" => &[0.0],
        "Cafe_Tips" => &[0.0],
        "Verm_Tips" => &[0.0],
        "Gross MiTi (MiTi)" => &[16.0],
        "Gross MiTi (LoLa)" => &[23.5],
        "Gross MiTi (MiTi) Card" => &[16.0],
        "Net MiTi (MiTi) Card" => &[15.76],
        "Net MiTi (LoLa)" => &[23.2],
        "Contribution MiTi" => &[4.64],
        "Net MiTi (LoLA) - Share LoLa" => &[18.56],
        "Debt to MiTi" => &[16.9],
        "Income LoLa MiTi" => &[18.86],
        "MealCount_Regular" => &[1],
        "MealCount_Children" => &[0],
    )
    .expect("valid summary dataframe 03")
}

//endregion

//region:03 - from summary to accounting / miti

/// Sample record 03 matching the summary dataframe created from the intermediate csv file
/// accounting
/// miti
#[fixture]
pub fn summary_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
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
        "Debt to MiTi" => &[145.76],
        "Income LoLa MiTi" => &[42.49],
        "MealCount_Regular" => &[14],
        "MealCount_Children" => &[1],
    )
    .expect("valid summary dataframe 01")
}

/// Sample record 03 matching the accounting dataframe created from summary_df_03
#[fixture]
pub fn accounting_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
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
        "20051/10900" => &[145.76],
        "20051/30500" => &[42.49],
    )
    .expect("Valid accounting df")
}

/// Sample record 03 matching the miti dataframe created from summary_df_03
#[fixture]
pub fn miti_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Datum" => &[sample_date],
        "Hauptgang" => &[14],
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
        "Überweisung" => &[145.76],
    )
    .expect("Valid miti df")
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
    df!(
        "Date" => &[Some(sample_date), Some(sample_date2), None],
        "MiTi_Cash" => &[0.0, 0.0, 0.0],
        "MiTi_Card" => &[0.0, 20.0, 20.0],
        "MiTi Total" =>&[0.0, 20.0, 20.0],
        "Cafe_Cash" => &[0.0, 0.0, 0.0],
        "Cafe_Card" => &[0.0, 0.0, 0.0],
        "Cafe Total" => &[0.0, 0.0, 0.0],
        "Verm_Cash" => &[0.0, 0.0, 0.0],
        "Verm_Card" => &[0.0, 0.0, 0.0],
        "Verm Total" => &[0.0, 0.0, 0.0],
        "SoFe_Cash" => &[0.0, 0.0, 0.0],
        "SoFe_Card" => &[0.0, 0.0, 0.0],
        "SoFe Total" => &[0.0, 0.0, 0.0],
        "Deposit_Cash" => &[0.0, 0.0, 0.0],
        "Deposit_Card" => &[0.0, 0.0, 0.0],
        "Deposit Total" => &[0.0, 0.0, 0.0],
        "Packaging_Cash" => &[0.0, 0.0, 0.0],
        "Packaging_Card" => &[0.0, 0.0, 0.0],
        "Packaging Total" => &[0.0, 0.0, 0.0],
        "Rental_Cash" => &[0.0, 0.0, 0.0],
        "Rental_Card" => &[0.0, 0.0, 0.0],
        "Rental Total" => &[0.0, 0.0, 0.0],
        "Culture_Cash" => &[489.1, 0.0, 489.1],
        "Culture_Card" => &[0.0, 0.0, 0.0],
        "Culture Total" => &[489.1, 0.0, 489.1],
        "PaidOut_Cash" => &[0.0, 0.0, 0.0],
        "PaidOut_Card" => &[0.0, 0.0, 0.0],
        "PaidOut Total" => &[0.0, 0.0, 0.0],
        "Gross Cash" => &[489.1, 0.0,489.1],
        "Tips_Cash" => &[0.0, 0.0, 0.0],
        "SumUp Cash" => &[489.1, 0.0,489.1],
        "Gross Card" => &[0.0, 20.0, 20.0],
        "Tips_Card" => &[0.0, 0.0, 0.0],
        "SumUp Card" => &[0.0, 20.0, 20.0],
        "Gross Total" => &[489.1, 20.0, 509.1],
        "Tips Total" => &[0.0, 0.0, 0.0],
        "SumUp Total" => &[489.1, 20.0, 509.1],
        "Gross Card MiTi" => &[0.0, 20.0, 20.0],
        "MiTi_Commission" => &[0.0, 0.3, 0.3],
        "Net Card MiTi" => &[0.0, 19.7, 19.7],
        "Gross Card LoLa" => &[0.0, 0.0, 0.0],
        "LoLa_Commission" => &[0.0, 0.0, 0.0],
        "LoLa_Commission_MiTi" => &[0.0, 0.0, 0.0],
        "Net Card LoLa" => &[0.0, 0.0, 0.0],
        "Gross Card Total" => &[0.0, 20.0, 20.0],
        "Total Commission" => &[0.0, 0.3, 0.3],
        "Net Card Total" => &[0.0, 19.7, 19.7],
        "Net Payment SumUp MiTi" => &[0.0, 19.7, 19.7],
        "MiTi_Tips_Cash" => &[0.0, 0.0, 0.0],
        "MiTi_Tips_Card" => &[0.0, 0.0, 0.0],
        "MiTi_Tips" => &[0.0, 0.0, 0.0],
        "Cafe_Tips" => &[0.0, 0.0, 0.0],
        "Verm_Tips" => &[0.0, 0.0, 0.0],
        "Gross MiTi (MiTi)" => &[0.0, 20.0, 20.0],
        "Gross MiTi (LoLa)" => &[0.0, 0.0, 0.0],
        "Gross MiTi (MiTi) Card" => &[0.0, 20.0, 20.0],
        "Net MiTi (MiTi) Card" => &[0.0, 19.7, 19.7],
        "Net MiTi (LoLa)" => &[0.0, 0.0, 0.0],
        "Contribution MiTi" => &[0.0, 0.0, 0.0],
        "Net MiTi (LoLA) - Share LoLa" => &[0.0, 0.0, 0.0],
        "Debt to MiTi" => &[0.0, 19.7, 19.7],
        "Income LoLa MiTi" => &[0.0, 0.0, 0.0],
        "MealCount_Regular" => &[0, 0, 0],
        "MealCount_Children" => &[0, 3, 3],
    )
    .expect("valid summary dataframe 04")
}

//end region
