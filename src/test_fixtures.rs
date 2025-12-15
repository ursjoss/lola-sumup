use chrono::{Duration, NaiveDate, NaiveTime};
use polars::df;
use polars::frame::DataFrame;
use polars::prelude::AnyValue;
use rstest::fixture;

#[fixture]
pub fn sample_date() -> NaiveDate {
    NaiveDate::parse_from_str("24.3.23", "%d.%m.%y").expect("valid date")
}

#[fixture]
pub fn sample_time() -> NaiveTime {
    NaiveTime::parse_from_str("12:32", "%H:%M").expect("valid time")
}

#[fixture]
pub fn sample_time_minus_5(sample_time: NaiveTime) -> NaiveTime {
    sample_time - Duration::minutes(5)
}

#[fixture]
pub fn sample_time_plus_5(sample_time: NaiveTime) -> NaiveTime {
    sample_time + Duration::minutes(5)
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
    let date = sample_date.format("%d.%m.%Y").to_string();
    let time = sample_time.format("%H:%M").to_string();
    df!(
        "Datum" => &[format!("{date}, {time}")],
        "Typ" => &["Verkauf"],
        "Transaktionsnummer" => &[trx_id],
        "Zahlungsmethode" => &["MC"],
        "Menge" => &[1_i64],
        "Beschreibung" => &[" foo "],
        "Währung" => &["CHF"],
        "Preis vor Rabatt" => &[16.0],
        "Rabatt" => &[0.0],
        "Preis (brutto)" => &[16.0],
        "Preis (netto)" => &[16.0],
        "Steuer" => &[0.0],
        "Steuersatz" => &[""],
        "Konto" => &["a@b.ch"],
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
        "Transaktionsart" => &["Umsatz"],
        "Status" => &["Erfolgreich"],
        "Betrag inkl. MwSt." => &[17.0],
        "Trinkgeldbetrag" => &[1.0],
        "Gebühr" => &[0.24],
    )
    .expect("valid dataframe transaction report data frame 01")
}

/// Sample record 01 matching the structure of the intermediate csv file,
/// It is the result of processing `sales_report_df_01` and `transaction_report_df_01`
#[fixture]
pub fn intermediate_df_01(sample_date: NaiveDate, sample_time: NaiveTime) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch"],
        "Date" => &[sample_date, sample_date],
        "Time" => &[sample_time, sample_time],
        "Type" => &["Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDE"],
        "Payment Method" => &["Card", "Card"],
        "Quantity" => &[1_i64, 1_i64],
        "Description" => &["foo", "Trinkgeld"],
        "Currency" => &["CHF", "CHF"],
        "Price (Gross)" => &[16.0, 1.0],
        "Price (Net)" => &[16.0, 1.0],
        "Commission" => &[0.2259, 0.0141],
        "Topic" => &["MiTi", "MiTi"],
        "Owner" => &["LoLa", "MiTi"],
        "Purpose" => &["Consumption", "Tip"],
        "Comment" => &[AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 01")
}

//endregion

//region:02 - from intermediate to details

/// Sample record 02 matching the structure of the intermediate csv file
#[fixture]
pub fn intermediate_df_02(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch", "a@B.ch"],
        "Date" => &[sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date, sample_date],
        "Time" => &["12:32:00", "12:33:00", "12:34:00", "12:35:00", "12:36:00", "12:37:00", "12:40:00", "13:50:00", "13:51:00", "13:52:00", "13:53:00", "13:54:00"],
        "Type" => &["Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDF", "TEGUCXAGDG", "TEGUCXAGDH", "TEGUCXAGDI", "TEGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDI", "EGUCXAGDJ", "EGUCXAGDK", "EGUCXAGDL", "EGUCXAGDM"],
        "Payment Method" => &["Card", "Cash", "Card", "Card", "Card", "Card", "Card", "Cash", "Card", "Card", "Cash", "Cash"],
        "Quantity" => &[1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "Description" => &["Hauptgang, normal", "Kaffee", "Cappuccino", "Schlüsseldepot", "Kulturevent", "Rental fee", "Sold by renter, paid out in cash", "SoFe 1", "SoFe 2", "Recircle Tupper Depot", "Recircle Tupper Depot", "Hauptgang Fleisch  Reduziert"],
        "Currency" => &["CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF", "CHF"],
        "Price (Gross)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0, 13.0],
        "Price (Net)" => &[16.0, 3.50, 20.0, 100.0, 400.0, 500.0, 100.0, 10.0, 20.0, 10.0, 20.0, 13.0],
        "Commission" =>[0.24123, 0.0, 0.3, 1.5, 6.0, 7.5, 1.5, 0.0, 0.3, 0.15, 0.0, 0.0],
        "Topic" => &["MiTi", "MiTi", "MiTi", "Deposit", "Culture", "Rental", "PaidOut", "SoFe", "SoFe", "Packaging", "Packaging", "MiTi"],
        "Owner" => &["MiTi", "LoLa", "LoLa", "", "", "", "", "", "", "", "", "MiTi"],
        "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        "Comment" => &[None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>, None::<String>],
    )
        .expect("valid intermediate dataframe 02")
}

/// Sample record 02 matching the details df, created from `intermediate_df_02`
/// details
#[fixture]
pub fn details_df_02(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
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
        "PaidOut Sumup" => &[1148.51],
        "MealCount_Regular" => &[1_i64],
        "MealCount_Reduced" => &[1_i64],
        "MealCount_Children" => &[None::<i64>],
        "MealCount_Praktikum" => &[None::<i64>],
        "Total Praktikum" => &[None::<i64>],
    )
    .expect("valid details dataframe 02")
}

//endregion

//region:03 - from details to accounting / miti

/// Sample record 03 matching the details dataframe created from the intermediate csv file
/// accounting
/// miti
#[fixture]
pub fn details_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date],
        "MiTi_Cash" => &[123.0],
        "MiTi_Card" => &[191.0],
        "MiTi Total" => &[314.0],
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
        "Gross Cash" => &[685.0],
        "Tips_Cash" => &[4.0],
        "SumUp Cash" => &[289.0],
        "Gross Card" => &[1353.0],
        "Tips_Card" => &[1.0],
        "SumUp Card" => &[1354.0],
        "Gross Total" => &[1628.0],
        "Tips Total" => &[5.0],
        "SumUp Total" => &[1643.0],
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
        "Gross MiTi (MiTi)" => &[261.0],
        "Gross MiTi (LoLa)" => &[53.0],
        "Gross MiTi (MiTi) Card" => &[167.0],
        "Net MiTi (MiTi) Card" => &[164.25],
        "Net MiTi (LoLa)" => &[52.56],
        "Contribution MiTi" => &[10.51],
        "Net MiTi (LoLA) - Share LoLa" => &[42.05],
        "Sponsored Reductions" => &[2.0],
        "Debt to MiTi" => &[147.76],
        "Income LoLa MiTi" => &[42.49],
        "PaidOut Sumup" => &[1413.76],
        "MealCount_Regular" => &[14],
        "MealCount_Reduced" => &[0],
        "MealCount_Children" => &[1],
        "MealCount_Praktikum" => &[1],
        "Total Praktikum" => &[11.0],
    )
    .expect("valid details dataframe 02")
}

/// Sample record 03 matching the accounting dataframe created from `details_df_03`
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
        "10000/30810" => &[10.0],
        "10000/31000" => &[0.0],
        "10000/32000" => &[0.0],
        "10000/46000" => &[20.0],
        "10920/10000" => &[100.0],
        "10920/23050" => &[100.0],
        "10920/30200" => &[20.0],
        "10920/30700" => &[12.0],
        "10920/30810" => &[20.0],
        "10920/31000" => &[500.0],
        "10920/32000" => &[400.0],
        "10920/46000" => &[10.0],
        "10920/20051" => &[189.25],
        "10920/10910" => &[0.0],
        "68450/10920" => &[17.99],
        "59991/20051" => &[2.0],
        "59991/20120" => &[11.0],
        "20051/10930" => &[147.76],
        "20051/30500" => &[42.49],
        "10930/10100" => &[147.76],
    )
    .expect("Valid accounting df 03")
}

/// Sample record 03 matching the miti dataframe created from `details_df_03`
#[fixture]
pub fn miti_df_03(sample_date: NaiveDate) -> DataFrame {
    df!(
        "Datum" => &[sample_date],
        "Hauptgang" => &[14],
        "Reduziert" => &[1],
        "Kind" => &[1],
        "Küche" => &[Some(261.0)],
        "Total Bar" => &[Some(53.0)],
        "Anteil LoLa" => &[Some(42.4)],
        "Anteil MiTi" => &[Some(10.6)],
        "Einnahmen barz." => &[123.5],
        "davon TG barz." => &[Some(0.5)],
        "Einnahmen Karte" => &[192.0],
        "davon TG Karte" => &[Some(1.0)],
        "Total Einnahmen (oT)" => &[314.0],
        "Kommission Bar" => &[0.44],
        "Netto Bar" => &[52.56],
        "Karte MiTi" => &[Some(167.0)],
        "Kommission MiTi" => &[2.75],
        "Netto Karte MiTi" => &[164.25],
        "Net Total Karte" => &[187.81],
        "Verkauf LoLa (80%)" => &[-42.05],
        "Gesponsort" => &[2.0],
        "Überweisung" => &[147.76],
    )
    .expect("Valid miti df 03")
}

//endregion

//region:04 - from intermediate to details - validating total row

#[fixture]
pub fn sample_date2() -> NaiveDate {
    NaiveDate::parse_from_str("25.3.23", "%d.%m.%y").expect("valid date")
}

#[fixture]
pub fn sample_date3() -> NaiveDate {
    NaiveDate::parse_from_str("26.3.2023", "%d.%m.%Y").expect("valid date")
}

#[fixture]
pub fn sample_date_lom() -> NaiveDate {
    NaiveDate::parse_from_str("31.3.2023", "%d.%m.%Y").expect("valid date")
}

#[fixture]
pub fn intermediate_df_04(
    sample_date: NaiveDate,
    sample_date2: NaiveDate,
    sample_date3: NaiveDate,
) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch"],
        "Date" => &[sample_date, sample_date2, sample_date3],
        "Time" => &["21:00:00", "13:47:00", "12:30:00"],
        "Type" => &["Sales", "Sales", "Sales"],
        "Transaction ID" => &["TEGUCXAGDE", "TEGUCXAGDE", "TEGUCXAGDE"],
        "Payment Method" => &["Cash", "Card", "Cash"],
        "Quantity" => &[1_i64, 3_i64, 1_i64],
        "Description" => &["foo", "Kindermenü", "Hauptgang Vegi Praktikum"],
        "Currency" => &["CHF", "CHF", "CHF"],
        "Price (Gross)" => &[489.1, 20.0, 11.0],
        "Price (Net)" => &[489.1, 20.0, 11.0],
        "Commission" => &[0.0, 0.3, 0.0],
        "Topic" => &["Culture", "MiTi", "MiTi"],
        "Owner" => &["", "MiTi", "MiTi"],
        "Purpose" => &["Consumption", "Consumption", "Consumption"],
        "Comment" => &[AnyValue::Null, AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 03")
}

#[fixture]
pub fn details_df_04(
    sample_date: NaiveDate,
    sample_date2: NaiveDate,
    sample_date3: NaiveDate,
) -> DataFrame {
    df!(
        "Date" => &[Some(sample_date), Some(sample_date2), Some(sample_date3), None],
        "MiTi_Cash" => &[None::<f64>, None::<f64>, Some(11.0), Some(11.0)],
        "MiTi_Card" => &[None::<f64>, Some(20.0), None::<f64>, Some(20.0)],
        "MiTi Total" =>&[Some(0.0), Some(20.0), Some(11.0), Some(31.0)],
        "Cafe_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Cafe_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Cafe Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Verm_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Verm_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Verm Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "SoFe_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "SoFe_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "SoFe Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Deposit_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Deposit_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Deposit Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Packaging_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Packaging_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Packaging Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Rental_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Rental_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Rental Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Culture_Cash" => &[Some(489.1), None::<f64>, None::<f64>, Some(489.1)],
        "Culture_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Culture Total" => &[Some(489.1), Some(0.0), Some(0.0), Some(489.1)],
        "PaidOut_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "PaidOut_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "PaidOut Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Gross Cash" => &[Some(489.1), Some(0.0), Some(11.0), Some(500.1)],
        "Tips_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "SumUp Cash" => &[Some(489.1), Some(0.0), Some(11.0), Some(500.1)],
        "Gross Card" => &[Some(0.0), Some(20.0), Some(0.0), Some(20.0)],
        "Tips_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "SumUp Card" => &[Some(0.0), Some(20.0), Some(0.0), Some(20.0)],
        "Gross Total" => &[Some(489.1), Some(20.0), Some(11.0), Some(520.1)],
        "Tips Total" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "SumUp Total" => &[Some(489.1), Some(20.0), Some(11.0), Some(520.1)],
        "Gross Card MiTi" => &[Some(0.0), Some(20.0), Some(0.0), Some(20.0)],
        "MiTi_Commission" => &[None::<f64>, Some(0.3), Some(0.0),  Some(0.3)],
        "Net Card MiTi" => &[Some(0.0), Some(19.7), Some(0.0), Some(19.7)],
        "Gross Card LoLa" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "LoLa_Commission" => &[Some(0.0), None::<f64>, None::<f64>, Some(0.0)],
        "LoLa_Commission_MiTi" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Net Card LoLa" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Gross Card Total" => &[Some(0.0), Some(20.0), Some(0.0), Some(20.0)],
        "Total Commission" => &[Some(0.0), Some(0.3), Some(0.0), Some(0.3)],
        "Net Card Total" => &[Some(0.0), Some(19.7), Some(0.0), Some(19.7)],
        "Net Payment SumUp MiTi" => &[Some(0.0), Some(19.7), Some(0.0), Some(19.7)],
        "MiTi_Tips_Cash" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "MiTi_Tips_Card" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "MiTi_Tips" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Cafe_Tips" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Verm_Tips" => &[None::<f64>, None::<f64>, None::<f64>, Some(0.0)],
        "Gross MiTi (MiTi)" => &[None::<f64>, Some(20.0), Some(11.0), Some(31.0)],
        "Gross MiTi (LoLa)" => &[None::<f64>, None::<f64>,None::<f64>,  Some(0.0)],
        "Gross MiTi (MiTi) Card" => &[None::<f64>, Some(20.0), None::<f64>, Some(20.0)],
        "Net MiTi (MiTi) Card" => &[Some(0.0), Some(19.7), Some(0.0), Some(19.7)],
        "Net MiTi (LoLa)" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Contribution MiTi" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Net MiTi (LoLA) - Share LoLa" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "Sponsored Reductions" =>[Some(0.0), Some(0.0), Some(2.0), Some(2.0)],
        "Debt to MiTi" => &[Some(0.0), Some(19.7), Some(2.0), Some(21.7)],
        "Income LoLa MiTi" => &[Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
        "PaidOut Sumup" =>&[Some(0.0), Some(19.7), Some(0.0), Some(19.7)],
        "MealCount_Regular" => &[None::<i64>, None::<i64>, None::<i64>, Some(0)],
        "MealCount_Reduced" => &[None::<i64>, None::<i64>, None::<i64>, Some(0)],
        "MealCount_Children" => &[None::<i64>, Some(3), None::<i64>, Some(3)],
        "MealCount_Praktikum" => &[None::<i64>, None::<i64>, Some(1), Some(1)],
        "Total Praktikum" => &[None::<f64>, None::<f64>, Some(11.0), Some(11.0)],
    )
    .expect("valid details dataframe 04")
}

//end region

//region:05 closing

#[fixture]
pub fn journal_df_01() -> DataFrame {
    df!(
        "Date" => &["2025-01-01", "2025-05-16", "2025-07-20"],
        "Description" => &["Posting 1", "Posting 2", "Posting 3"],
        "Debit" => &["10100", "36000", "30100"],
        "Credit" => &["30100", "30700", "31000"],
        "Amount" => &[1000.0, 200.0, 100.0],
    )
    .expect("valid journal dataframe 01")
}

#[fixture]
pub fn aggregated_df_01_202505() -> DataFrame {
    df!(
       "Group" => ["Ertrag Restauration", "Spenden"],
       "Budget 2025" => [30.01, 360.01],
       "1.1.-31.5.25" => [1200.0, -200.0],
       "Verbleibend" => [1169.99, -560.01],
    )
    .expect("valid journal dataframe 01")
}

#[fixture]
pub fn aggregated_df_01_202507() -> DataFrame {
    df!(
       "Group" => ["Ertrag Restauration", "Ertrag Vermietungen", "Spenden"],
       "Budget 2025" => [30.01, 31.01, 360.01],
       "1.1.-31.7.25" => [1100.0, 100.0, -200.0],
       "Verbleibend" => [1069.99, 68.99, -560.01],
    )
    .expect("valid journal dataframe 01")
}

//end region

// region:06 banana details

#[fixture]
pub fn intermediate_df_06(
    sample_date: NaiveDate,
    sample_date2: NaiveDate,
    sample_time: NaiveTime,
) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch", "a@b.ch"],
        "Date" => &[sample_date, sample_date, sample_date, sample_date2, sample_date2],
        "Time" => &[sample_time, sample_time, sample_time, sample_time, sample_time],
        "Type" => &["Sales", "Sales", "Sales", "Sales", "Sales"],
        "Transaction ID" => &["T1", "T2", "T3", "T4", "T5"],
        "Payment Method" => &["Cash", "Card", "Cash", "Cash", "Card"],
        "Quantity" => &[1_i64, 1_i64, 1_i64, 1_i64, 1_i64],
        "Description" => &["Hauptgang Vegi Standard", "Rental A", "Rental B", "Rental C", "Rental D"],
        "Currency" => &["CHF", "CHF", "CHF", "CHF", "CHF"],
        "Price (Gross)" => &[13.0, 400.0, 600.0, 100.0, 200.0],
        "Price (Net)" => &[13.0, 400.0, 600.0, 100.0, 200.0],
        "Commission" => &[0.0, 6.0, 0.0, 0.0, 3.0],
        "Topic" => &["MiTi", "Rental", "Rental", "Rental", "Rental"],
        "Owner" => &["MiTi", "", "", "", ""],
        "Purpose" => &["Consumption", "Consumption", "Consumption", "Consumption", "Consumption"],
        "Comment" => &[AnyValue::Null, AnyValue::Null, AnyValue::Null, AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 06")
}

#[fixture]
pub fn details_df_06(sample_date: NaiveDate, sample_date2: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[Some(sample_date), Some(sample_date2)],
        "MiTi_Cash" => &[Some(13.0), None::<f64>],
        "MiTi_Card" => &[None::<f64>, None::<f64>],
        "MiTi Total" => &[13.0, 0.0],
        "Cafe_Cash" => &[None::<f64>, None::<f64>],
        "Cafe_Card" => &[None::<f64>, None::<f64>],
        "Cafe Total" => &[0.0, 0.0],
        "Verm_Cash" => &[None::<f64>, None::<f64>],
        "Verm_Card" => &[None::<f64>, None::<f64>],
        "Verm Total" => &[0.0, 0.0],
        "SoFe_Cash" => &[None::<f64>, None::<f64>],
        "SoFe_Card" => &[None::<f64>, None::<f64>],
        "SoFe Total" => &[0.0, 0.0],
        "Deposit_Cash" => &[None::<f64>, None::<f64>],
        "Deposit_Card" => &[None::<f64>, None::<f64>],
        "Deposit Total" => &[0.0, 0.0],
        "Packaging_Cash" => &[None::<f64>, None::<f64>],
        "Packaging_Card" => &[None::<f64>, None::<f64>],
        "Packaging Total" => &[0.0, 0.0],
        "Rental_Cash" => &[600.0, 100.0],
        "Rental_Card" => &[400.0, 200.0],
        "Rental Total" => &[1000.0, 300.0],
        "Culture_Cash" => &[None::<f64>, None::<f64>],
        "Culture_Card" => &[None::<f64>, None::<f64>],
        "Culture Total" => &[0.0, 0.0],
        "PaidOut_Cash" => &[None::<f64>, None::<f64>],
        "PaidOut_Card" => &[None::<f64>, None::<f64>],
        "PaidOut Total" => &[0.0, 0.0],
        "Gross Cash" => &[Some(613.0), Some(100.0)],
        "Tips_Cash" => &[None::<f64>, None::<f64>],
        "SumUp Cash" => &[Some(613.0), Some(100.0)],
        "Gross Card" => &[Some(400.0), Some(200.0)],
        "Tips_Card" => &[None::<f64>, None::<f64>],
        "SumUp Card" => &[Some(400.0), Some(200.0)],
        "Gross Total" => &[Some(1013.0), Some(300.0)],
        "Tips Total" => &[0.0, 0.0],
        "SumUp Total" => &[Some(1013.0), Some(300.0)],
        "Gross Card MiTi" => &[0.0, 0.0],
        "MiTi_Commission" => &[Some(0.0), None::<f64>],
        "Net Card MiTi" => &[0.0, 0.0],
        "Gross Card LoLa" => &[400.0, 200.0],
        "LoLa_Commission" => &[Some(6.0), Some(3.0)],
        "LoLa_Commission_MiTi" => &[None::<f64>, None::<f64>],
        "Net Card LoLa" => &[(Some(394.0)), Some(197.0)],
        "Gross Card Total" => &[Some(400.0), Some(200.0)],
        "Total Commission" => &[Some(6.0), Some(3.0)],
        "Net Card Total" => &[Some(394.0), Some(197.0)],
        "Net Payment SumUp MiTi" => &[0.0, 0.0],
        "MiTi_Tips_Cash" => &[None::<f64>, None::<f64>],
        "MiTi_Tips_Card" => &[None::<f64>, None::<f64>],
        "MiTi_Tips" => &[None::<f64>, None::<f64>],
        "Cafe_Tips" => &[None::<f64>, None::<f64>],
        "Verm_Tips" => &[None::<f64>, None::<f64>],
        "Gross MiTi (MiTi)" => &[Some(13.0), None::<f64>],
        "Gross MiTi (LoLa)" => &[None::<f64>, None::<f64>],
        "Gross MiTi (MiTi) Card" => &[None::<f64>, None::<f64>],
        "Net MiTi (MiTi) Card" => &[0.0, 0.0],
        "Net MiTi (LoLa)" => &[0.0, 0.0],
        "Contribution MiTi" => &[0.0, 0.0],
        "Net MiTi (LoLA) - Share LoLa" => &[0.0, 0.0],
        "Sponsored Reductions" => &[0.0, 0.0],
        "Debt to MiTi" => &[0.0, 0.0],
        "Income LoLa MiTi" => &[0.0, 0.0],
        "PaidOut Sumup" => &[Some(394.0), Some(197.0)],
        "MealCount_Regular" => &[Some(1_i64), None::<i64>],
        "MealCount_Reduced" => &[None::<i64>, None::<i64>],
        "MealCount_Children" => &[None::<i64>, None::<i64>],
        "MealCount_Praktikum" => &[None::<i64>, None::<i64>],
        "Total Praktikum" => &[None::<i64>, None::<i64>],
    )
    .expect("valid details dataframe 02")
}

#[fixture]
pub fn accounting_df_06(sample_date: NaiveDate, sample_date2: NaiveDate) -> DataFrame {
    df!(
        "Date" => &[sample_date, sample_date2],
        "Payment SumUp" => &[394.0, 197.0],
        "Total Cash Debit" => &[600.0, 100.0],
        "Total Card Debit" => &[400.0, 200.0],
        "10000/23050" => &[None::<f64>, None::<f64>],
        "10000/30200" => &[None::<f64>, None::<f64>],
        "10000/30700" => &[None::<f64>, None::<f64>],
        "10000/30810" => &[None::<f64>, None::<f64>],
        "10000/31000" => &[600.0, 100.0],
        "10000/32000" => &[None::<f64>, None::<f64>],
        "10000/46000" => &[None::<f64>, None::<f64>],
        "10920/10000" => &[None::<f64>, None::<f64>],
        "10920/23050" => &[None::<f64>, None::<f64>],
        "10920/30200" => &[None::<f64>, None::<f64>],
        "10920/30700" => &[None::<f64>, None::<f64>],
        "10920/30810" => &[None::<f64>, None::<f64>],
        "10920/31000" => &[400.0, 200.0],
        "10920/32000" => &[None::<f64>, None::<f64>],
        "10920/46000" => &[None::<f64>, None::<f64>],
        "10920/20051" => &[0.0, 0.0],
        "10920/10910" => &[0.0, 0.0],
        "68450/10920" => &[6.0, 3.0],
        "59991/20051" => &[0.0, 0.0],
        "59991/20120" => &[None::<f64>, None::<f64>],
        "20051/10930" => &[0.0, 0.0],
        "20051/30500" => &[0.0, 0.0],
        "10930/10100" => &[0.0, 0.0],
    )
    .expect("Valid accounting df 06")
}

#[fixture]
pub fn banana_df_06(sample_date_lom: NaiveDate) -> DataFrame {
    df!(
    "Datum" => &[sample_date_lom, sample_date_lom, sample_date_lom],
    "Beleg" => &["", "", ""],
    "Rechnung" => &["", "", ""],
    "Beschreibung" => &["SU Vermietungen bar", "SU Vermietungen Karte", "SU Kartenkommission"],
    "KtSoll" => &["10000", "10920", "68450"],
    "KtHaben" => &["31000", "31000", "10920"],
    "Anzahl" => &["", "", ""],
    "Einheit" => &["", "", ""],
    "Preis/Einheit" => &["", "", ""],
    "Betrag CHF" => &[700.0, 600.0, 9.0],
    )
    .expect("Valid banana df 06")
}

#[fixture]
pub fn banana_df_ext_06(
    sample_date: NaiveDate,
    sample_date2: NaiveDate,
    sample_date_lom: NaiveDate,
) -> DataFrame {
    df!(
        "Datum" => &[sample_date, sample_date, sample_date2, sample_date2, sample_date_lom],
        "Beleg" => &["", "", "", "", ""],
        "Rechnung" => &["", "", "", "", ""],
        "Beschreibung" => &["Rental A", "Rental B", "Rental C", "Rental D", "SU Kartenkommission"],
        "KtSoll" => &["10920", "10000", "10000", "10920", "68450"],
        "KtHaben" => &["31000", "31000", "31000", "31000", "10920"],
        "Anzahl" => &["", "", "", "", ""],
        "Einheit" => &["", "", "", "", ""],
        "Preis/Einheit" => &["", "", "", "", ""],
        "Betrag CHF" => &[400.0, 600.0, 100.0, 200.0, 9.0],
    )
    .expect("Valid banana df ext 06")
}

//end region

//region:07

#[fixture]
pub fn sales_report_df_07(
    sample_date: NaiveDate,
    sample_time: NaiveTime,
    sample_time_minus_5: NaiveTime,
    sample_time_plus_5: NaiveTime,
) -> DataFrame {
    let date = sample_date.format("%d.%m.%Y").to_string();
    let time1 = sample_time_minus_5.format("%H:%M").to_string();
    let time2 = sample_time.format("%H:%M").to_string();
    let time3 = sample_time_plus_5.format("%H:%M").to_string();
    df!(
        "Datum" => &[format!("{date}, {time1}"), format!("{date}, {time2}"), format!("{date}, {time3}")],
        "Typ" => &["Verkauf", "Verkauf", "Verkauf"],
        "Transaktionsnummer" => &["T1", "T2", "T3"],
        "Zahlungsmethode" => &["Bar", "MC", "Bar"],
        "Menge" => &[1_i64, 1_i64, 1_i64],
        "Beschreibung" => &["SCHICHTWECHSEL", "Kaffee", "SCHICHTWECHSEL"],
        "Währung" => &["CHF", "CHF", "CHF"],
        "Preis vor Rabatt" => &[0.01, 3.50, 0.01],
        "Rabatt" => &[0.0, 0.0, 0.0],
        "Preis (brutto)" => &[0.01, 3.50, 0.01],
        "Preis (netto)" => &[0.01, 3.50, 0.01],
        "Steuer" => &[0.0, 0.0, 0.0],
        "Steuersatz" => &["", "", ""],
        "Konto" => &["a@b.ch", "a@b.ch", "a@b.ch"],
    )
    .expect("valid dataframe sales report data frame 01")
}

#[fixture]
pub fn transaction_report_df_07() -> DataFrame {
    df!(
        "Transaktions-ID" => &["T1", "T2", "T3"],
        "Transaktionsart" => &["Umsatz", "Umsatz", "Umsatz"],
        "Status" => &["Erfolgreich", "Erfolgreich", "Erfolgreich"],
        "Betrag inkl. MwSt." => &[0.01, 3.5, 0.01],
        "Trinkgeldbetrag" => &[0.0, 0.0, 0.0],
        "Gebühr" => &[0.0, 0.05, 0.0],
    )
    .expect("valid dataframe transaction report data frame 07")
}

#[fixture]
pub fn intermediate_df_07(
    sample_date: NaiveDate,
    sample_time: NaiveTime,
    sample_time_minus_5: NaiveTime,
    sample_time_plus_5: NaiveTime,
) -> DataFrame {
    df!(
        "Account" => &["a@b.ch", "a@b.ch", "a@b.ch"],
        "Date" => &[sample_date, sample_date, sample_date],
        "Time" => &[sample_time_minus_5, sample_time, sample_time_plus_5],
        "Type" => &["Sales", "Sales", "Sales"],
        "Transaction ID" => &["T1", "T2", "T3"],
        "Payment Method" => &["Cash", "Card", "Cash"],
        "Quantity" => &[1_i64, 1_i64, 1_i64 ],
        "Description" => &["SCHICHTWECHSEL", "Kaffee", "SCHICHTWECHSEL"],
        "Currency" => &["CHF", "CHF", "CHF"],
        "Price (Gross)" => &[0.0, 3.5, 0.0],
        "Price (Net)" => &[0.0, 3.5, 0.0],
        "Commission" => &[0.0, 0.05, 0.0],
        "Topic" => &["MiTi", "MiTi", "MiTi"],
        "Owner" => &["LoLa", "LoLa", "LoLa"],
        "Purpose" => &["Consumption", "Consumption", "Consumption"],
        "Comment" => &[AnyValue::Null, AnyValue::Null, AnyValue::Null],
    )
    .expect("valid intermediate dataframe 07")
}

//end region
