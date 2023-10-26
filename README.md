# lola-sumup


[![](https://github.com/ursjoss/lola-sumup/workflows/Verify/badge.svg?branch=main)](https://github.com/ursjoss/lola-sumup/actions/workflows/verify.yml)

CLI to evaluate the monthly SumUp CSV extracts and extract LoLa specific reports.

## Summary

The cli application `lola-sumup` has two subcommands: `prepare` and `export`.

The `prepare` subcommand parses two SumUp extracts with monthly data and creates an intermediate file,
combining data from both reports, enriched with three columns `Topic`, `Owner`, and `Purpose`.
The user may redact the content of those three columns, as the simple heuristics may not get it right
out of the box.

The (potentially redacted) intermediate file is consumed by the second `export` step.
It generates three different exports from it, dedicated to different purposes in the context of LoLa's
monthly closing process.


## CLI

The `lola-sumup` command has two subcommands:

```
A cli program to create exports from sumup transactions exported in CSV format

Usage: lola-sumup <COMMAND>

Commands:
  prepare  Prepares an enriched intermediate file from the original sumup sales report CSV and transaction report CSV
  export   Consumes the (potentially redacted) intermediate file and exports to different special purpose CSV files
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### The prepare step

The `lola-sumup prepare` command works as follows:

```
Prepares an enriched intermediate file from the original sumup sales report CSV and transaction report CSV

Usage: lola-sumup prepare --sales-report <SALES_REPORT> --transaction-report <TRANSACTION_REPORT> <MONTH>

Arguments:
  <MONTH>  the month for which transactions are to be processed (<yyyymm>, e.g. 202305)

Options:
  -s, --sales-report <SALES_REPORT>              the sales-report to process
  -t, --transaction-report <TRANSACTION_REPORT>  the transaction-report to process
  -h, --help                                     Print help
  -V, --version                                  Print version
```

It produces a file named e.g. `intermediate_202305_20230603142215.csv`,
where `202305` is the processed month with the timestamp indicating when the process was executed
(03. June 2023 14:22:15).

#### Manual redaction of existing transactions

The last four columns of the file are pre-filled using sensible heuristics.
The derived values may or may not be correct though and can be redacted.
If modified, ensure certain constraints are met, otherwise further processing will fail during the export step.

The four columns that may be modified are:

- `Topic`: The main topic of the transaction, one of
  - `MiTi`: Items sold by Mittagstisch. Automatically assigned if the transaction occurred before 14:15.
  - `Cafe`: Items sold by LoLa Café. Automatically assigned if the transaction occurred between 14:15 and 18:00.
  - `Verm`: Items sold by Renters of the rooms. Automatically assigned if the transaction occurred after 18:00.
  - `SoFe`: Items sold in context of the summer party ("Sommer-Fest").
  - `Deposit`: Key deposit.
  - `Rental`: Rental fee.
  - `Culture`: Items sold in context of cultural events.
  - `PaidOut`: Income paid out in cash to external party.
  - `Packaging`: Sold re-usable packaging for dishes
- `Owner`: Only relevant for Topic `MiTi`: `MiTi` (for menus produced and sold by Mittagstisch) or `LoLa` (LoLa beverages and food from LoLa, sold by Mittagstisch)
- `Purpose`: `Consumption` or `Tip` (the former is also used for Topics `Deposit`, `Rental`, `Culture`, or `PaidOut`)
- `Comment`: Empty, can be manually filled to keep some context

#### Adding artificial transactions for Cash payments that were not entered into SumUp

It is also possible to add lines to capture transactions that were not entered into the SumUp System.

If you do so, ensure some fields are filled correctly, and some fields are left blank. I.e.

- `Account`: Provide an email address that clearly identifies who created the "artificial" transaction
- `Date`
- `Time`: Best guess
- `Type`: "Sales"
- `Transaction ID`: Leave blank
- `Receipt Number`: Leave blank - unless you do have some receipt
- `Payment Method`: `Cash` (as `Card` would never be missing in the SumUp Transactions)
- `Quantity`: Best guess
- `Description`: Best effort - ideally copy one of the existing descriptions to be precise
- `Currency`: "CHF"
- `Price (Gross)`: The paid amount
- `Price (Net)`: Copy of the previous value in `Price (Gross)`
- `Tax`: Leave blank
- `Tax rate`: Leave blank
- `Transaction refunded`: Leave blank
- `Commission`: 0 (it's a cash payment)
- `Topic`: See list of Topics above
- `Owner`: Blank if Topic is not `MiTi`. `MiTi` or `LoLa` if topic is `MiTi`
- `Purpose`: Likely `Consumption` unless it's a tip
- `Comment`: Provide some reference for later audit as to why the transaction is artificially created (e.g. reference to email)

#### Constraints for manual redactions

For all items (existing SumUp transactions and artificially added transactions), it must be true that:

- `Topic` and `Purpose` contain valid values
- `Owner` contains either valid values or is empty
- for `Topic` `MiTi`: `Owner` must be either `MiTi` or `LoLa`
- for `Topic` other than `MiTi`: `Owner` must be blank

### The export step

The `lola-sumup export` command:

```
Consumes the (potentially redacted) intermediate file and exports to different special purpose CSV files

Usage: lola-sumup export <INTERMEDIATE_FILE>

Arguments:
  <INTERMEDIATE_FILE>  the intermediate file to process

Options:
  -h, --help     Print help
  -V, --version  Print version
```

It produces three exports (with month and execution timestamp accordingly):
- `accounting_202305_20230603142503.csv`
- `mittagstisch_202305_20230603142503.csv`
- `summary_202305_20230603142503.csv`

## Description of the exports

### Summary Report

The summary file collects all original and derived columns that are required to build the other reports or for deeper insights.

The columns of the resulting summary file are defined as follows:

- Generic Columns:
  - `Date`: Calendar Date
- Gross Values of consumptions of topics `MiTi`, `Cafe`, `Verm` (commissions not subtracted) split by payment method
  - `MiTi_Cash`: Gross Cash Income Mittagstisch (including LoLa beverages)
  - `MiTi_Card`: Gross Card Income Mittagstisch (including LoLa beverages)
  - `MiTi Total`: Total Gross Income Mittagstisch (including LoLa beverages) [`MiTi_Cash` + `MiTi_Card`], also [`Gross MiTi (MiTi)` + `Gross MiTi (LoLa)`]
  - `Cafe_Cash`: Gross Cash Income Café
  - `Cafe_Card`: Gross Card Income Café
  - `Cafe Total`: Total Gross Income Café [`Cafe_Cash` + `Cafe_Card`]
  - `Verm_Cash`: Gross Cash Income Rentals
  - `Verm_Card`: Gross Card Income Rentals
  - `Verm Total`: Total Gross Income Rentals [`Verm_Cash` + `Verm_Card`]
  - `SoFe_Cash`: Gross Cash summer party
  - `SoFe_Card`: Gross Card summer party
  - `SoFe Total`: Total Gross summer party [`SoFe_Cash` + `SoFe_Card`]
  - `Deposit_Cash`: Gross Cash Key Deposit
  - `Deposit_Card`: Gross Card Key Deposit
  - `Deposit Total`: Total Gross Key Deposit [`Deposit_Cash` + `Deposit_Card`]
  - `Packaging_Cash`: Gross Cash Key Deposit
  - `Packaging_Card`: Gross Card Key Deposit
  - `Packaging Total`: Total Gross Key Deposit [`Packaging_Cash` + `Packaging_Card`]
  - `Rental_Cash`: Gross Cash Rental Payment
  - `Rental_Card`: Gross Card Rental Payment
  - `Rental Total`: Total Gross Rental Payment [`Rental_Cash` + `Rental_Card`]
  - `Culture_Cash`: Gross Cash Culture
  - `Culture_Card`: Gross Card Culture
  - `Culture Total`: Total Gross Culture [`Culture_Cash` + `Culture_Card`]
  - `PaidOut_Cash`: Gross Cash PaidOut
  - `PaidOut_Card`: Gross Card PaidOut
  - `PaidOut Total`: Total Gross PaidOut [`PaidOut_Cash` + `PaidOut_Card`]
- Gross values consumption, Tips and total reported values by payment method:
  - `Gross Cash`: Total Gross Income Cash [`MiTi_Cash` + `Cafe_Cash` + `Verm_Cash` + `SoFe_Cash` + `Deposit_Cash` + `Packaging_Cash` + `Rental_Cash` + `Culture_Cash` + `PaidOut_Cash`]
  - `Tips_Cash`: Tips Cash
  - `SumUp Cash`: Total Income Cash [`Gross Cash` + `Tips_Cash`]
  - `Gross Card`: Gross Gross Income Card [`MiTi_Card` + `Cafe_Card` + `Verm_Card` + `SoFe_Card` + `Deposit_Card` + `Packaging_Card` + `Rental_Card` + `Culture_Card` + `PaidOut_Card`]
  - `Tips_Card`: Tips Card
  - `SumUp Card`: Total Gross Income Card [`Gross Card` + `Tips_Card`]
  - `Gross Total`: Gross Total Income [`Gross Cash` + `Gross Card`]
  - `Tips Total`: [`Tips_Cash` + `Tips_Card`]
  - `SumUp Total`: [`Gross Total` + `Tips Total`] or [`SumUp Cash` + `SumUp Card`]
- Card related payments: Gross values, commission and net values by topic:
  - `Gross Card MiTi`: Gross Card Income Mittagstisch [`MiTi_Card`] (including beverages LoLa)
  - `MiTi_Commission`: Card Commission for Mittagstisch (Menus and Tips, but not from LoLa beverages)
  - `Net Card MiTi`: Net Card Income Mittagstisch [`Gross Card MiTi` - `MiTi_Commission`] - commission on meals and tips are deducted, sales of beverages still included
  - `Gross Card LoLa`: Gross Card Income LoLa (Café, Vermietungen, summer party, Deposit, Rental, Culture) [`Cafe_Card` + `Verm_Card` + `SoFe_Card` + `Deposit_Card` + `Rental_Card` + `Culture_Card` + `PaidOut_Card`]
  - `LoLa_Commission`: Card Commission for LoLa (non-Mittagstisch related, but including commission for items sold by MiTi)
  - `LoLa_Commission_MiTi`: Card Commission for LoLa items sold by MiTi only, so not from Café or Rentals
  - `Net Card LoLa`: Net Card Income LoLa (Café and Vermietungen) [`Gross Card LoLa` - `LoLa_Commission`]
  - `Gross Card Total`: Gross Card Income (MiTi, Café, Vermietungen) [`Gross Card MiTi` + `Gross Card LoLa`]
  - `Total Commission`: Card Commission Total (MiTi, Café, Vermietungen) [`MiTi_Commission` + `LoLa_Commission`]
  - `Net Card Total`: Total Net Card Income [`Gross Card Total` - `Total Commission`]
  - `Net Payment SumUp MiTi`: Total Net Payment SumUp Mittagstisch related (Net card payments concerning Mittagstisch (Sales from meals, tips, sales of LoLa goods paid via card)) [`MiTi_Card` + `MiTi_Tips_Card` - `MiTi_Total_Commission`]
- Tips by Topic:
  - `MiTi_Tips_Cash`: Tips for Mittagstisch paid in Cash
  - `MiTi_Tips_Card`: Tips for Mittagstisch paid by Card
  - `MiTi_Tips`: Tips for Mittagstisch (Total) [`MiTi_Tips_Cash` + `MiTi_Tips_Card`]
  - `Cafe_Tips`: Tips for Café
  - `Verm_Tips`: Tips for Vermietungen
- Split of Topic Mittagstisch by Owner:
  - `Gross MiTi (MiTi)`: Gross Income Mittagstisch from their own Menus (paid via Card or Cash)
  - `Gross MiTi (LoLa)`: Gross Income Mittagstisch with LoLa-items (Beverages...) (paid via Card or Cash)
  - `Gross MiTi (MiTi) Card`: Gross Income Mittagstisch from their own Menus (paid via Card only) (not including tips)
  - `Net MiTi (MiTi) Card`: Net Income Mittagstisch Menus w/o commission paid by card [`Gross MiTi (MiTi) Card` - `MiTi_Commission`]
  - `Net MiTi (LoLa)`: Net total income Mittagstisch with LoLa items w/o commission [`Gross MiTi (LoLa)` - `LoLa_Commission_MiTi`]
  - `Contribution MiTi`: Share MiTi from selling LoLa items [20% * `Net MiTi (LoLa)`]
  - `Net MiTi (LoLA) - Share LoLa`: 80% of Net total income Mittagstisch with LoLa items w/o commission [`Net MiTi (LoLa)` * 0.8]
  - `Debt to MiTi`: Net amount LoLa needs to pay out to Mittagstisch [`Net Payment SumUp MiTi` - `Net MiTi (LoLA) - Share LoLa`]
  - `Income LoLa MiTi`: Income LoLa from MiTi selling LoLa [`Gross MiTi (LoLa)` - `Contribution MiTi`]
- Statistics relevant for Mittagstisch:
  - `MealCount_Regular`: Number of regular meals per day
  - `MealCount_Children`: Number of children meals per day

### Mittagstisch Report

The purpose of the Mittagstisch export is to provide the relevant financial information to the Mittagstisch team.

The columns of the resulting file are defined as follows:

- Generic columns
  - `Datum`: [`Date`]
- Count of Menus
  - `Hauptgang`: Number of regular meals per day [`MealCount_Regular`]
  - `Kind`: Number of children meals per day [`MealCount_Children`]
- Income by ownership (MiTi or LoLa):
  - `Küche`: Gross income from menus [`Gross MiTi (MiTi)`]
  - `Total Bar`: Gross income from selling LoLa items (Bar) [`Gross MiTi (LoLa)`]
  - `Anteil LoLa`: Gross income LoLa from MiTi selling LoLa items
  - `Anteil MiTi`: Gross income MiTi from selling LoLa items
- Gross totals by payment method, differentiating income and tips:
  - `Einnahmen barz.`: Income and Tips paid in Cash [`MiTi_Cash` + `MiTi_Tips_Cash`]
  - `davon TG barz.`: Total Tips paid in cash [`MiTi_Tips_Cash`]
  - `Einnahmen Karte`: Income and Tips paid by Card [`MiTi_Card` + `MiTi_Tips_Card`]
  - `davon TG Karte`: Total Tips paid by card [`MiTi_Tips_Card`]
  - `Total Einnahmen (oT)`: Total payments (Cash + Card - w/o Tips) [`MiTi Total`]
- Net totals:
  - `Kommission Bar`: Commission for LoLa payments [`LoLa_Commission_MiTi`]
  - `Netto Bar`: Net Income for selling LoLa items [`Total Bar` - `Kommission Bar`]
  - `Karte MiTi`: Gross income from menus paid by card (not including tips) [`Gross MiTi (MiTi) Card`]
  - `Kommission MiTi`: Commission on `Gross Card MiTi` (including commission on tips) [`MiTi_Commission`]
  - `Netto Karte MiTi`: Net income from menus paid by card [`Net MiTi (MiTi) Card`]
- Netting with LoLa
  - `Net Total Karte`: Net card payments concerning Mittagstisch (Sales from meals, tips, sales of LoLa goods paid via card) [`Net Paymnet SumUp MiTi`]
  - `Verkauf LoLa (80%)` 80% of net sales lola goods [-`Net MiTi (LoLA) - Share LoLa`]
  - `Überweisung`: Net Payment LoLa to Mittagstisch [`Debt to MiTi`]

### Accounting Report

The purpose of the accounting export is to provide the relevant information on monthly level to the book-keeper.

The columns of the resulting accounting.csv file are defined as follows:

- `Date`: [`Date`]
- `Payment SumUp`: Total Net Income plus tips paid via Card. Daily payment by SumUp (next business day) [`Net Card Total` + `Tips_Card`]. Will be posted `10110/10920`, but based on Account Statement, not this report.
- `Total Cash Debit`: Total daily cash debit [`Gross Cash` - `MiTi_Cash` - `PaidOut Total`]
- `Total Card Debit`: Total daily card debit [`Gross_Card_LoLa` + `Tips_Card` - `MiTi_Tips_Card`]
- `10000/23050`: Total Cash Income Key Deposit [`Deposit_Cash`]
- `10000/30200`: Total Cash Income Cafe [`Cafe_Cash`]
- `10000/30700`: Total Cash Income Food Rentals [`Verm_Cash`] (LoLa Food sold by renters)
- `10000/30800`: Total Cash Income Food Rentals [`SoFe_Cash`] (LoLa Food sold during summer party)
- `10000/31000`: Total Cash Income Rental Fee [`Rental_Cash`] (fees for renting the rooms)
- `10000/32000`: Total Cash Income Cultural Payments [`Culture_Cash`]
- `10000/46000`: Total Cash Cost Reduction on Material Cost [`Packaging_Cash`]
- `10920/10000`: Total Gross Payments Card paid out in cash to external parties [`PaidOut_Card`]
- `10920/23050`: Total Gross Payments Card Key Deposit [`Deposit_Card`]
- `10920/30200`: Total Gross Payments Card Cafe [`Cafe_Card`]
- `10920/30700`: Total Gross Payments Card Rentals [`Verm_Card`] (LoLa Food sold by renters)
- `10920/30800`: Total Gross Payments Card summer party [`SoFe_Card`] (LoLa Food sold during summer party)
- `10920/31000`: Total Gross Payments Card Rental Fee [`Rental_Card`] (fees for renting the rooms)
- `10920/32000`: Total Gross Payments Card Cultural Payments [`Culture_Card`]
- `10920/46000`: Total Gross Payments Card Reduction on Material Cost [`Packaging_Card`]
- `10920/20051`: Net Card income + tips (card) Mittagstisch [`Net Card MiTi` + `MiTi_Tips_Card`]
- `10920/10910`: Tips LoLa paid via Card [`Tips_Card` - `MiTi_Tips_Card`]
- `68450/10920`: Commission for Café, Vermietung, summer party, Deposit, Rental, Cultural Payments, and `PaidOut`, i.e. w/o Mittagstisch [`Commission LoLa`]
- `20051/10900`: Amount LoLa owes to Mittagstisch (`Debt to MiTi`)
- `20051/30500`: Income LoLa from MiTi selling LoLa [`Gross MiTi (LoLa)` - `Contribution MiTi` = `Income LoLa MiTi`]


The first three columns after the date do not require postings.
They serve for consolidation purposes:
- `Payment SumUp` has to match the bank statement showing the daily sumup payments
  (which are posted on our account with a delay of one or more days).
- `Total Cash Debit` and `Total Card Debit` helps to reconcile the cash account ledger ("Kassendokument").
  Please note that those two columns potentially aggregate multiple entries in the cash account ledger for each day.

Where the absolute net sum for the transitory accounts must not be > 0.02, i.e.:
- for `10920`: abs(`10920/30200` + `10920/30700` + `10920/30800` + `10920/23050` + `10920/46000` + `10920/31000` + `10920/32000` +`10920/20051` + `10920/10000` + `10920/10910` - `Payment SumUp` - `68450/10920`) < 0.02
- for `20051`: abs(`10920/20051` - `20051/10900` - `20051/30200`) <= 0.02
