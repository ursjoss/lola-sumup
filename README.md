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
- Gross values consumption, Tips and total reported values by payment method:
  - `Gross Cash`: Total Gross Income Cash [`MiTi_Cash` + `Cafe_Cash` + `Verm_Cash`]
  - `Tips_Cash`: Tips Cash
  - `Sumup Cash`: Total Income Cash [`Gross Cash` + `Tips_Cash`]
  - `Gross Card`: Gross Gross Income Card [`MiTi_Card` + `Cafe_Card` + `Verm_Card`]
  - `Tips_Card`: Tips Card
  - `Sumup Card`: Total Gross Income Card [`Gross Card` + `Tips_Card`]
  - `Gross Total`: Gross Total Income [`Gross Cash` + `Gross Card`]
  - `Tips Total`: [`Tips_Cash` + `Tips_Card`]
  - `SumUp Total`: [`Gross Total` + `Tips Total`] or [`Sumup Cash` + `Sumup Card`]
- Card related payments: Gross values, commission and net values by topic:
  - `Gross Card MiTi`: Gross Card Income Mittagstisch [`MiTi_Card`] (including beverages LoLa)
  - `MiTi_Commission`: Card Commission for Mittagstisch (Menus and Tips, but not from LoLa beverages)
  - `Net Card MiTi`: Net Card Income Mittagstisch [`Gross Card MiTi` - `MiTi_Commission`] - commission on meals and tips are deducted, sales of beverages still included
  - `Gross Card LoLa`: Gross Card Income LoLa (Café and Vermietungen) [`Cafe_Card` + `Verm_Card`]
  - `LoLa_Commission`: Card Commission for LoLa (non-Mittagstisch related, but including commission for items sold by MiTi)
  - `LoLa_Commission_MiTi`: Card Commission for LoLa items sold by MiTi only, so not from Café or Rentals
  - `Net Card LoLa`: Net Card Income LoLa (Café and Vermietungen) [`Gross Card LoLa` - `LoLa_Commission`]
  - `Gross Card Total`: Gross Card Income (MiTi, Café, Vermietungen) [`Gross Card MiTi` + `Gross Card LoLa`]
  - `Total Commission`: Card Commission Total (MiTi, Café, Vermietungen) [`MiTi_Commission` + `LoLa_Commission`]
  - `Net Card Total`: Total Net Card Income [`Gross Card Total` - `Total Commission`]
  - `Net Paymnet SumUp MiTi`: Total Net Payment SumUp Mittagstisch related (Net card payments concerning Mittagstisch (Sales from meals, tips, sales of LoLa goods payed via card)) [`MiTi_Card` + `MiTi_Tips_Card` - `MiTi_Total_Commission`]
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
  - `Anteil MiTi`: Gross income MiTi from selling LoLa items
  - `Anteil LoLa`: Gross income LoLa from MiTi selling LoLa items
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
  - `Net Total Karte`: Net card payments concerning Mittagstisch (Sales from meals, tips, sales of LoLa goods payed via card) [`Net Paymnet SumUp MiTi`]
  - `Verkauf LoLa (80%)` 80% of net sales lola goods [-`Net MiTi (LoLA) - Share LoLa`]
  - `Überweisung`: Net Payment LoLa to Mittagstisch [`Debt to MiTi`]

### Accounting Report

The purpose of the accounting export is to provide the relevant information on monthly level to the book-keeper.

The columns of the resulting accounting.csv file are defined as follows:

- `Date`: [`Date`]
- `10920/30200`:  Total Gross Payments Card Cafe [`Cafe_Card`]
- `10920/30700`:  Total Gross Payments Card Rentals [`Verm_Card`]
- `10920/20051`: Net Card income + tips (card) Mittagstisch [`Net Card MiTi` + `MiTi_Tips_Card`]
- `10920/10910`: Tips LoLa paid via Card [`Tips_Card` - `MiTi_Tips_Card`]
- `Payment SumUp`: Total Net Income plus tips paid via Card. Daily payment by SumUp (next business day) [`Net Card Total` + `Tips_Card`]. Will be posted `10110/10920`, but based on Account Statement, not this report.
- `68450/10920`:  Commission for Café and Vermietung, i.e. w/o Mittagstisch [`Commission LoLa`]
- `20051/10900`:  Amount LoLa owes to Mittagstisch (`Debt to MiTi`)
- `20051/30500`:  Income LoLa from MiTi selling LoLa [`Gross MiTi (LoLa)` - `Contribution MiTi` = `Income LoLa MiTi`]

Where the net sum for the transitory accounts must be 0.0, i.e.:
- for `10920`: `10920/30200` + `10920/30700` + `10920/20051` + `10920/10910` - `Payment Sumup` - `68450/10920` = 0.0
- for `20051`: `10920/20051` - `20051/10900` - `20051/30200` = 0.0
