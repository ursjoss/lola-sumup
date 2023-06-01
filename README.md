# lola-sumup


[![](https://github.com/ursjoss/lola-sumup/workflows/Verify/badge.svg?branch=main)](https://github.com/ursjoss/lola-sumup/actions/workflows/verify.yml)

CLI to evaluate the monthly sumup CSV file and extract useful reports

# Summary Report

The columns of the resulting summary file are defined as follows:

- Generic Columns:
  - `Date`: Calendar Date
- Gross Values of consumptions of topics `MiTi`, `Cafe`, `Verm` (commissions not subtracted) split by payment method
  - `MiTi_Cash`: Gross Cash Income Mitagstisch
  - `MiTi_Card`: Gross Card Income Mitagstisch
  - `MiTi Total`: Total Gross Income Mitagstisch [`MiTi_Cash` + `MiTi_Card`], also [`Gross MiTi (MiTi)` + `Gross MiTi (LoLa)`]
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
  - `Gross Card MiTi`: Gross Card Income Mittagstisch [`MiTi_Card`]
  - `MiTi_Commission`: Card Commission for Mittagstisch
  - `Net Card MiTi`: Net Card Income Mitagstisch [`Gross Card MiTi` - `MiTi_Commission`]
  - `Gross Card LoLa`: Gross Card Income LoLa (Café and Vermietungen) [`Cafe_Card` + `Verm_Card`]
  - `LoLa_Commission`: Card Commission for LoLa (including commission for items sold by MiTi)
  - `LoLa_Commission_MiTi`: Card Commission for LoLa items sold by MiTi only
  - `Net Card LoLa`: Net Card Income LoLa (Café and Vermietungen) [`Gross Card LoLa` - `LoLa_Commission`]
  - `Gross Card Total`: Gross Card Income (MiTi, Café, Vermietungen) [`Gross Card MiTi` + `Gross Card LoLa`]
  - `Total Commission`: Card Commission Total (MiTi, Café, Vermietungen) [`MiTi_Commission` + `LoLa_Commission`]
  - `Net Card Total`: Total Net Card Income [`Gross Card Total` - `Total Commission`]
- Tips by Topic:
  - `MiTi_Tips_Cash`: Tips for Mittagstisch payed in Cash
  - `MiTi_Tips_Card`: Tips for Mittagstisch payed by Card
  - `MiTi_Tips`: Tips for Mittagstisch (Total) [`MiTi_Tips_Cash` + `MiTi_Tips_Card`]
  - `Cafe_Tips`: Tips for Café
  - `Verm_Tips`: Tips for Vermietungen
- Split of Topic Mitagstisch by Owner:
  - `Gross MiTi (MiTi)`: Gross Income Mittagstisch from their own Menus (payed via Card or Cash)
  - `Gross MiTi (LoLa)`: Gross Income Mittagstisch with LoLa-items (Beverages...) (payed via Card or Cash)
  - `Gross MiTi (MiTi) Card`: Gross Income Mittagstisch from their own Menus (payed via Card only)
  - `Net MiTi (MiTi) Card `: Net Income Mittagstisch Menus w/O commission payed by card [`Gross MiTi (MiTi) Card` - `MiTi_Commission`]
  - `Contribution LoLa`: Share MiTi from selling LoLa items [20% * (`Gross MiTi (LoLa)` - `LoLa_Commission_MiTi`)]
  - `Credit MiTi`: Money from MiTi sales via Card w/O commission + contribution Lola sales [`Net MiTi (MiTi) Card` + `Contribution LoLa`]

# Mittagstisch Report

The columns of the resulting miti.csv file are defined as follows:

- Generic columns
  - `Date`: [`Date`]
- Gross values by payment method, differentiating income and tips:
  - `Income Cash`: Total income payed in cash [`MiTi_Cash`]
  - `Tips Cash`: Total Tips payed in cash [`MiTi_Tips_Cash`]
  - `Total Cash`: Income and Tips payed in Cash [`MiTi_Cash` + `MiTi_Tips_Cash`]
  - `Income Card`: Total income payed by card [`MiTi_Card`]
  - `Tips Card`: Total Tips payed by card [`MiTi_Tips_Card`]
  - `Total Card`: Income and Tips payed by Card [`MiTi_Card` + `MiTi_Tips_Card`]
  - `Income Total`: Total Income (cash and card) [`MiTi Total`]
  - `Tips Total`: Tips payed in Cash and by Card [`MiTi_Tips_Cash` + `MiTi_Tips_Card`]
  - `Payment Total`: Total payments (Cash + Card including Tips) [`MiTi Total` + `Tips Total`]
- Income by ownership (MiTi or LoLa):
  - `Gross Income MiTi`: Gross income from menus [`Gross MiTi (MiTi)`]
  - `Gross Income LoLa`: Gross income from LoLa items [`Gross MiTi (LoLa)`]
- Card related income, gross, commission and net, income from selling LoLa and total credit:
  - `Gross Card MiTi`: Gross income from menus payed by card [`Gross MiTi (MiTi) Card`]
  - `Commission MiTi`: Commission on `Gross Card MiTi` [`MiTi_Commission`]
  - `Net Card MiTi`: Net income from menus payed by card [`Net MiTi (MiTi) Card`]
  - `Contribution LoLa`: 20% share on net income from selling lola items
  - `Credit MiTi`: Total credit, i.e. [`Net Card MiTi` + `Contribution LoLa`, or `Credit MiTi`]
