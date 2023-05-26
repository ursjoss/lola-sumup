# lola-sumup


[![](https://github.com/ursjoss/lola-sumup/workflows/Verify/badge.svg?branch=main)](https://github.com/ursjoss/lola-sumup/actions/workflows/verify.yml)

CLI to evaluate the monthly sumup CSV file and extract useful reports

# Summary columns

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
  - `MiTi_Tips`: Tips for Mittagstisch
  - `Cafe_Tips`: Tips for Café
  - `Verm_Tips`: Tips for Vermietungen
- Split of Topic Mitagstisch by Owner:
  - `Gross MiTi (MiTi)`: Gross Income Mittagstisch from their own Menus (payed via Card or Cash)
  - `Gross MiTi (LoLa)`: Gross Income Mittagstisch with LoLa-items (Beverages...) (payed via Card or Cash)
  - `Gross MiTi (MiTi) Card`: Gross Income Mittagstisch from their own Menus (payed via Card only)
