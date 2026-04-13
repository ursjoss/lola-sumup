# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.4](https://github.com/jococo-ch/lola-sumup/compare/v0.4.3...v0.4.4) - 2026-04-13

### Fixed

- update and pin checkout action to v6
- update and pin checkout action to v6

## [0.4.3](https://github.com/jococo-ch/lola-sumup/compare/v0.4.2...v0.4.3) - 2026-04-13

### Fixed

- Dummy change to trigger release

## [0.4.2](https://github.com/jococo-ch/lola-sumup/compare/v0.4.1...v0.4.2) - 2026-04-13

### Fixed

- have release-plz rely on git only and not cargo registry at all
- nudge release-plz to not compare content with the cargo registry

### Other

- release v0.4.1 ([#525](https://github.com/jococo-ch/lola-sumup/pull/525))

## [0.4.1](https://github.com/jococo-ch/lola-sumup/compare/v0.1.0...v0.4.1) - 2026-04-13

### Added

- [#515] Refund or Rückerstattung accepted in sales report ([#520](https://github.com/jococo-ch/lola-sumup/pull/520))
- [#516] Levarage Category for Topic and Owner ([#518](https://github.com/jococo-ch/lola-sumup/pull/518))
- Rounding correction for account 20051 ([#507](https://github.com/jococo-ch/lola-sumup/pull/507))
- [#482] verify refund removal nets to 0 ([#494](https://github.com/jococo-ch/lola-sumup/pull/494))
- [#476] Shift column order in details export ([#477](https://github.com/jococo-ch/lola-sumup/pull/477))
- [#473] Align Cultureal sales with Mittagstisch sales in interemedate ([#474](https://github.com/jococo-ch/lola-sumup/pull/474))
- [#470] Improve handling of PaidOut in Banana Export ([#471](https://github.com/jococo-ch/lola-sumup/pull/471))
- [#463] Details export with new column 'PaidOut Sumup' ([#464](https://github.com/jococo-ch/lola-sumup/pull/464))
- [#461] Suggest Items starting with Kerze to be of topic Culture ([#462](https://github.com/jococo-ch/lola-sumup/pull/462))
- First iteration of creating closing excel file ([#448](https://github.com/jococo-ch/lola-sumup/pull/448))
- [#444] Recognize description 'Miete' as Topic::Rental ([#445](https://github.com/jococo-ch/lola-sumup/pull/445))
- [#442] Improve rental reporting in banana export ([#443](https://github.com/jococo-ch/lola-sumup/pull/443))
- [#302] Dynamic change of shift ([#429](https://github.com/jococo-ch/lola-sumup/pull/429))
- [#428] On weekends, during the day is Culture not Café
- [#17,#120] Persist intermediate and exports as XLSX not CSV ([#427](https://github.com/jococo-ch/lola-sumup/pull/427))
- [#406] Rename summary export to details ([#423](https://github.com/jococo-ch/lola-sumup/pull/423))
- [#419] Closing: read journal not accounts ([#421](https://github.com/jococo-ch/lola-sumup/pull/421))
- [#414] Extend columns in banana export ([#415](https://github.com/jococo-ch/lola-sumup/pull/415))
- [#361] Separate date and time in filename timestamps with underscore ([#363](https://github.com/jococo-ch/lola-sumup/pull/363))
- Create import file into banana accounting software ([#320](https://github.com/jococo-ch/lola-sumup/pull/320))
- deprecate account 30800 in favor of 30810
- [#301] Fine tune ([#304](https://github.com/jococo-ch/lola-sumup/pull/304))
- [#297] Handle meals stagaire ([#298](https://github.com/jococo-ch/lola-sumup/pull/298))
- PaidOut/Culture also before 6:00
- PaidOut if the description contains ' (PO)' - not ends with ([#274](https://github.com/jococo-ch/lola-sumup/pull/274))
- [#266] Reclassification evening transactions ([#267](https://github.com/jococo-ch/lola-sumup/pull/267))
- [#247] warn when encountring net price zero ([#248](https://github.com/jococo-ch/lola-sumup/pull/248))
- [**breaking**] [#245] Consume sumup exports as of 2024-05 ([#246](https://github.com/jococo-ch/lola-sumup/pull/246))

### Fixed

- [#492] Improve handling of refunds ([#493](https://github.com/jococo-ch/lola-sumup/pull/493))
- [#480] Fix number format for Excel on Mac ([#481](https://github.com/jococo-ch/lola-sumup/pull/481))
- [#478] Consider the seconds from trx report when determining change of shift ([#479](https://github.com/jococo-ch/lola-sumup/pull/479))
- Determine last of month in December correctly
- [#452] Rename of transaction report column Zahlungsart to Transaktionsart ([#453](https://github.com/jococo-ch/lola-sumup/pull/453))
- [#446] Avoid duplications in intermediate based on mulitlpe SCHICHTWECHSEL ([#447](https://github.com/jococo-ch/lola-sumup/pull/447))
- [#355] Accept two new columns in sales report ([#364](https://github.com/jococo-ch/lola-sumup/pull/364))
- fix build badge in README

### Other

- use PAT instead of organisational permissions ([#524](https://github.com/jococo-ch/lola-sumup/pull/524))
- *(sec)* narrow down permisions of verify github action ([#522](https://github.com/jococo-ch/lola-sumup/pull/522))
- release workflow using releaze-plz and cargo dist ([#521](https://github.com/jococo-ch/lola-sumup/pull/521))
- *(deps)* bump toml from 1.1.0+spec-1.1.0 to 1.1.2+spec-1.1.0 ([#519](https://github.com/jococo-ch/lola-sumup/pull/519))
- Fix/514 new columns in verkaufsbericht ([#517](https://github.com/jococo-ch/lola-sumup/pull/517))
- *(deps)* bump toml from 1.0.7+spec-1.1.0 to 1.1.0+spec-1.1.0 ([#513](https://github.com/jococo-ch/lola-sumup/pull/513))
- *(deps)* bump rustls-webpki from 0.103.9 to 0.103.10 in the cargo group across 1 directory ([#512](https://github.com/jococo-ch/lola-sumup/pull/512))
- *(deps)* bump toml from 1.0.6+spec-1.1.0 to 1.0.7+spec-1.1.0 ([#511](https://github.com/jococo-ch/lola-sumup/pull/511))
- *(deps)* cargo update
- *(deps)* bump clap from 4.5.60 to 4.6.0 ([#510](https://github.com/jococo-ch/lola-sumup/pull/510))
- *(deps)* bump Swatinem/rust-cache from 2.8.2 to 2.9.1 in the all-actions group ([#509](https://github.com/jococo-ch/lola-sumup/pull/509))
- *(deps)* bump quinn-proto from 0.11.13 to 0.11.14 in the cargo group across 1 directory ([#508](https://github.com/jococo-ch/lola-sumup/pull/508))
- clarify intent of functions
- *(deps)* bump toml from 1.0.3+spec-1.1.0 to 1.0.6+spec-1.1.0 ([#505](https://github.com/jococo-ch/lola-sumup/pull/505))
- *(deps)* bump calamine from 0.33.0 to 0.34.0 ([#506](https://github.com/jococo-ch/lola-sumup/pull/506))
- *(deps)* bump clap from 4.5.58 to 4.5.60 ([#504](https://github.com/jococo-ch/lola-sumup/pull/504))
- *(deps)* bump strum_macros from 0.27.2 to 0.28.0 ([#503](https://github.com/jococo-ch/lola-sumup/pull/503))
- *(deps)* bump chrono from 0.4.43 to 0.4.44 ([#502](https://github.com/jococo-ch/lola-sumup/pull/502))
- *(deps)* bump toml from 1.0.2+spec-1.1.0 to 1.0.3+spec-1.1.0 ([#501](https://github.com/jococo-ch/lola-sumup/pull/501))
- *(deps)* bump quick-xml from 0.39.0 to 0.39.2 ([#500](https://github.com/jococo-ch/lola-sumup/pull/500))
- *(deps)* bump strum from 0.27.2 to 0.28.0 ([#499](https://github.com/jococo-ch/lola-sumup/pull/499))
- *(deps)* bump clap from 4.5.57 to 4.5.58 ([#498](https://github.com/jococo-ch/lola-sumup/pull/498))
- *(deps)* bump toml from 0.9.11+spec-1.1.0 to 1.0.2+spec-1.1.0 ([#497](https://github.com/jococo-ch/lola-sumup/pull/497))
- *(deps)* bump calamine from 0.32.0 to 0.33.0 ([#495](https://github.com/jococo-ch/lola-sumup/pull/495))
- *(deps)* bump polars_excel_writer from 0.22.0 to 0.24.0 ([#488](https://github.com/jococo-ch/lola-sumup/pull/488))
- *(deps)* bump clap from 4.5.54 to 4.5.56 ([#490](https://github.com/jococo-ch/lola-sumup/pull/490))
- fix some typos in README
- *(deps)* bump rust_xlsxwriter from 0.92.3 to 0.92.4 ([#487](https://github.com/jococo-ch/lola-sumup/pull/487))
- *(deps)* bump actions/checkout from 6.0.1 to 6.0.2 in the all-actions group ([#486](https://github.com/jococo-ch/lola-sumup/pull/486))
- *(deps)* bump chrono from 0.4.42 to 0.4.43 ([#485](https://github.com/jococo-ch/lola-sumup/pull/485))
- *(deps)* bump rust_xlsxwriter from 0.92.2 to 0.92.3 ([#484](https://github.com/jococo-ch/lola-sumup/pull/484))
- *(deps)* bump toml from 0.9.10+spec-1.1.0 to 0.9.11+spec-1.1.0 ([#483](https://github.com/jococo-ch/lola-sumup/pull/483))
- *(deps)* bump quick-xml from 0.38.4 to 0.39.0 ([#482](https://github.com/jococo-ch/lola-sumup/pull/482))
- *(deps)* bump clap from 4.5.53 to 4.5.54 ([#475](https://github.com/jococo-ch/lola-sumup/pull/475))
- [#463] PaidOut Sumup in README
- Clarify a few test cases
- *(deps)* bump toml from 0.9.8 to 0.9.10+spec-1.1.0 ([#472](https://github.com/jococo-ch/lola-sumup/pull/472))
- Bump to 0.3.2
- Bump to 0.3.2
- *(deps)* bump actions/checkout from 6.0.0 to 6.0.1 in the all-actions group ([#469](https://github.com/jococo-ch/lola-sumup/pull/469))
- *(deps)* bump calamine from 0.31.0 to 0.32.0 ([#468](https://github.com/jococo-ch/lola-sumup/pull/468))
- *(deps)* bump rust_xlsxwriter from 0.92.1 to 0.92.2 ([#467](https://github.com/jococo-ch/lola-sumup/pull/467))
- *(deps)* bump clap from 4.5.52 to 4.5.53 ([#466](https://github.com/jococo-ch/lola-sumup/pull/466))
- *(deps)* bump the all-actions group with 2 updates ([#465](https://github.com/jococo-ch/lola-sumup/pull/465))
- *(deps)* bump polars from 0.51.0 to 0.52.0 ([#455](https://github.com/jococo-ch/lola-sumup/pull/455))
- *(deps)* bump clap from 4.5.51 to 4.5.52 ([#460](https://github.com/jococo-ch/lola-sumup/pull/460))
- *(deps)* bump quick-xml from 0.38.3 to 0.38.4 ([#458](https://github.com/jococo-ch/lola-sumup/pull/458))
- *(deps)* bump actions/checkout from 5.0.0 to 5.0.1 in the all-actions group ([#457](https://github.com/jococo-ch/lola-sumup/pull/457))
- *(deps)* bump clap from 4.5.50 to 4.5.51 ([#454](https://github.com/jococo-ch/lola-sumup/pull/454))
- *(deps)* bump clap from 4.5.49 to 4.5.50 ([#451](https://github.com/jococo-ch/lola-sumup/pull/451))
- *(deps)* bump toml from 0.9.7 to 0.9.8 ([#450](https://github.com/jococo-ch/lola-sumup/pull/450))
- *(deps)* bump clap from 4.5.48 to 4.5.49 ([#449](https://github.com/jococo-ch/lola-sumup/pull/449))
- cargo update
- *(deps)* bump calamine from 0.30.1 to 0.31.0 ([#440](https://github.com/jococo-ch/lola-sumup/pull/440))
- *(deps)* bump polars_excel_writer from 0.20.0 to 0.21.0 ([#441](https://github.com/jococo-ch/lola-sumup/pull/441))
- *(deps)* bump rust_xlsxwriter from 0.90.1 to 0.90.2 ([#439](https://github.com/jococo-ch/lola-sumup/pull/439))
- *(deps)* bump serde from 1.0.226 to 1.0.228 ([#438](https://github.com/jococo-ch/lola-sumup/pull/438))
- cargo update
- *(deps)* bump polars from 0.50.0 to 0.51.0 ([#437](https://github.com/jococo-ch/lola-sumup/pull/437))
- *(deps)* bump Swatinem/rust-cache from 2.8.0 to 2.8.1 in the all-actions group ([#434](https://github.com/jococo-ch/lola-sumup/pull/434))
- *(deps)* bump toml from 0.9.6 to 0.9.7 ([#432](https://github.com/jococo-ch/lola-sumup/pull/432))
- *(deps)* bump toml from 0.9.5 to 0.9.6 ([#430](https://github.com/jococo-ch/lola-sumup/pull/430))
- Bump to 0.3.1
- cargo update
- *(deps)* bump chrono from 0.4.41 to 0.4.42 ([#425](https://github.com/jococo-ch/lola-sumup/pull/425))
- *(deps)* bump clap from 4.5.46 to 4.5.47 ([#426](https://github.com/jococo-ch/lola-sumup/pull/426))
- *(deps)* bump clap from 4.5.45 to 4.5.46 ([#424](https://github.com/jococo-ch/lola-sumup/pull/424))
- *(deps)* bump quick-xml from 0.38.1 to 0.38.3 ([#422](https://github.com/jococo-ch/lola-sumup/pull/422))
- Add information about the close command to the README
- *(deps)* bump clap from 4.5.44 to 4.5.45 ([#420](https://github.com/jococo-ch/lola-sumup/pull/420))
- *(deps)* bump slab from 0.4.10 to 0.4.11 in the cargo group ([#418](https://github.com/jococo-ch/lola-sumup/pull/418))
- *(deps)* bump clap from 4.5.42 to 4.5.44 ([#416](https://github.com/jococo-ch/lola-sumup/pull/416))
- *(deps)* bump actions/checkout from 4.2.2 to 5.0.0 in the all-actions group ([#417](https://github.com/jococo-ch/lola-sumup/pull/417))
- *(deps)* Bump polars to 0.50.0
- cargo update
- cargo upgrade
- *(deps)* Cargo update
- *(deps)* bump strum_macros from 0.27.1 to 0.27.2 ([#411](https://github.com/jococo-ch/lola-sumup/pull/411))
- *(deps)* bump toml from 0.9.2 to 0.9.5 ([#410](https://github.com/jococo-ch/lola-sumup/pull/410))
- *(deps)* bump strum from 0.27.1 to 0.27.2 ([#409](https://github.com/jococo-ch/lola-sumup/pull/409))
- *(deps)* bump rstest from 0.25.0 to 0.26.1 ([#408](https://github.com/jococo-ch/lola-sumup/pull/408))
- *(deps)* bump quick-xml from 0.38.0 to 0.38.1 ([#407](https://github.com/jococo-ch/lola-sumup/pull/407))
- Add SECRETS.adoc
- *(deps)* bump toml from 0.8.23 to 0.9.2 ([#404](https://github.com/jococo-ch/lola-sumup/pull/404))
- *(deps)* bump clap from 4.5.40 to 4.5.41 ([#405](https://github.com/jococo-ch/lola-sumup/pull/405))
- Fix typo in readme
- four exports in README
- *(deps)* bump quick-xml from 0.37.5 to 0.38.0 ([#402](https://github.com/jococo-ch/lola-sumup/pull/402))
- *(deps)* bump Swatinem/rust-cache from 2.7.8 to 2.8.0 in the all-actions group ([#403](https://github.com/jococo-ch/lola-sumup/pull/403))
- *(deps)* bump polars from 0.48.1 to 0.49.1 ([#401](https://github.com/jococo-ch/lola-sumup/pull/401))
- *(deps)* bump toml from 0.8.22 to 0.8.23 ([#399](https://github.com/jococo-ch/lola-sumup/pull/399))
- *(deps)* bump clap from 4.5.39 to 4.5.40 ([#400](https://github.com/jococo-ch/lola-sumup/pull/400))
- *(deps)* bump clap from 4.5.38 to 4.5.39 ([#398](https://github.com/jococo-ch/lola-sumup/pull/398))
- [#396] Resolve deprecatio warning for is_in ([#397](https://github.com/jococo-ch/lola-sumup/pull/397))
- simplify the github action workflow ([#395](https://github.com/jococo-ch/lola-sumup/pull/395))
- *(deps)* cargo upgrade ([#394](https://github.com/jococo-ch/lola-sumup/pull/394))
- fix dependabot configuration
- replace renovate with dependabot ([#393](https://github.com/jococo-ch/lola-sumup/pull/393))
- *(deps)* Bump rust-toolchain to 1.87.0
- *(deps)* lock file maintenance ([#392](https://github.com/jococo-ch/lola-sumup/pull/392))
- *(deps)* update rust crate polars to 0.48.1 ([#382](https://github.com/jococo-ch/lola-sumup/pull/382))
- *(deps)* update taiki-e/install-action digest to e328d9d ([#391](https://github.com/jococo-ch/lola-sumup/pull/391))
- *(deps)* update taiki-e/install-action digest to 941e8a4 ([#390](https://github.com/jococo-ch/lola-sumup/pull/390))
- *(deps)* update taiki-e/install-action digest to 13608a1 ([#389](https://github.com/jococo-ch/lola-sumup/pull/389))
- *(deps)* lock file maintenance ([#388](https://github.com/jococo-ch/lola-sumup/pull/388))
- *(deps)* update rust crate clap to v4.5.38 ([#387](https://github.com/jococo-ch/lola-sumup/pull/387))
- cargo update
- *(deps)* update taiki-e/install-action digest to 83254c5 ([#386](https://github.com/jococo-ch/lola-sumup/pull/386))
- do not depend on polars lazy internal structures ([#385](https://github.com/jococo-ch/lola-sumup/pull/385))
- *(deps)* update taiki-e/install-action digest to 86c23ee ([#384](https://github.com/jococo-ch/lola-sumup/pull/384))
- *(deps)* lock file maintenance ([#383](https://github.com/jococo-ch/lola-sumup/pull/383))
- *(deps)* update taiki-e/install-action digest to 33734a1 ([#381](https://github.com/jococo-ch/lola-sumup/pull/381))
- *(deps)* update dtolnay/rust-toolchain digest to b3b07ba ([#380](https://github.com/jococo-ch/lola-sumup/pull/380))
- *(deps)* update rust crate chrono to v0.4.41 ([#379](https://github.com/jococo-ch/lola-sumup/pull/379))
- *(deps)* update rust crate toml to v0.8.22 ([#378](https://github.com/jococo-ch/lola-sumup/pull/378))
- *(deps)* lock file maintenance ([#377](https://github.com/jococo-ch/lola-sumup/pull/377))
- *(deps)* update rust crate quick-xml to v0.37.5 ([#376](https://github.com/jococo-ch/lola-sumup/pull/376))
- *(deps)* update taiki-e/install-action digest to ab3728c ([#375](https://github.com/jococo-ch/lola-sumup/pull/375))
- *(deps)* update rust crate toml to v0.8.21 ([#374](https://github.com/jococo-ch/lola-sumup/pull/374))
- *(deps)* update taiki-e/install-action digest to 9903ab6 ([#373](https://github.com/jococo-ch/lola-sumup/pull/373))
- *(deps)* lock file maintenance ([#372](https://github.com/jococo-ch/lola-sumup/pull/372))
- *(deps)* update rust crate clap to v4.5.37 ([#371](https://github.com/jococo-ch/lola-sumup/pull/371))
- *(deps)* update taiki-e/install-action digest to 09dc018 ([#370](https://github.com/jococo-ch/lola-sumup/pull/370))
- *(deps)* update taiki-e/install-action digest to be7c31b ([#369](https://github.com/jococo-ch/lola-sumup/pull/369))
- *(deps)* lock file maintenance ([#368](https://github.com/jococo-ch/lola-sumup/pull/368))
- *(deps)* update taiki-e/install-action digest to 5e434d4 ([#367](https://github.com/jococo-ch/lola-sumup/pull/367))
- *(deps)* update rust crate clap to v4.5.36 ([#366](https://github.com/jococo-ch/lola-sumup/pull/366))
- *(deps)* update taiki-e/install-action digest to a48a502 ([#365](https://github.com/jococo-ch/lola-sumup/pull/365))
- *(deps)* update taiki-e/install-action digest to d4635f2 ([#362](https://github.com/jococo-ch/lola-sumup/pull/362))
- *(deps)* lock file maintenance ([#360](https://github.com/jococo-ch/lola-sumup/pull/360))
- *(deps)* update taiki-e/install-action digest to f1390fd ([#359](https://github.com/jococo-ch/lola-sumup/pull/359))
- *(deps)* update taiki-e/install-action digest to 575f713 ([#358](https://github.com/jococo-ch/lola-sumup/pull/358))
- *(deps)* update rust crate clap to v4.5.35 ([#357](https://github.com/jococo-ch/lola-sumup/pull/357))
- *(deps)* update rust crate quick-xml to v0.37.4 ([#356](https://github.com/jococo-ch/lola-sumup/pull/356))
- *(deps)* lock file maintenance ([#354](https://github.com/jococo-ch/lola-sumup/pull/354))
- *(deps)* update taiki-e/install-action digest to 1c861c2 ([#353](https://github.com/jococo-ch/lola-sumup/pull/353))
- *(deps)* update taiki-e/install-action digest to 63f2419 ([#352](https://github.com/jococo-ch/lola-sumup/pull/352))
- *(deps)* update rust crate clap to v4.5.34 ([#351](https://github.com/jococo-ch/lola-sumup/pull/351))
- *(deps)* update rust crate clap to v4.5.33 ([#350](https://github.com/jococo-ch/lola-sumup/pull/350))
- *(deps)* update rust crate quick-xml to v0.37.3 ([#349](https://github.com/jococo-ch/lola-sumup/pull/349))
- *(deps)* lock file maintenance ([#348](https://github.com/jococo-ch/lola-sumup/pull/348))
- *(config)* migrate renovate config ([#347](https://github.com/jococo-ch/lola-sumup/pull/347))
- fix renovate-config
- *(deps)* update taiki-e/install-action digest to a433d87 ([#345](https://github.com/jococo-ch/lola-sumup/pull/345))
- Auto-merge digest updates to github-actions
- *(deps)* update taiki-e/install-action digest to 351cce3 ([#344](https://github.com/jococo-ch/lola-sumup/pull/344))
- Update verify.yml
- Update verify.yml
- *(deps)* Pin some versions
- Update renovate.json
- *(deps)* lock file maintenance ([#343](https://github.com/jococo-ch/lola-sumup/pull/343))
- *(deps)* update rust crate clap to v4.5.32 ([#342](https://github.com/jococo-ch/lola-sumup/pull/342))
- *(deps)* lock file maintenance ([#341](https://github.com/jococo-ch/lola-sumup/pull/341))
- *(deps)* update rust crate serde to v1.0.219 ([#340](https://github.com/jococo-ch/lola-sumup/pull/340))
- *(deps)* lock file maintenance ([#339](https://github.com/jococo-ch/lola-sumup/pull/339))
- *(deps)* update rust crate rstest to 0.25.0 ([#338](https://github.com/jococo-ch/lola-sumup/pull/338))
- *(deps)* update rust crate chrono to v0.4.40 ([#337](https://github.com/jococo-ch/lola-sumup/pull/337))
- *(deps)* update rust crate clap to v4.5.31 ([#336](https://github.com/jococo-ch/lola-sumup/pull/336))
- *(deps)* lock file maintenance ([#335](https://github.com/jococo-ch/lola-sumup/pull/335))
- bump rust edition from 2021 to 2024
- cargo update
- *(deps)* update rust crate serde to v1.0.218 ([#334](https://github.com/jococo-ch/lola-sumup/pull/334))
- *(deps)* update rust crate clap to v4.5.30 ([#333](https://github.com/jococo-ch/lola-sumup/pull/333))
- *(deps)* lock file maintenance ([#332](https://github.com/jococo-ch/lola-sumup/pull/332))
- *(deps)* update rust crate clap to v4.5.29 ([#330](https://github.com/jococo-ch/lola-sumup/pull/330))
- *(deps)* lock file maintenance ([#329](https://github.com/jococo-ch/lola-sumup/pull/329))
- *(deps)* update strum monorepo to 0.27.0 (minor) ([#328](https://github.com/jococo-ch/lola-sumup/pull/328))
- *(deps)* update rust crate toml to v0.8.20 ([#327](https://github.com/jococo-ch/lola-sumup/pull/327))
- *(deps)* update rust crate clap to v4.5.28 ([#326](https://github.com/jococo-ch/lola-sumup/pull/326))
- *(deps)* lock file maintenance ([#325](https://github.com/jococo-ch/lola-sumup/pull/325))
- Process monthly closing from Banana ([#300](https://github.com/jococo-ch/lola-sumup/pull/300))
- *(deps)* update rust crate polars to 0.46.0 ([#324](https://github.com/jococo-ch/lola-sumup/pull/324))
- *(deps)* lock file maintenance ([#323](https://github.com/jococo-ch/lola-sumup/pull/323))
- *(deps)* update rust crate clap to v4.5.27 ([#322](https://github.com/jococo-ch/lola-sumup/pull/322))
- *(deps)* lock file maintenance ([#321](https://github.com/jococo-ch/lola-sumup/pull/321))
- *(deps)* update rust crate clap to v4.5.26 ([#319](https://github.com/jococo-ch/lola-sumup/pull/319))
- *(deps)* update rust crate clap to v4.5.24 ([#318](https://github.com/jococo-ch/lola-sumup/pull/318))
- *(deps)* lock file maintenance ([#317](https://github.com/jococo-ch/lola-sumup/pull/317))
- *(deps)* update rust crate rstest to 0.24.0 ([#316](https://github.com/jococo-ch/lola-sumup/pull/316))
- *(deps)* lock file maintenance ([#315](https://github.com/jococo-ch/lola-sumup/pull/315))
- *(deps)* update rust crate serde to v1.0.217 ([#314](https://github.com/jococo-ch/lola-sumup/pull/314))
- *(deps)* lock file maintenance ([#313](https://github.com/jococo-ch/lola-sumup/pull/313))
- *(deps)* lock file maintenance ([#312](https://github.com/jococo-ch/lola-sumup/pull/312))
- *(deps)* update rust crate serde to v1.0.216 ([#311](https://github.com/jococo-ch/lola-sumup/pull/311))
- *(deps)* update rust crate polars to v0.45.1 ([#310](https://github.com/jococo-ch/lola-sumup/pull/310))
- *(deps)* update rust crate chrono to v0.4.39 ([#309](https://github.com/jococo-ch/lola-sumup/pull/309))
- *(deps)* lock file maintenance ([#308](https://github.com/jococo-ch/lola-sumup/pull/308))
- *(deps)* update rust crate polars to 0.45.0 ([#307](https://github.com/jococo-ch/lola-sumup/pull/307))
- *(deps)* update rust crate clap to v4.5.23 ([#306](https://github.com/jococo-ch/lola-sumup/pull/306))
- *(deps)* update rust crate clap to v4.5.22 ([#305](https://github.com/jococo-ch/lola-sumup/pull/305))
- *(deps)* lock file maintenance ([#303](https://github.com/jococo-ch/lola-sumup/pull/303))
- *(deps)* lock file maintenance ([#299](https://github.com/jococo-ch/lola-sumup/pull/299))
- *(deps)* lock file maintenance ([#296](https://github.com/jococo-ch/lola-sumup/pull/296))
- *(deps)* update rust crate clap to v4.5.21 ([#295](https://github.com/jococo-ch/lola-sumup/pull/295))
- *(deps)* lock file maintenance ([#293](https://github.com/jococo-ch/lola-sumup/pull/293))
- *(deps)* update rust crate serde to v1.0.215 ([#294](https://github.com/jococo-ch/lola-sumup/pull/294))
- Update verify.yml
- *(deps)* lock file maintenance ([#292](https://github.com/jococo-ch/lola-sumup/pull/292))
- *(deps)* lock file maintenance ([#291](https://github.com/jococo-ch/lola-sumup/pull/291))
- *(deps)* lock file maintenance ([#289](https://github.com/jococo-ch/lola-sumup/pull/289))
- *(config)* migrate renovate config ([#290](https://github.com/jococo-ch/lola-sumup/pull/290))
- Bump rust toolchain from 1.80.1 to 1.82.0
- *(deps)* update rust crate polars to v0.44.2 ([#288](https://github.com/jococo-ch/lola-sumup/pull/288))
- *(deps)* update rust crate polars to v0.44.1 ([#287](https://github.com/jococo-ch/lola-sumup/pull/287))
- *(deps)* update rust crate serde to v1.0.214 ([#286](https://github.com/jococo-ch/lola-sumup/pull/286))
- *(deps)* lock file maintenance ([#285](https://github.com/jococo-ch/lola-sumup/pull/285))
- *(deps)* update rust crate polars to 0.44.0 ([#284](https://github.com/jococo-ch/lola-sumup/pull/284))
- *(deps)* update actions/checkout digest to 11bd719 ([#283](https://github.com/jococo-ch/lola-sumup/pull/283))
- *(deps)* update rust crate serde to v1.0.213 ([#282](https://github.com/jococo-ch/lola-sumup/pull/282))
- *(deps)* update rust crate serde to v1.0.211 ([#281](https://github.com/jococo-ch/lola-sumup/pull/281))
- *(deps)* lock file maintenance ([#280](https://github.com/jococo-ch/lola-sumup/pull/280))
- *(deps)* lock file maintenance ([#279](https://github.com/jococo-ch/lola-sumup/pull/279))
- *(deps)* update rust crate clap to v4.5.20 ([#278](https://github.com/jococo-ch/lola-sumup/pull/278))
- *(deps)* update actions/checkout digest to eef6144 ([#277](https://github.com/jococo-ch/lola-sumup/pull/277))
- *(deps)* lock file maintenance ([#276](https://github.com/jococo-ch/lola-sumup/pull/276))
- *(deps)* update rust crate clap to v4.5.19 ([#275](https://github.com/jococo-ch/lola-sumup/pull/275))
- *(deps)* lock file maintenance ([#273](https://github.com/jococo-ch/lola-sumup/pull/273))
- *(deps)* update rust crate rstest to 0.23.0 ([#272](https://github.com/jococo-ch/lola-sumup/pull/272))
- *(deps)* lock file maintenance ([#271](https://github.com/jococo-ch/lola-sumup/pull/271))
- *(deps)* update rust crate clap to v4.5.18 ([#270](https://github.com/jococo-ch/lola-sumup/pull/270))
- *(deps)* lock file maintenance ([#269](https://github.com/jococo-ch/lola-sumup/pull/269))
- *(deps)* update rust crate polars to 0.43.0 ([#268](https://github.com/jococo-ch/lola-sumup/pull/268))
- *(deps)* lock file maintenance ([#265](https://github.com/jococo-ch/lola-sumup/pull/265))
- *(deps)* lock file maintenance ([#264](https://github.com/jococo-ch/lola-sumup/pull/264))
- update rust toolchain
- *(deps)* lock file maintenance ([#262](https://github.com/jococo-ch/lola-sumup/pull/262))
- *(deps)* update rust crate serde to v1.0.209 ([#261](https://github.com/jococo-ch/lola-sumup/pull/261))
- *(deps)* lock file maintenance ([#260](https://github.com/jococo-ch/lola-sumup/pull/260))
- *(deps)* update rust crate clap to v4.5.16 ([#259](https://github.com/jococo-ch/lola-sumup/pull/259))
- *(deps)* update rust crate serde to v1.0.208 ([#258](https://github.com/jococo-ch/lola-sumup/pull/258))
- *(deps)* update rust crate polars to 0.42.0 ([#257](https://github.com/jococo-ch/lola-sumup/pull/257))
- *(deps)* update rust crate serde to v1.0.207 ([#256](https://github.com/jococo-ch/lola-sumup/pull/256))
- *(deps)* lock file maintenance ([#255](https://github.com/jococo-ch/lola-sumup/pull/255))
- *(deps)* update rust crate serde to v1.0.206 ([#254](https://github.com/jococo-ch/lola-sumup/pull/254))
- *(deps)* update rust crate clap to v4.5.15 ([#253](https://github.com/jococo-ch/lola-sumup/pull/253))
- *(deps)* update rust crate clap to v4.5.14 ([#252](https://github.com/jococo-ch/lola-sumup/pull/252))
- *(deps)* update rust crate serde to v1.0.205 ([#251](https://github.com/jococo-ch/lola-sumup/pull/251))
- *(deps)* lock file maintenance ([#250](https://github.com/jococo-ch/lola-sumup/pull/250))
- *(deps)* update rust crate rstest to 0.22.0 ([#249](https://github.com/jococo-ch/lola-sumup/pull/249))
- Bump version to 0.2.0
