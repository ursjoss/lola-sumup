pub struct Posting {
    pub column_name: &'static str,
    pub alias: &'static str,
    pub description: &'static str,
}

impl Posting {
    pub const DEPOSIT_CASH: Posting = Posting {
        column_name: "Deposit_Cash",
        alias: "10000/23050",
        description: "Schlüsseldepot bar",
    };
    pub const CAFE_CASH: Posting = Posting {
        column_name: "Cafe_Cash",
        alias: "10000/30200",
        description: "Cafe bar",
    };
    pub const VERM_CASH: Posting = Posting {
        column_name: "Verm_Cash",
        alias: "10000/30700",
        description: "Verkäufe bei Vermietung bar",
    };
    pub const SOFE_CASH: Posting = Posting {
        column_name: "SoFe_Cash",
        alias: "10000/30810",
        description: "Sommerfest bar",
    };
    pub const RENTAL_CASH: Posting = Posting {
        column_name: "Rental_Cash",
        alias: "10000/31000",
        description: "Vermietungen bar",
    };
    pub const CULTURE_CASH: Posting = Posting {
        column_name: "Culture_Cash",
        alias: "10000/32000",
        description: "Kulturverkäufe bar",
    };
    pub const PACKAGING_CASH: Posting = Posting {
        column_name: "Packaging_Cash",
        alias: "10000/46000",
        description: "Recircle bar",
    };
    pub const PAIDOUT_CARD: Posting = Posting {
        column_name: "PaidOut_Card",
        alias: "10920/10000",
        description: "Auszahlung Kooperationen Kartenzahlungen",
    };
    pub const DEPOSIT_CARD: Posting = Posting {
        column_name: "Deposit_Card",
        alias: "10920/23050",
        description: "Schlüsseldepot Karte",
    };
    pub const CAFE_CARD: Posting = Posting {
        column_name: "Cafe_Card",
        alias: "10920/30200",
        description: "Cafe Karte",
    };
    pub const VERM_CARD: Posting = Posting {
        column_name: "Verm_Card",
        alias: "10920/30700",
        description: "Verkäufe Vermietung Karte",
    };
    pub const SOFE_CARD: Posting = Posting {
        column_name: "SoFe_Card",
        alias: "10920/30810",
        description: "Sommerfest Karte",
    };
    pub const RENTAL_CARD: Posting = Posting {
        column_name: "Rental_Card",
        alias: "10920/31000",
        description: "Vermietungen Karte",
    };
    pub const CULTURE_CARD: Posting = Posting {
        column_name: "Culture_Card",
        alias: "10920/32000",
        description: "Kulturverkäufe Karte",
    };
    pub const PACKAGING_CARD: Posting = Posting {
        column_name: "Packaging_Card",
        alias: "10920/46000",
        description: "Recircle Karte",
    };
    pub const NET_CARD_TOTAL_MITI: Posting = Posting {
        column_name: "Net Card Total MiTi",
        alias: "10920/20051",
        description: "Netto-Ertrag + Tips Karte MiTi",
    };
    pub const TIPS_CARD_LOLA: Posting = Posting {
        column_name: "Tips Card LoLa",
        alias: "10920/10910",
        description: "Tips Karte LoLa",
    };
    pub const LOLA_COMMISSION: Posting = Posting {
        column_name: "LoLa_Commission",
        alias: "68450/10920",
        description: "Kartenkommission",
    };
    pub const SPONSORED_REDUCTIONS: Posting = Posting {
        column_name: "Sponsored Reductions",
        alias: "59991/20051",
        description: "Preisreduktionen",
    };
    pub const TOTAL_PRAKTIKUM: Posting = Posting {
        column_name: "Total Praktikum",
        alias: "59991/20120",
        description: "Essen Praktikum",
    };
    pub const DEBT_TO_MITI: Posting = Posting {
        column_name: "Debt to MiTi",
        alias: "20051/10930",
        description: "Anteil MiTi",
    };
    pub const INCOME_LOLA_MITI: Posting = Posting {
        column_name: "Income LoLa MiTi",
        alias: "20051/30500",
        description: "Verkäufe Mittagstisch LoLa",
    };
    pub const PAYMENT_TO_MITI: Posting = Posting {
        column_name: "Debt to MiTi",
        alias: "10930/10100",
        description: "Überweisung an MiTi",
    };
    const ALL: [Posting; 23] = [
        Posting::DEPOSIT_CASH,
        Posting::CAFE_CASH,
        Posting::VERM_CASH,
        Posting::SOFE_CASH,
        Posting::RENTAL_CASH,
        Posting::CULTURE_CASH,
        Posting::PACKAGING_CASH,
        Posting::PAIDOUT_CARD,
        Posting::DEPOSIT_CARD,
        Posting::CAFE_CARD,
        Posting::VERM_CARD,
        Posting::SOFE_CARD,
        Posting::RENTAL_CARD,
        Posting::CULTURE_CARD,
        Posting::PACKAGING_CARD,
        Posting::NET_CARD_TOTAL_MITI,
        Posting::TIPS_CARD_LOLA,
        Posting::LOLA_COMMISSION,
        Posting::SPONSORED_REDUCTIONS,
        Posting::TOTAL_PRAKTIKUM,
        Posting::DEBT_TO_MITI,
        Posting::INCOME_LOLA_MITI,
        Posting::PAYMENT_TO_MITI,
    ];

    pub fn from_alias(alias: &str) -> Option<&Posting> {
        Posting::ALL.iter().find(|&b| b.alias == alias)
    }
}
