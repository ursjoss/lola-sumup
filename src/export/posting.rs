pub struct Posting {
    pub column_name: &'static str,
    pub alias: &'static str,
    pub description: &'static str,
}

impl Posting {
    pub const DEPOSIT_CASH: Posting = Posting {
        column_name: "Deposit_Cash",
        alias: "10000/23050",
        description: "Deposit Cash",
    };
    pub const CAFE_CASH: Posting = Posting {
        column_name: "Cafe_Cash",
        alias: "10000/30200",
        description: "Cafe Cash",
    };
    pub const VERM_CASH: Posting = Posting {
        column_name: "Verm_Cash",
        alias: "10000/30700",
        description: "Verm Cash",
    };
    pub const SOFE_CASH: Posting = Posting {
        column_name: "SoFe_Cash",
        alias: "10000/30810",
        description: "SoFe Cash",
    };
    pub const RENTAL_CASH: Posting = Posting {
        column_name: "Rental_Cash",
        alias: "10000/31000",
        description: "Rental Cash",
    };
    pub const CULTURE_CASH: Posting = Posting {
        column_name: "Culture_Cash",
        alias: "10000/32000",
        description: "Culture Cash",
    };
    pub const PACKAGING_CASH: Posting = Posting {
        column_name: "Packaging_Cash",
        alias: "10000/46000",
        description: "Packaging Cash",
    };
    pub const PAIDOUT_CARD: Posting = Posting {
        column_name: "PaidOut_Card",
        alias: "10920/10000",
        description: "PaidOut Card",
    };
    pub const DEPOSIT_CARD: Posting = Posting {
        column_name: "Deposit_Card",
        alias: "10920/23050",
        description: "Deposit Card",
    };
    pub const CAFE_CARD: Posting = Posting {
        column_name: "Cafe_Card",
        alias: "10920/30200",
        description: "Cafe Card",
    };
    pub const VERM_CARD: Posting = Posting {
        column_name: "Verm_Card",
        alias: "10920/30700",
        description: "Verm Card",
    };
    pub const SOFE_CARD: Posting = Posting {
        column_name: "SoFe_Card",
        alias: "10920/30810",
        description: "SoFe Card",
    };
    pub const RENTAL_CARD: Posting = Posting {
        column_name: "Rental_Card",
        alias: "10920/31000",
        description: "Rental Card",
    };
    pub const CULTURE_CARD: Posting = Posting {
        column_name: "Culture_Card",
        alias: "10920/32000",
        description: "Culture Card",
    };
    pub const PACKAGING_CARD: Posting = Posting {
        column_name: "Packaging_Card",
        alias: "10920/46000",
        description: "Packaging Card",
    };
    pub const NET_CARD_TOTAL_MITI: Posting = Posting {
        column_name: "Net Card Total MiTi",
        alias: "10920/20051",
        description: "Net Card MiTi Total",
    };
    pub const TIPS_CARD_LOLA: Posting = Posting {
        column_name: "Tips Card LoLa",
        alias: "10920/10910",
        description: "Tips Card LoLa",
    };
    pub const LOLA_COMMISSION: Posting = Posting {
        column_name: "LoLa_Commission",
        alias: "68450/10920",
        description: "LoLa Commission",
    };
    pub const SPONSORED_REDUCTIONS: Posting = Posting {
        column_name: "Sponsored Reductions",
        alias: "59991/20051",
        description: "Sponsored Reductions",
    };
    pub const TOTAL_PRAKTIKUM: Posting = Posting {
        column_name: "Total Praktikum",
        alias: "59991/20120",
        description: "Total Praktikum",
    };
    pub const DEBT_TO_MITI: Posting = Posting {
        column_name: "Debt to MiTi",
        alias: "20051/10930",
        description: "Debt to MiTi",
    };
    pub const INCOME_LOLA_MITI: Posting = Posting {
        column_name: "Income LoLa MiTi",
        alias: "20051/30500",
        description: "Income LoLa MiTi",
    };
    pub const PAYMENT_TO_MITI: Posting = Posting {
        column_name: "Debt to MiTi",
        alias: "10930/10100",
        description: "Payment to MiTi",
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
