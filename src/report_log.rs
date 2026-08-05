//! Building blocks shared by the German log messages that `prepare` and `export`
//! emit for every workbook they write (or skip).

/// Returns the name of the first of the `sheets` that has no rows to write, if any.
pub fn first_empty_sheet<'a>(sheets: &[(&'a str, usize)]) -> Option<&'a str> {
    sheets
        .iter()
        .find(|(_, rows)| *rows == 0)
        .map(|(name, _)| *name)
}

/// Renders `count` rows as a correctly inflected German noun phrase.
pub fn rows_phrase(count: usize) -> String {
    if count == 1 {
        "1 Zeile".to_string()
    } else {
        format!("{count} Zeilen")
    }
}

/// Renders `count` sheets as a correctly inflected German noun phrase in the dative,
/// to be used after "mit".
pub fn sheets_phrase(count: usize) -> String {
    if count == 1 {
        "1 Blatt".to_string()
    } else {
        format!("{count} Blättern")
    }
}

/// Renders the per-sheet row counts as a comma separated list, e.g.
/// `«details»: 21 Zeilen, «transaktionen»: 1 Zeile`.
pub fn per_sheet_phrase(sheets: &[(&str, usize)]) -> String {
    sheets
        .iter()
        .map(|(name, rows)| format!("«{name}»: {}", rows_phrase(*rows)))
        .collect::<Vec<String>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(&[("details", 21), ("transaktionen", 20)], None)]
    #[case(&[("mittagstisch", 6), ("transaktionen", 0)], Some("transaktionen"))]
    #[case(&[("banana", 0), ("transaktionen", 20)], Some("banana"))]
    #[case(&[("banana", 0), ("transaktionen", 0)], Some("banana"))]
    #[case(&[("202607", 253)], None)]
    #[case(&[("202607", 0)], Some("202607"))]
    fn test_first_empty_sheet(#[case] sheets: &[(&str, usize)], #[case] expected: Option<&str>) {
        assert_eq!(first_empty_sheet(sheets), expected);
    }

    #[rstest]
    #[case(0, "0 Zeilen")]
    #[case(1, "1 Zeile")]
    #[case(2, "2 Zeilen")]
    fn test_rows_phrase(#[case] count: usize, #[case] expected: &str) {
        assert_eq!(rows_phrase(count), expected);
    }

    #[rstest]
    #[case(1, "1 Blatt")]
    #[case(2, "2 Blättern")]
    fn test_sheets_phrase(#[case] count: usize, #[case] expected: &str) {
        assert_eq!(sheets_phrase(count), expected);
    }

    #[rstest]
    #[case(&[("202607", 253)], "«202607»: 253 Zeilen")]
    #[case(
        &[("details", 21), ("transaktionen", 1)],
        "«details»: 21 Zeilen, «transaktionen»: 1 Zeile"
    )]
    fn test_per_sheet_phrase(#[case] sheets: &[(&str, usize)], #[case] expected: &str) {
        assert_eq!(per_sheet_phrase(sheets), expected);
    }
}
