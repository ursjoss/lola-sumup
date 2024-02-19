use polars::frame::DataFrame;

/// Asserts two dataframes by first checking the shape, then by comparing row by row
/// and last but not least applying a `equals_missing`.
pub fn assert_dataframe(actual: &DataFrame, expected: &DataFrame) {
    assert_eq!(
        actual.shape(),
        expected.shape(),
        "shape of the dataframes is not matching!"
    );
    for n in 0..actual.shape().0 {
        assert_eq!(
            actual.get_row(n).unwrap(),
            expected.get_row(n).unwrap(),
            "row {n} is not matching the expectation!",
        );
    }
    assert!(actual.equals_missing(expected));
}
