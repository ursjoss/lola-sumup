use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use polars::prelude::*;

use crate::prepare::Topic;

/// check constraints in `raw_df` read from intermediate file
pub fn validation_topic_owner(raw_df: &DataFrame) -> Result<(), Box<dyn Error>> {
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::MiTiWithoutOwner,
        col("Topic")
            .eq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).eq(lit(""))),
    )?;
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::LoLaWithOwner,
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).neq(lit(""))),
    )?;
    Ok(())
}

enum TopicOwnerConstraintViolationError {
    MiTiWithoutOwner(DataFrame),
    LoLaWithOwner(DataFrame),
}

impl Display for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicOwnerConstraintViolationError::MiTiWithoutOwner(df) => {
                write!(f, "Row with topic 'MiTi' must have an Owner! {df}")
            }
            TopicOwnerConstraintViolationError::LoLaWithOwner(df) => {
                write!(
                    f,
                    "Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner! {df}"
                )
            }
        }
    }
}

impl Debug for TopicOwnerConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}:?")
    }
}

impl Error for TopicOwnerConstraintViolationError {}

fn topic_owner_constraint(
    raw_df: &DataFrame,
    error: fn(DataFrame) -> TopicOwnerConstraintViolationError,
    predicate: Expr,
) -> Result<(), Box<dyn Error>> {
    let df = raw_df
        .clone()
        .lazy()
        .with_row_count("Row-No", Some(2))
        .filter(predicate)
        .select([
            col("Row-No"),
            col("Date"),
            col("Time"),
            col("Transaction ID"),
            col("Description"),
            col("Price (Gross)"),
            col("Topic"),
            col("Owner"),
            col("Purpose"),
            col("Comment"),
        ])
        .collect()?;
    if df.shape().0 > 0 {
        Err(Box::try_from(error(df)).unwrap())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    // MiTi must have an owner
    #[case(Topic::MiTi, Some("LoLa"), true, None)]
    #[case(Topic::MiTi, Some("MiTi"), true, None)]
    // LoLa (Cafe/Verm) must have no owner
    #[case(Topic::Cafe, Some(""), true, None)]
    #[case(Topic::Verm, Some(""), true, None)]
    #[case(Topic::Verm, None, true, None)]
    // MiTi w/o or non-MiTi with owner should fail
    #[case(
        Topic::MiTi,
        Some(""),
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::MiTi,
        None,
        false,
        Some("Row with topic 'MiTi' must have an Owner!")
    )]
    #[case(
        Topic::Cafe,
        Some("Cafe"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
    )]
    #[case(
        Topic::Verm,
        Some("MiTi"),
        false,
        Some("Row with LoLa topic ('Cafe' or 'Verm') must not have an Owner!")
    )]
    fn test_constraints(
        #[case] topic: Topic,
        #[case] owner: Option<&str>,
        #[case] expected_valid: bool,
        #[case] error_msg: Option<&str>,
    ) -> PolarsResult<()> {
        let df = df!(
            "Account" => &["a@b.ch"],
            "Date" => &["17.04.2023"],
            "Time" => &["12:32:00"],
            "Type" => &["Sales"],
            "Transaction ID" => &["TEGUCXAGDE"],
            "Receipt Number" => &["S20230000303"],
            "Payment Method" => &["Cash"],
            "Quantity" => &[1],
            "Description" => &["foo"],
            "Currency" => &["CHF"],
            "Price (Gross)" => &[16.0],
            "Price (Net)" => &[16.0],
            "Tax" => &["0.0%"],
            "Tax rate" => &[""],
            "Transaction refunded" => &[""],
            "Commission" => &[0.24],
            "Topic" => &[topic.to_string()],
            "Owner" => &[owner],
            "Purpose" => &["Consumption"],
            "Comment" => &[None::<String>],
        )?;
        match validation_topic_owner(&df) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                let msg = error_msg.expect("should have an error message");
                assert!(e.to_string().starts_with(msg));
            }
        }
        Ok(())
    }
}
