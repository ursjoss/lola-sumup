use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use polars::prelude::*;
use strum::IntoEnumIterator;

use crate::prepare::{Owner, PaymentMethod, Purpose, Topic};

/// check we only have valid payment methods
/// expects the `raw_df` to validate and the `columns` to display in case of a validation error.
pub fn validate_payment_methods(
    raw_df: &DataFrame,
    columns: &[Expr],
) -> Result<(), Box<dyn Error>> {
    let payment_methods: Vec<String> = PaymentMethod::iter().map(|t| t.to_string()).collect();
    validate_field(
        "Payment Method",
        payment_methods,
        FieldConstraintViolationError::PaymentMethod,
        raw_df,
        columns,
    )?;
    Ok(())
}

/// check we only have valid topics
/// expects the `raw_df` to validate and the `columns` to display in case of a validation error.
pub fn validate_topics(raw_df: &DataFrame, columns: &[Expr]) -> Result<(), Box<dyn Error>> {
    let topics: Vec<String> = Topic::iter().map(|t| t.to_string()).collect();
    validate_field(
        "Topic",
        topics,
        FieldConstraintViolationError::Topic,
        raw_df,
        columns,
    )?;
    Ok(())
}

/// check we only have empty or valid owners
/// expects the `raw_df` to validate and the `columns` to display in case of a validation error.
pub fn validate_owners(raw_df: &DataFrame, columns: &[Expr]) -> Result<(), Box<dyn Error>> {
    let mut owners: Vec<String> = Owner::iter().map(|t| t.to_string()).collect();
    owners.push(String::new());
    validate_field(
        "Owner",
        owners,
        FieldConstraintViolationError::Owner,
        raw_df,
        columns,
    )?;
    Ok(())
}

/// check we only have valid purposes
/// expects the `raw_df` to validate and the `columns` to display in case of a validation error.
pub fn validate_purposes(raw_df: &DataFrame, columns: &[Expr]) -> Result<(), Box<dyn Error>> {
    let purposes: Vec<String> = Purpose::iter().map(|t| t.to_string()).collect();
    validate_field(
        "Purpose",
        purposes,
        FieldConstraintViolationError::Purpose,
        raw_df,
        columns,
    )?;
    Ok(())
}

fn validate_field(
    field: &str,
    values: Vec<String>,
    error: fn(DataFrame) -> FieldConstraintViolationError,
    raw_df: &DataFrame,
    columns: &[Expr],
) -> Result<(), Box<dyn Error>> {
    let df = raw_df
        .clone()
        .lazy()
        .with_row_index("Row-No", Some(2))
        .filter(
            col(field)
                .fill_null(lit(""))
                .is_in(lit(Series::from_iter(values)))
                .not(),
        )
        .select(columns)
        .collect()?;
    if df.shape().0 > 0 {
        Err(Box::from(error(df)))
    } else {
        Ok(())
    }
}

enum FieldConstraintViolationError {
    PaymentMethod(DataFrame),
    Topic(DataFrame),
    Owner(DataFrame),
    Purpose(DataFrame),
}

impl Display for FieldConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldConstraintViolationError::PaymentMethod(df) => {
                write!(f, "Row has an invalid Payment Method! {df}")
            }
            FieldConstraintViolationError::Topic(df) => {
                write!(f, "Row has an invalid Topic! {df}")
            }
            FieldConstraintViolationError::Owner(df) => {
                write!(f, "Row has an invalid Owner! {df}")
            }
            FieldConstraintViolationError::Purpose(df) => {
                write!(f, "Row has an invalid Purpose! {df}")
            }
        }
    }
}

impl Debug for FieldConstraintViolationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Error for FieldConstraintViolationError {}

/// check constraints in `raw_df` read from intermediate file
/// expects the `raw_df` to validate and the `columns` to display in case of a validation error.
pub fn validation_topic_owner(raw_df: &DataFrame, columns: &[Expr]) -> Result<(), Box<dyn Error>> {
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::MiTiWithoutOwner,
        col("Topic")
            .eq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).eq(lit(""))),
        columns,
    )?;
    topic_owner_constraint(
        raw_df,
        TopicOwnerConstraintViolationError::LoLaWithOwner,
        col("Topic")
            .neq(lit(Topic::MiTi.to_string()))
            .and(col("Owner").fill_null(lit("")).neq(lit(""))),
        columns,
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
        write!(f, "{self}")
    }
}

impl Error for TopicOwnerConstraintViolationError {}

fn topic_owner_constraint(
    raw_df: &DataFrame,
    error: fn(DataFrame) -> TopicOwnerConstraintViolationError,
    predicate: Expr,
    columns: &[Expr],
) -> Result<(), Box<dyn Error>> {
    let df = raw_df
        .clone()
        .lazy()
        .with_row_index("Row-No", Some(2))
        .filter(predicate)
        .select(columns)
        .collect()?;
    if df.shape().0 > 0 {
        Err(Box::from(error(df)))
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
        let df = new_df(&topic.to_string(), owner, "Consumption", "Cash")?;
        let columns = [col("Row-No")];
        match validation_topic_owner(&df, &columns) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                let msg = error_msg.expect("should have an error message");
                assert!(e.to_string().starts_with(msg));
            }
        }
        Ok(())
    }

    fn new_df(
        topic: &str,
        owner: Option<&str>,
        purpose: &str,
        payment_method: &str,
    ) -> Result<DataFrame, PolarsError> {
        let df = df!(
            "Account" => &["a@b.ch"],
            "Date" => &["17.04.2023"],
            "Time" => &["12:32:00"],
            "Type" => &["Sales"],
            "Transaction ID" => &["TEGUCXAGDE"],
            "Payment Method" => &[payment_method],
            "Quantity" => &[1],
            "Description" => &["foo"],
            "Currency" => &["CHF"],
            "Price (Gross)" => &[16.0],
            "Price (Net)" => &[16.0],
            "Commission" => &[0.24],
            "Topic" => &[topic],
            "Owner" => &[owner],
            "Purpose" => &[purpose],
            "Comment" => &[None::<String>],
        )?;
        Ok(df)
    }

    #[rstest]
    #[case("Cash", true)]
    #[case("Card", true)]
    #[case("", false)]
    #[case("Cash ", false)]
    #[case(" Card", false)]
    #[case("xyz", false)]
    fn validate_payment_method(
        #[case] payment_method: &str,
        #[case] expected_valid: bool,
    ) -> Result<(), Box<dyn Error>> {
        let df = new_df("cafe", None, "", payment_method)?;
        let columns = [col("Row-No")];
        match validate_payment_methods(&df, &columns) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                assert!(e
                    .to_string()
                    .starts_with("Row has an invalid Payment Method!"));
            }
        }
        Ok(())
    }

    #[rstest]
    #[case("MiTi", true)]
    #[case("Cafe", true)]
    #[case("Verm", true)]
    #[case("Deposit", true)]
    #[case("Rental", true)]
    #[case("Culture", true)]
    #[case("PaidOut", true)]
    #[case("", false)]
    #[case("Vermiet", false)]
    #[case("xyz", false)]
    fn validate_topic(
        #[case] topic: &str,
        #[case] expected_valid: bool,
    ) -> Result<(), Box<dyn Error>> {
        let df = new_df(topic, None, "", "Cash")?;
        let columns = [col("Row-No")];
        match validate_topics(&df, &columns) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                assert!(e.to_string().starts_with("Row has an invalid Topic!"));
            }
        }
        Ok(())
    }

    #[rstest]
    #[case(Some("LoLa"), true)]
    #[case(Some("MiTi"), true)]
    #[case(Some(""), true)]
    #[case(None, true)]
    #[case(Some("miti"), false)]
    #[case(Some("XX"), false)]
    fn validate_owner(
        #[case] owner: Option<&str>,
        #[case] expected_valid: bool,
    ) -> Result<(), Box<dyn Error>> {
        let df = new_df("", owner, "", "Card")?;
        let columns = [col("Row-No")];
        match validate_owners(&df, &columns) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                assert!(e.to_string().starts_with("Row has an invalid Owner!"));
            }
        }
        Ok(())
    }

    #[rstest]
    #[case("Consumption", true)]
    #[case("Tip", true)]
    #[case("", false)]
    #[case("XX", false)]
    fn validate_purpose(
        #[case] purpose: &str,
        #[case] expected_valid: bool,
    ) -> Result<(), Box<dyn Error>> {
        let df = new_df("", None, purpose, "Cash")?;
        let columns = [col("Row-No")];
        match validate_purposes(&df, &columns) {
            Ok(()) => assert!(expected_valid),
            Err(e) => {
                assert!(!expected_valid);
                assert!(e.to_string().starts_with("Row has an invalid Purpose!"));
            }
        }
        Ok(())
    }
}
