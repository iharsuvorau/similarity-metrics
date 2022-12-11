use std::error::Error;

use polars::prelude::DataType::Datetime;
use polars::prelude::TimeUnit::Milliseconds;
use polars::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let filename_one = &args[1];
    let filename_two = &args[2];

    let columns = &[
        "concept:name",
        "Resource",
        "start_timestamp",
        "time:timestamp",
    ];

    let (distance, similarity) = damerau_levenshtein_on_logs(filename_one, filename_two, columns);

    println!("The Damerau-Levenshtein distance: {distance}");
    println!("The similarity: {similarity}");

    Ok(())
}

/// The Damerau-Levenshtein distance calculation given two event logs and columns to concatenate into a string.
///
/// # Arguments
///
/// * `filename_one` - The first event log file.
/// * `filename_two` - The second event log file.
/// * `columns` - The columns to concatenate into a string.
///
/// # Returns
///
/// * `distance` - The Damerau-Levenshtein distance.
/// * `similarity` - The similarity of two event logs on the relative scale.
///
/// # Examples
///
/// ```
/// use damerau_levenshtein_on_logs;
///
/// let (distance, similarity) = damerau_levenshtein_on_logs("filename_one.csv", "filename_two.csv", &["concept:name", "org:resource", "start_timestamp", "time:timestamp"]);
/// ```
pub fn damerau_levenshtein_on_logs(
    filename_one: &String,
    filename_two: &String,
    columns: &[&str],
) -> (usize, f64) {
    let df_a = load_log_df(filename_one).unwrap();
    let df_b = load_log_df(filename_two).unwrap();

    let a_col = concatenate_columns(&df_a, columns);
    let b_col = concatenate_columns(&df_b, columns);

    let distance = damerau_levenshtein(&a_col, &b_col);

    let similarity = similarity(distance, a_col.len());

    (distance, similarity)
}

/// The Damerau-Levenshtein distance calculation given two vectors of strings.
///
/// # Arguments
///
/// * `log_one` - The first vector of strings collected from the first event log.
/// * `log_two` - The second vector of strings collected from the second event log.
///
/// # Returns
///
/// The Damerau-Levenshtein distance between the two vectors of strings.
///
/// # Examples
///
/// ```
/// use polars::prelude::*;
/// use polars::prelude::DataType::Datetime;
/// use polars::prelude::TimeUnit::Milliseconds;
/// use polars::prelude::*;
///
/// let df_a = load_log_df(filename_one)?;
/// let df_b = load_log_df(filename_two)?;
///
/// let columns = &[
///     "concept:name",
///     "Resource",
///     "start_timestamp",
///     "time:timestamp",
/// ];
///
/// let a_col = concatenate_columns(&df_a, columns);
/// let b_col = concatenate_columns(&df_b, columns);
///
/// let distance = damerau_levenshtein(&a_col, &b_col).expect("Distance computation failed");
/// println!("The Damerau-Levenshtein distance: {distance}");
/// ```
pub fn damerau_levenshtein(log_one: &Vec<String>, log_two: &Vec<String>) -> usize {
    // Compute the lengths of the event logs
    let m = log_one.len();
    let n = log_two.len();

    // Create a matrix to store the Damerau-Levenshtein distances
    let mut distance = vec![vec![0; n + 1]; m + 1];

    // Initialize the first row and column of the matrix
    for i in 0..m + 1 {
        distance[i][0] = i;
    }
    for j in 0..n + 1 {
        distance[0][j] = j;
    }

    // Iterate over each row and column in the matrix
    for i in 1..m + 1 {
        for j in 1..n + 1 {
            // If the characters in the two logs are the same, the distance is equal to the value in the previous cell
            if log_one[i - 1] == log_two[j - 1] {
                distance[i][j] = distance[i - 1][j - 1];
            } else {
                // Otherwise, the distance is the minimum of the previous row, column, or diagonal plus one
                distance[i][j] = std::cmp::min(distance[i - 1][j] + 1, distance[i][j - 1] + 1);
                distance[i][j] = std::cmp::min(distance[i][j], distance[i - 1][j - 1] + 1);
            }
        }
    }

    // The Damerau-Levenshtein distance is equal to the value in the bottom-right cell of the matrix
    distance[m][n]
}

/// Computes the similarity between two event logs given the Damerau-Levenshtein distance and the length of any
/// of the event logs.
pub fn similarity(distance: usize, length: usize) -> f64 {
    1.0 - (distance as f64 / length as f64)
}

fn concatenate_columns(input: &DataFrame, column_names: &[&str]) -> Vec<String> {
    assert_ne!(column_names.len(), 0, "No columns to concatenate");
    assert_ne!(column_names.len(), 1, "Only one column to concatenate");

    let mut df = input.clone();

    for i in 0..column_names.len() {
        if i + 1 == column_names.len() {
            break;
        }

        // Concatenating the first column with all the others

        let a = column_names[0];
        let b = column_names[i + 1];

        df.with_column(df.column(a).unwrap() + df.column(b).unwrap())
            .unwrap();
    }

    df.column(column_names[0])
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .map(|x| x.unwrap().to_string())
        .collect()
}

fn load_log_df(filename: &str) -> PolarsResult<DataFrame> {
    // TODO: refactor hard-coded column names

    LazyCsvReader::new(filename)
        .has_header(true)
        .finish()?
        .select(&[
            col("concept:name"),
            col("Resource"),
            col("start_timestamp"),
            col("time:timestamp"),
        ])
        .with_column(
            col("start_timestamp")
                .str()
                .strptime(StrpTimeOptions {
                    date_dtype: Datetime(Milliseconds, None),
                    fmt: Some("%Y-%m-%d %H:%M:%S%z".into()),
                    strict: false,
                    exact: false,
                })
                .alias("start_timestamp_dt"),
        )
        .with_column(
            col("time:timestamp")
                .str()
                .strptime(StrpTimeOptions {
                    date_dtype: Datetime(Milliseconds, None),
                    fmt: Some("%Y-%m-%d %H:%M:%S%z".into()),
                    strict: false,
                    exact: false,
                })
                .alias("time:timestamp_dt"),
        )
        .sort(
            "start_timestamp_dt",
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concatenate_columns() {
        let df = DataFrame::new(vec![
            Series::new("a", &["foo", "bar", "baz"]),
            Series::new("b", &["_one", "_two", "_three"]),
            Series::new("c", &["_a", "_b", "_c"]),
        ])
        .unwrap();

        let result = concatenate_columns(&df, &["a", "b", "c"]);

        let expected = vec!["foo_one_a", "bar_two_b", "baz_three_c"];

        dbg!(&result);

        assert_eq!(result, expected);
    }
}
