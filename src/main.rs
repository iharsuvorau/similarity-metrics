use std::error::Error;
use std::fs::File;
use std::io::prelude::*;

use polars::prelude::DataType::Datetime;
use polars::prelude::TimeUnit::Milliseconds;
use polars::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let filename_one = &args[1];
    let filename_two = &args[2];

    let df_a = load_log_df(filename_one)?;
    let df_b = load_log_df(filename_two)?;

    let columns = &[
        "concept:name",
        "Resource",
        "start_timestamp",
        "time:timestamp",
    ];

    let a_col = concatenate_columns(&df_a, columns);
    let b_col = concatenate_columns(&df_b, columns);

    let distance = distance_polars(&a_col, &b_col).expect("Distance computation failed");
    println!("The Damerau-Levenshtein distance: {distance}");

    let similarity = similarity(distance, a_col.len());
    println!("The similarity: {similarity}");

    Ok(())
}

/// Load the event logs from the CSV files.
fn load_log(filename: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut log = Vec::new();

    let mut file = File::open(filename)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    for line in contents.lines() {
        log.push(line.to_string());
    }

    Ok(log)
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

fn distance_csv() -> Result<(), Box<dyn Error>> {
    // Get args from command line
    let args: Vec<String> = std::env::args().collect();
    let filename_one = &args[1];
    let filename_two = &args[2];

    let log1 = load_log(filename_one)?;
    let log2 = load_log(filename_two)?;

    // Compute the lengths of the event logs
    let m = log1.len();
    let n = log2.len();

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
            if log1[i - 1] == log2[j - 1] {
                distance[i][j] = distance[i - 1][j - 1];
            } else {
                // Otherwise, the distance is the minimum of the previous row, column, or diagonal plus one
                distance[i][j] = std::cmp::min(distance[i - 1][j] + 1, distance[i][j - 1] + 1);
                distance[i][j] = std::cmp::min(distance[i][j], distance[i - 1][j - 1] + 1);
            }
        }
    }

    // The Damerau-Levenshtein distance is equal to the value in the bottom-right cell of the matrix
    println!("{}", distance[m][n]);

    Ok(())
}

fn similarity(distance: usize, length: usize) -> f64 {
    1.0 - (distance as f64 / length as f64)
}

/// The Damerau-Levenshtein distance calculation.
fn distance_polars(log1: &Vec<String>, log2: &Vec<String>) -> Result<usize, Box<dyn Error>> {
    // Compute the lengths of the event logs
    let m = log1.len();
    let n = log2.len();

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
            if log1[i - 1] == log2[j - 1] {
                distance[i][j] = distance[i - 1][j - 1];
            } else {
                // Otherwise, the distance is the minimum of the previous row, column, or diagonal plus one
                distance[i][j] = std::cmp::min(distance[i - 1][j] + 1, distance[i][j - 1] + 1);
                distance[i][j] = std::cmp::min(distance[i][j], distance[i - 1][j - 1] + 1);
            }
        }
    }

    // The Damerau-Levenshtein distance is equal to the value in the bottom-right cell of the matrix
    Ok(distance[m][n])
}

fn distance_alt(log1: &Vec<String>, log2: &Vec<String>) -> usize {
    let a_str = log1.join(" ");
    let b_str = log2.join(" ");
    distance::damerau_levenshtein(&a_str, &b_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_to_string_conversion() {
        let series = Series::new("strings", &["foo", "bar", "baz"]);

        let result = series
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .replace("\"", "");

        assert_eq!(result, "foo bar baz");
    }

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
