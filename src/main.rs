use std::error::Error;

use similarity_metrics::string_distances::damerau_levenshtein_on_logs;

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
