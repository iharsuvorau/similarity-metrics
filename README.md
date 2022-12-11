# similarity-metrics

[![crates.io](https://img.shields.io/crates/v/similarity-metrics.svg)](https://crates.io/crates/similarity-metrics)
[![docs.rs](https://docs.rs/similarity-metrics/badge.svg)](https://docs.rs/similarity-metrics)

Library to compute event logs similarity metrics.

## Examples

```rust
use similarity_metrics::string_distances::damerau_levenshtein_on_logs;

let path_one = "filename_one.csv";
let path_two = "filename_two.csv";
let columns = &["concept:name", "org:resource", "start_timestamp", "time:timestamp"];

let (distance, similarity) = damerau_levenshtein_on_logs(path_one, path_two, columns);
```

## Features

* `damerau_levenshtein` - Compute the Damerau-Levenshtein distance between two event logs.
