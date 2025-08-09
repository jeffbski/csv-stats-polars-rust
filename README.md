# csv-stats-polars-rust

Example rust project which uses polars library to read a CSV file and calculate some statistics

## Usage

```sh
cargo build --release
target/release/csv-stats-polars-rust --help # Display help
target/release/csv-stats-polars-rust -f FILE_PATH -c COLUMN_NAME # Calculate stats for a column in a CSV file
```

## Resources

- Polars home - https://pola.rs/
- Polars User Guide - https://docs.pola.rs/
- Rust Polars API Reference - https://docs.rs/polars/latest/polars/
