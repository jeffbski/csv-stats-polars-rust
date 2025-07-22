use anyhow::{Result, anyhow};
use clap::Parser;
use polars::prelude::*;
use std::path::PathBuf;

/// A CLI tool to calculate statistics for a numeric column in a CSV file.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the CSV file.
    #[arg(short, long)]
    file_path: PathBuf,

    /// The name of the column to analyze.
    #[arg(short, long, default_value = "Amount Received")]
    column_name: String,
}

/// A container for the calculated statistics.
#[derive(Debug)]
struct SelectedStats {
    /// Total number of records (rows).
    count: usize,
    /// The minimum value in the column.
    min: Option<f64>,
    /// The maximum value in the column.
    max: Option<f64>,
    /// The sum of all values in the column.
    sum: Option<f64>,
    /// The mean (average) of all values in the column.
    mean: Option<f64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Execute the data processing function.
    let stats = process_csv(&cli.file_path, &cli.column_name)?;

    // Print the results directly.
    println!("--- Statistics for '{}' ---", cli.column_name);
    println!("Count: {}", stats.count);

    // Helper to format Option<f64> values consistently.
    let format_opt = |val: Option<f64>| {
        val.map(|v| format!("{:.4}", v))
            .unwrap_or_else(|| "N/A".to_string())
    };

    println!("Min:   {}", format_opt(stats.min));
    println!("Max:   {}", format_opt(stats.max));
    println!("Sum:   {}", format_opt(stats.sum));
    println!("Mean:  {}", format_opt(stats.mean));

    Ok(())
}

/// Reads a CSV file and calculates descriptive statistics for a specified column.
///
/// This function uses the modern Polars API with CsvReadOptions to robustly
/// read a CSV file into a DataFrame.
fn process_csv(file_path: &PathBuf, column_name: &str) -> Result<SelectedStats> {
    // Use the CsvReadOptions pattern to configure and read the CSV file.
    // This is the modern, idiomatic way to handle CSV parsing in Polars.
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .try_into_reader_with_file_path(Some(file_path.clone()))?
        .finish()?;

    // Attempt to select the specified column from the DataFrame.
    // Provide a helpful error message if the column is not found.
    let series = df.column(column_name).map_err(|e| {
        anyhow!(
            "Failed to find column '{}': {}. Available columns: {:?}",
            column_name,
            e,
            df.get_column_names()
        )
    })?;

    // To perform numeric calculations, we must work with a numeric type.
    // We'll cast the series to a ChunkedArray of Float64. This will fail
    // if the column contains non-numeric data, which is the desired behavior.
    let num_series = series.f64()?;

    // Now we can calculate the statistics directly on the numeric series.
    // These methods return Option<f64> to gracefully handle empty or all-null series.
    let stats = SelectedStats {
        count: num_series.len(),
        min: num_series.min(),
        max: num_series.max(),
        sum: num_series.sum(),
        mean: num_series.mean(),
    };

    Ok(stats)
}
