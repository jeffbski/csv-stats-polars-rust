use anyhow::Result;
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

    // Helper to format Option<f64> values consistently to 4 decimal places.
    let format_opt = |val: Option<f64>| {
        val.map(|v| format!("{:.4}", v))
            .unwrap_or_else(|| "N/A".to_string())
    };

    // Print the results line by line.
    println!("--- Statistics for '{}' ---", cli.column_name);
    println!("Count: {}", stats.count);
    println!("Min:   {}", format_opt(stats.min));
    println!("Max:   {}", format_opt(stats.max));
    println!("Sum:   {}", format_opt(stats.sum));
    println!("Mean:  {}", format_opt(stats.mean));

    Ok(())
}

/// Reads a CSV file and calculates descriptive statistics for a specified column using LazyFrame.
///
/// This function uses the Polars lazy API to build an optimized query plan,
/// which is ideal for performance on large datasets.
fn process_csv(file_path: &PathBuf, column_name: &str) -> Result<SelectedStats> {
    // Create a LazyFrame from the CSV file. This does not read the file yet, only sets up the plan.
    let lf = LazyCsvReader::new(file_path.clone())
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .finish()?;

    // Build a query plan to calculate all statistics in a single pass.
    // We cast the target column to Float64 to ensure numeric operations are valid.
    let aggregations = [
        // The `count` aggregation works on any type, no cast needed.
        col(column_name).count().alias("count"),
        // For numeric stats, we first cast the column to f64.
        col(column_name).cast(DataType::Float64).min().alias("min"),
        col(column_name).cast(DataType::Float64).max().alias("max"),
        col(column_name).cast(DataType::Float64).sum().alias("sum"),
        col(column_name)
            .cast(DataType::Float64)
            .mean()
            .alias("mean"),
    ];

    // Execute the query. This materializes the result into a DataFrame.
    // The resulting DataFrame will have a single row with our calculated stats.
    let stats_df = lf.select(aggregations).collect()?;

    // Helper to extract an optional f64 stat value from the results DataFrame.
    // The DataFrame has only one row, so we always get the value at index 0.
    let get_optional_f64 = |df: &DataFrame, stat_name: &str| -> Result<Option<f64>> {
        let any_value = df.column(stat_name)?.get(0)?;
        match any_value {
            AnyValue::Null => Ok(None),
            // The `try_extract` method will handle the conversion from AnyValue to f64.
            // The `?` will propagate any PolarsError, which gets converted into an anyhow::Error.
            av => Ok(Some(av.try_extract()?)),
        }
    };

    // The count is a special case as it's a u32, not an optional f64.
    let count = stats_df.column("count")?.get(0)?.try_extract::<u32>()? as usize;

    // Extract all the required stats using the helpers.
    let stats = SelectedStats {
        count,
        min: get_optional_f64(&stats_df, "min")?,
        max: get_optional_f64(&stats_df, "max")?,
        sum: get_optional_f64(&stats_df, "sum")?,
        mean: get_optional_f64(&stats_df, "mean")?,
    };

    Ok(stats)
}
