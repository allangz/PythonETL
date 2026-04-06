# Python ETL Pipeline

An Example ETL (Extract, Transform, Load) pipeline built with Python and Pandas to process CSV data,
the example CSV file was taken from: https://github.com/datablist/sample-csv-files,
apply business logic, and store results in an SQLite database.

## Project Structure

- `main.py`: The entry point and core logic of the ETL pipeline.
- `config.py`: Configuration file for business rules, database names, and logging.
- `test_main.py`: Unit tests for the transformation logic.
- `data/`: Directory for input CSV files, extract the .zip file to test locally.
- `leads_data.db`: SQLite database where results are stored.
- `python_etl.log`: Log file for tracking pipeline execution and errors.

## Installation

This project uses `uv` for dependency management. To install dependencies:

```bash
uv sync
```

## Usage

To run the ETL pipeline, provide the path to the source CSV file as an argument:

```bash
python main.py data/organizations-500000.csv
```

## Testing

Run the unit tests using `pytest`:

```bash
pytest test_main.py
```

## Logging

All pipeline activities, including successes and failures, are recorded in `python_etl.log`.
