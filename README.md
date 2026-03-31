# Pipeline - Big Five
Data pipeline: SQL Server -> PySpark -> Plotly -> Airflow

## Overview
This project ingests the Big Five dataset from SQL Server, writes a bronze layer in Parquet,
then transforms data for downstream analytics and visualization.

## Pipeline (Bronze -> Silver -> Gold)
1. **Bronze**: raw extract from `vw_big_five_clean` into partitioned Parquet.
2. **Silver**: transformations, reverse scoring, trait calculations.
3. **Gold**: aggregates and visualization-ready datasets.

## Prerequisites
- Python 3.12
- Java 11+ (required by PySpark)
- SQL Server 2019 (local or reachable) with view `vw_big_five_clean`
- JDBC driver in `drivers/`

## Local setup
1. Create/activate virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `scripts/.env.example` to `scripts/.env` and fill your local values.

## Notebook parameters (recommended)
The extract notebook supports in-notebook parameters. You can override these via
environment variables if needed (avoid putting secrets in notebook outputs).

Key parameters:
- `BRONZE_PATH`
- `JDBC_JAR`
- `JDBC_URL`
- `SOURCE_VIEW`
- `EXPECTED_COLS`, `EXPECTED_MIN_ROWS`, `SCORE_MIN`, `SCORE_MAX`

## Run: Extract Bronze
Open `notebook/01_extract_bronze.ipynb` and run all cells. The output is written to:
`data/bronze/big_five/` with `_bronze_meta.json` metadata.

## Run SQL load script
Dry-run (reads CSV, validates config, does not write):

```powershell
python .\scripts\load_to_sqlserver.py --dry-run --source data\raw\data-final.csv
```

Write to SQL Server (explicit mode recommended):

```powershell
python .\scripts\load_to_sqlserver.py --source data\raw\data-final.csv --table big_five_raw --if-exists append
```

## Data artifacts
This repository does not store data files. Keep `data/` local and do not commit
raw or bronze artifacts. Use `data/docs/codebook.txt` for the dataset reference.

## CI and code quality
- `ruff check . --exclude notebook`
- `black --check .`
- `pytest -q`

## Project structure
- `notebook/`: notebooks for extraction, EDA, transformation, and visualization
- `scripts/`: SQL Server loader and helpers
- `spark/`: Spark session utilities
- `sql/`: SQL views and DDL
- `airflow/`: DAG definitions
- `data/docs/`: codebook and documentation

## Tests
```powershell
.venv\Scripts\pytest.exe -q
```
