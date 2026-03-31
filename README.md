# pipeline-big_five_personality
Data pipeline: SQL Server -> PySpark -> Plotly -> Airflow

## Local setup

1. Create/activate virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `scripts/.env.example` to `scripts/.env` and fill your local values.

## Data artifacts

This repository does not store data files. Keep `data/` local and do not commit
raw or bronze artifacts.

## Run SQL load script

Dry-run (reads CSV, validates config, does not write):

```powershell
python .\scripts\load_to_sqlserver.py --dry-run --source data\raw\data-final.csv
```

Write to SQL Server (explicit mode recommended):

```powershell
python .\scripts\load_to_sqlserver.py --source data\raw\data-final.csv --table big_five_raw --if-exists append
```

## Tests

```powershell
.venv\Scripts\pytest.exe -q
```

