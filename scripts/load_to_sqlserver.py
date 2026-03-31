"""
Small utility to load CSV -> SQL Server table using pandas + SQLAlchemy.
Uses environment variables for DB configuration (DB_URL preferred).
Provides a --dry-run mode and basic logging + error handling.
"""

import os
import sys
import argparse
import logging
import time
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_local_env(env_path: Path):
    """Load KEY=VALUE pairs from a local .env file if present.
    Existing environment variables are not overwritten.
    """
    if not env_path.exists():
        return

    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] in {'"', "'"} and value[-1] == value[0]:
            value = value[1:-1]
        else:
            # Strip inline comments like: KEY=value  # comment
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        if key and key not in os.environ:
            os.environ[key] = value


def build_db_url():
    """
    Prefer DB_URL env var (full SQLAlchemy URL). Fallback to component env vars for
    Windows integrated auth or local dev.
    """
    url = os.getenv("DB_URL")
    if url:
        return url

    # component fallback
    host = os.getenv("DB_HOST")
    db = os.getenv("DB_NAME")
    trusted = os.getenv("DB_TRUSTED", "yes")
    driver = os.getenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")

    if not host or not db:
        raise ValueError(
            "Missing database configuration. Set DB_URL or both DB_HOST and DB_NAME "
            "(you can store them in scripts/.env)."
        )

    logger.warning(
        "Using fallback component DB env vars. Set DB_URL to avoid this warning."
    )
    return f"mssql+pyodbc://{host}/{db}?driver={driver}&trusted_connection={trusted}"


def parse_args():
    p = argparse.ArgumentParser(description="Load CSV into SQL Server (pandas.to_sql).")
    p.add_argument(
        "--source", "-s", default="data/raw/data-final.csv", help="Path to CSV file"
    )
    p.add_argument("--sep", default="\t", help="CSV separator")
    p.add_argument("--table", "-t", default="big_five_raw", help="Target table name")
    p.add_argument(
        "--chunksize", type=int, default=5000, help="Rows per chunk for to_sql"
    )
    p.add_argument(
        "--if-exists",
        choices=("fail", "replace", "append"),
        default="fail",
        help="to_sql if_exists mode",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Do everything except write to DB"
    )
    return p.parse_args()


def main():
    env_path = Path(__file__).resolve().with_name(".env")
    load_local_env(env_path)

    args = parse_args()

    try:
        db_url = build_db_url()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(2)

    logger.info(
        "DB URL source: %s",
        "DB_URL env" if os.getenv("DB_URL") else "fallback components",
    )
    logger.info("CSV source: %s", args.source)
    logger.info("Target table: %s", args.table)

    if not os.path.exists(args.source):
        logger.error("Source CSV not found: %s", args.source)
        sys.exit(2)

    try:
        logger.info("Reading CSV (this may take a while)...")
        df = pd.read_csv(args.source, sep=args.sep, low_memory=False)
        logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    except Exception as e:
        logger.exception("Failed to read CSV: %s", e)
        sys.exit(3)

    if args.dry_run:
        logger.info("Dry-run enabled -> skipping DB write")
        return

    try:
        engine = create_engine(db_url)
        logger.info(
            "Writing to database (chunksize=%s, if_exists=%s)...",
            args.chunksize,
            args.if_exists,
        )
        start = time.time()
        df.to_sql(
            args.table,
            engine,
            if_exists=args.if_exists,
            index=False,
            chunksize=args.chunksize,
        )
        elapsed = time.time() - start
        logger.info("Write complete - took %.2f seconds", elapsed)
    except SQLAlchemyError as e:
        logger.exception("Database operation failed: %s", e)
        sys.exit(4)
    except Exception as e:
        logger.exception("Unexpected error during DB write: %s", e)
        sys.exit(5)


if __name__ == "__main__":
    main()
