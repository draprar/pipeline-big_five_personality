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

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_db_url():
    """
    Prefer DB_URL env var (full SQLAlchemy URL). Fallback to component env vars for
    Windows integrated auth or local dev.
    """
    url = os.getenv("DB_URL")
    if url:
        return url

    # component fallback (legacy)
    host = os.getenv("DB_HOST", "WALERY\\SQLEXPRESS")
    db = os.getenv("DB_NAME", "BigFiveDB")
    trusted = os.getenv("DB_TRUSTED", "yes")
    driver = os.getenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")
    logger.warning("Using fallback component DB env vars. Set DB_URL to avoid this warning.")
    return f"mssql+pyodbc://{host}/{db}?driver={driver}&trusted_connection={trusted}"


def parse_args():
    p = argparse.ArgumentParser(description="Load CSV into SQL Server (pandas.to_sql).")
    p.add_argument("--source", "-s", default="data/raw/data-final.csv", help="Path to CSV file")
    p.add_argument("--sep", default="\t", help="CSV separator")
    p.add_argument("--table", "-t", default="big_five_raw", help="Target table name")
    p.add_argument("--chunksize", type=int, default=5000, help="Rows per chunk for to_sql")
    p.add_argument("--if-exists", choices=("fail", "replace", "append"), default="replace", help="to_sql if_exists mode")
    p.add_argument("--dry-run", action="store_true", help="Do everything except write to DB")
    return p.parse_args()


def main():
    args = parse_args()

    db_url = build_db_url()
    logger.info("DB URL source: %s", "DB_URL env" if os.getenv("DB_URL") else "fallback components")
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
        logger.info("Writing to database (chunksize=%s, if_exists=%s)...", args.chunksize, args.if_exists)
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
