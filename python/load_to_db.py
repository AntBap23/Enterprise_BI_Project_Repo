"""
Load (already-cleaned) CSVs into Postgres "raw" tables.
- Reads config from .env
- Creates raw schema if missing
- Loads all CSVs from a directory into raw.<table_name>
- Uses chunked loads for large files
- No cleaning/validation logic here (handled upstream by your ingestion.py pipeline)

"""

import logging
import os
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


# LOGGING SETUP

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# LOAD ENV

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATA_DIR = os.getenv("DATA_DIR", "data/cleaned")          # where cleaned CSVs live
RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")              # target schema for raw tables
IF_EXISTS = os.getenv("IF_EXISTS", "replace").lower()    # replace | append
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "50000"))
CSV_GLOB = os.getenv("CSV_GLOB", "*.csv")                # e.g., "*.csv"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))         # retry attempts for transient failures

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is required in your .env file")



# HELPERS

def snake_case(name: str) -> str:
    """Convert string to snake_case."""
    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"_+", "_", name).strip("_").lower()


def validate_schema_name(schema: str) -> bool:
    """Validate schema name to prevent SQL injection."""
    # PostgreSQL identifiers can contain letters, digits, underscores, and must start with letter/underscore
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema))


def build_engine() -> Engine:
    """Create SQLAlchemy engine with connection pooling."""
    # Removed future=True as it's deprecated in SQLAlchemy 2.0+
    # Added pool settings for better connection management
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,  # Verify connections before using
        pool_size=5,
        max_overflow=10,
        echo=False  # Set to True for SQL debugging
    )


def ensure_schema(engine: Engine, schema: str) -> None:
    """Create schema if it doesn't exist. Uses parameterized query for safety."""
    if not validate_schema_name(schema):
        raise ValueError(f"Invalid schema name: {schema}. Must be alphanumeric with underscores.")
    
    # Use identifier quoting to safely handle schema names
    # PostgreSQL requires double quotes for case-sensitive identifiers
    with engine.begin() as conn:
        # Use text() with bind parameters where possible, but for DDL we need to quote identifiers
        # This is safe because we've validated the schema name above
        quoted_schema = f'"{schema}"'
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema};"))
    logger.info(f"Schema '{schema}' ensured")


def infer_table_name(csv_path: Path) -> str:
    """
    Default behavior:
      data/cleaned/cleaned_orders.csv -> cleaned_orders
    If you want to strip prefixes like 'cleaned_' or 'raw_', set TABLE_PREFIX_STRIP in .env.
    """
    table_name = snake_case(csv_path.stem)

    # Optional prefix stripping (comma-separated prefixes)
    prefixes = os.getenv("TABLE_PREFIX_STRIP", "").strip()
    if prefixes:
        for p in [snake_case(x) for x in prefixes.split(",") if x.strip()]:
            if table_name.startswith(p):
                table_name = table_name[len(p):].lstrip("_")

    return table_name


def list_csvs(data_dir: str, pattern: str) -> List[Path]:
    """List all CSV files matching the pattern in the data directory."""
    base = Path(data_dir)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"DATA_DIR not found or not a directory: {data_dir}")

    files = sorted(base.glob(pattern))
    if not files:
        raise RuntimeError(f"No CSV files matching '{pattern}' found in {data_dir}")

    return files


def get_table_row_count(engine: Engine, schema: str, table: str) -> int:
    """Get the current row count of a table."""
    if not validate_schema_name(schema) or not validate_schema_name(table):
        return 0
    
    try:
        with engine.connect() as conn:
            quoted_schema = f'"{schema}"'
            quoted_table = f'"{table}"'
            result = conn.execute(text(f'SELECT COUNT(*) FROM {quoted_schema}.{quoted_table}'))
            return result.scalar() or 0
    except SQLAlchemyError:
        return 0


def load_csv_to_raw(
    engine: Engine,
    csv_path: Path,
    schema: str,
    table: str,
    if_exists: str,
    chunksize: int,
) -> None:
    """Load CSV file into PostgreSQL table with error handling and progress tracking."""
    logger.info(f"Loading {csv_path.name} -> {schema}.{table}")
    
    if not validate_schema_name(schema) or not validate_schema_name(table):
        raise ValueError(f"Invalid schema or table name: {schema}.{table}")
    
    # Get initial row count if appending
    initial_count = 0
    if if_exists == "append":
        initial_count = get_table_row_count(engine, schema, table)
    
    try:
        # Get total file size for progress estimation
        file_size = csv_path.stat().st_size
        logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
        
        first = True
        chunk_num = 0
        total_rows = 0
        
        # Use transaction for atomicity - all chunks of a file succeed or fail together
        with engine.begin() as conn:
            for chunk in pd.read_csv(csv_path, chunksize=chunksize):
                chunk_num += 1
                chunk_rows = len(chunk)
                total_rows += chunk_rows
                
                # Minimal, safe normalization: column names only.
                # All cleaning/validation is handled upstream in ingestion.py.
                chunk.columns = [snake_case(str(c)) for c in chunk.columns]
                
                # Validate no empty column names
                if any(not col or col.strip() == '' for col in chunk.columns):
                    raise ValueError(f"CSV {csv_path.name} contains empty column names")
                
                mode = if_exists if first else "append"
                
                # Load chunk to database
                chunk.to_sql(
                    name=table,
                    con=conn,  # Use connection from transaction
                    schema=schema,
                    if_exists=mode,
                    index=False,
                    method="multi",  # Faster bulk insert
                )
                
                first = False
                logger.info(f"  Chunk {chunk_num}: {chunk_rows:,} rows loaded (total: {total_rows:,})")
        
        # Verify final row count
        final_count = get_table_row_count(engine, schema, table)
        rows_added = final_count - initial_count
        
        logger.info(f"✅ Finished {schema}.{table} - {total_rows:,} rows loaded, {final_count:,} total rows in table")
        
        if if_exists == "replace" and final_count != total_rows:
            logger.warning(f"⚠️  Row count mismatch: expected {total_rows:,}, got {final_count:,}")
        elif if_exists == "append" and rows_added != total_rows:
            logger.warning(f"⚠️  Row count mismatch: expected {total_rows:,} added, got {rows_added:,}")
            
    except pd.errors.EmptyDataError:
        logger.warning(f"⚠️  {csv_path.name} is empty, skipping")
    except Exception as e:
        logger.error(f"❌ Failed to load {csv_path.name}: {str(e)}")
        raise



# MAIN

def main() -> None:
    """Main execution function with comprehensive error handling."""
    if IF_EXISTS not in {"replace", "append"}:
        raise ValueError("IF_EXISTS must be 'replace' or 'append'")

    logger.info("=" * 60)
    logger.info("Starting data load to PostgreSQL")
    logger.info("=" * 60)
    
    try:
        engine = build_engine()
        
        # Connectivity check with retry
        logger.info("Testing database connection...")
        for attempt in range(MAX_RETRIES):
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("✅ Database connection successful")
                break
            except SQLAlchemyError as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed, retrying...")
                    continue
                else:
                    logger.error(f"❌ Failed to connect to database after {MAX_RETRIES} attempts")
                    raise
        
        ensure_schema(engine, RAW_SCHEMA)
        
        csv_files = list_csvs(DATA_DIR, CSV_GLOB)
        
        logger.info("Load-to-DB config:")
        logger.info(f"  DATA_DIR    : {DATA_DIR}")
        logger.info(f"  CSV_GLOB    : {CSV_GLOB}")
        logger.info(f"  RAW_SCHEMA  : {RAW_SCHEMA}")
        logger.info(f"  IF_EXISTS   : {IF_EXISTS}")
        logger.info(f"  CHUNKSIZE   : {CHUNKSIZE:,}")
        logger.info(f"  CSV files   : {len(csv_files)}")
        logger.info("-" * 60)
        
        # Process each file with error handling
        successful = 0
        failed = 0
        
        for csv_path in csv_files:
            try:
                table = infer_table_name(csv_path)
                load_csv_to_raw(
                    engine=engine,
                    csv_path=csv_path,
                    schema=RAW_SCHEMA,
                    table=table,
                    if_exists=IF_EXISTS,
                    chunksize=CHUNKSIZE,
                )
                successful += 1
            except Exception as e:
                failed += 1
                logger.error(f"❌ Failed to process {csv_path.name}: {str(e)}")
                # Continue with next file instead of stopping
                continue
        
        logger.info("=" * 60)
        logger.info(f"Load complete: {successful} successful, {failed} failed")
        logger.info("=" * 60)
        
        if failed > 0:
            raise RuntimeError(f"{failed} file(s) failed to load. Check logs above for details.")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        raise
    finally:
        # Clean up engine connections
        if "engine" in locals():
            engine.dispose()
            logger.info("Database connections closed")


def load_to_db() -> None:
    """
    Thin wrapper so pipeline.py can call `load_to_db()`.
    """
    main()


if __name__ == "__main__":
    main()

