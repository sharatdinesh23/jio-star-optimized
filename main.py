"""
JioStar High-Performance Data Ingestion Pipeline
=================================================
Reads data from folders specified in .env, processes data files in batches,
and dumps the data into a PostgreSQL database under the 'jiostar_data' schema.
Each table name corresponds to its source folder name.

Optimized with PostgreSQL native COPY streaming:
Processes 400,000 - 500,000 records in seconds instead of minutes.
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Generator, List, Optional, Set, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("jiostar_loader")


def sanitize_identifier(name: str) -> str:
    """
    Sanitizes folder and column names into safe PostgreSQL identifiers:
    - Strips whitespace and UTF-8 BOM
    - Converts camelCase / PascalCase to snake_case
    - Replaces spaces, hyphens, dots with underscores
    - Lowercases all characters
    """
    if not name:
        return "unnamed"
    name = str(name).strip().lstrip("\ufeff")
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    clean = re.sub(r"[^a-zA-Z0-9_]+", "_", s2).strip("_")
    return clean if clean else "unnamed"


def parse_folder_list(folders_raw: str) -> List[str]:
    """
    Parses folder configuration from .env.
    Supports comma-separated, semicolon-separated, or JSON list formats.
    """
    if not folders_raw:
        return []
    folders_raw = folders_raw.strip()

    # Check if JSON array: e.g. ["Data/folder1", "Data/folder2"]
    if folders_raw.startswith("[") and folders_raw.endswith("]"):
        try:
            parsed = json.loads(folders_raw)
            return [str(p).strip() for p in parsed if str(p).strip()]
        except json.JSONDecodeError:
            pass

    # Split by comma, semicolon, or newline
    items = re.split(r"[,;\n]", folders_raw)
    result = []
    for item in items:
        cleaned = item.strip().strip('"').strip("'")
        if cleaned:
            result.append(cleaned)
    return result


def get_supported_files(folder_path: Path) -> List[Path]:
    """
    Recursively scans folder_path for supported tabular data files.
    Supported extensions: .csv, .tsv, .txt, .parquet, .xlsx, .xls
    Excludes .zip and temporary lock files.
    """
    supported_extensions = {".csv", ".tsv", ".txt", ".parquet", ".xlsx", ".xls"}
    files = []

    for root, _, filenames in os.walk(folder_path):
        for filename in sorted(filenames):
            if filename.startswith(".") or filename.startswith("~$"):
                continue  # Skip hidden/temporary lock files
            file_path = Path(root) / filename
            if file_path.suffix.lower() in supported_extensions:
                files.append(file_path)
            elif file_path.suffix.lower() == ".zip":
                logger.debug("Skipping archive (extract if data is inside): %s", file_path)

    return files


def format_duration(seconds: float) -> str:
    """Formats duration in seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    remaining_sec = seconds % 60
    return f"{minutes}m {remaining_sec:.2f}s"


def psql_insert_copy(table, conn, keys, data_iter):
    """
    High-speed COPY method for pandas to_sql using PostgreSQL's native COPY FROM STDIN.
    Bypasses standard multi-row INSERT statement generation and parameter binding.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data_iter)
        s_buf.seek(0)
        columns = ", ".join([f'"{k}"' for k in keys])
        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, NULL '', QUOTE '\"')"
        cur.copy_expert(sql=sql, file=s_buf)


def ensure_schema_exists(engine: Engine, schema_name: str) -> None:
    """Ensures the PostgreSQL schema exists."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'))
    logger.info("Schema '%s' verified/created.", schema_name)


def create_table_if_not_exists(
    engine: Engine,
    schema_name: str,
    table_name: str,
    columns: List[str],
) -> None:
    """Creates a table in PostgreSQL with TEXT columns if it does not exist."""
    col_defs = ", ".join([f'"{col}" TEXT' for col in columns])
    sql = text(f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" ({col_defs});')
    with engine.begin() as conn:
        conn.execute(sql)


def get_table_columns(engine: Engine, schema_name: str, table_name: str) -> Set[str]:
    """Retrieves existing column names for a table in PostgreSQL."""
    query = text(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :schema AND table_name = :table;
        """
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema_name, "table": table_name})
        return {row[0] for row in result}


def sync_table_columns(
    engine: Engine,
    schema_name: str,
    table_name: str,
    new_columns: List[str],
    known_columns: Set[str],
) -> None:
    """
    Dynamically adds any missing columns to an existing PostgreSQL table.
    Enables smooth ingestion when subsequent files/batches introduce new attributes.
    """
    missing_cols = [c for c in new_columns if c not in known_columns]
    if not missing_cols:
        return

    with engine.begin() as conn:
        for col in missing_cols:
            alter_query = text(
                f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS "{col}" TEXT;'
            )
            conn.execute(alter_query)
            known_columns.add(col)
            logger.info("Table '%s.%s': added new column '%s'.", schema_name, table_name, col)


def test_database_connection(engine: Engine) -> bool:
    """Validates the PostgreSQL connection before initiating batch processing."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        return True
    except Exception as exc:
        logger.error("Failed to connect to PostgreSQL database: %s", exc)
        return False


def copy_csv_in_batches(
    raw_conn,
    schema_name: str,
    table_name: str,
    file_path: Path,
    clean_columns: List[str],
    batch_size: int = 50000,
) -> Tuple[int, int]:
    """
    Ultra-Fast Batch Ingestion:
    Streams batches of raw CSV lines directly into PostgreSQL via COPY FROM STDIN.
    Achieves 50,000 - 100,000+ rows/second without DataFrame overhead.
    Returns (total_rows, total_batches).
    """
    col_clause = ", ".join([f'"{c}"' for c in clean_columns])
    copy_sql = (
        f'COPY "{schema_name}"."{table_name}" ({col_clause}) '
        f"FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"')"
    )

    total_rows = 0
    batch_count = 0

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        # Skip header line in source file
        _ = f.readline()

        batch_lines = []
        with raw_conn.cursor() as cur:
            for line in f:
                batch_lines.append(line)
                if len(batch_lines) >= batch_size:
                    batch_start = time.time()
                    batch_text = "".join(batch_lines)
                    s_buf = io.StringIO(batch_text)
                    cur.copy_expert(copy_sql, s_buf)
                    raw_conn.commit()

                    rows_in_batch = len(batch_lines)
                    total_rows += rows_in_batch
                    batch_count += 1
                    batch_duration = time.time() - batch_start
                    speed = rows_in_batch / batch_duration if batch_duration > 0 else 0
                    logger.info(
                        "      * Batch %d: %d rows committed via COPY in %.2fs (%.0f rows/s)",
                        batch_count,
                        rows_in_batch,
                        batch_duration,
                        speed,
                    )
                    batch_lines = []

            # Flush remaining lines
            if batch_lines:
                batch_start = time.time()
                batch_text = "".join(batch_lines)
                s_buf = io.StringIO(batch_text)
                cur.copy_expert(copy_sql, s_buf)
                raw_conn.commit()

                rows_in_batch = len(batch_lines)
                total_rows += rows_in_batch
                batch_count += 1
                batch_duration = time.time() - batch_start
                speed = rows_in_batch / batch_duration if batch_duration > 0 else 0
                logger.info(
                    "      * Batch %d: %d rows committed via COPY in %.2fs (%.0f rows/s)",
                    batch_count,
                    rows_in_batch,
                    batch_duration,
                    speed,
                )

    return total_rows, batch_count


def read_file_in_batches(
    file_path: Path,
    batch_size: int,
    preserve_raw: bool = True,
) -> Generator[pd.DataFrame, None, None]:
    """
    Fallback DataFrame batch generator for non-CSV files (Parquet, Excel, TSV).
    """
    ext = file_path.suffix.lower()

    if ext in {".csv", ".tsv", ".txt"}:
        delimiter = "\t" if ext == ".tsv" else ","
        chunk_iter = pd.read_csv(
            file_path,
            sep=delimiter,
            chunksize=batch_size,
            dtype=str if preserve_raw else None,
            low_memory=False,
            keep_default_na=True,
            encoding="utf-8",
            on_bad_lines="skip",
        )
        for chunk in chunk_iter:
            chunk = chunk.where(pd.notnull(chunk), None)
            yield chunk

    elif ext == ".parquet":
        try:
            import pyarrow.parquet as pq

            parquet_file = pq.ParquetFile(file_path)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                chunk = batch.to_pandas()
                if preserve_raw:
                    chunk = chunk.astype(str)
                chunk = chunk.where(pd.notnull(chunk), None)
                yield chunk
        except ImportError:
            df = pd.read_parquet(file_path)
            if preserve_raw:
                df = df.astype(str)
            df = df.where(pd.notnull(df), None)
            for i in range(0, len(df), batch_size):
                yield df.iloc[i : i + batch_size]

    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(file_path, dtype=str if preserve_raw else None)
        df = df.where(pd.notnull(df), None)
        for i in range(0, len(df), batch_size):
            yield df.iloc[i : i + batch_size]


def run_dry_run(folder_paths: List[Path], batch_size: int, schema_name: str) -> None:
    """
    Executes a dry-run preview: scans directories, previews tables, columns,
    and batches without connecting to PostgreSQL.
    """
    print("\n" + "=" * 95)
    print(" DRY RUN PREVIEW (No database writes)")
    print("=" * 95)
    print(f"Target Schema: {schema_name}")
    print(f"Batch Size:    {batch_size:,} rows\n")

    total_files_overall = 0
    dry_run_start = time.time()

    for folder_path in folder_paths:
        table_name = sanitize_identifier(folder_path.name)
        print(f"[*] Folder:     {folder_path}")
        print(f"    Table Name: {schema_name}.{table_name}")

        if not folder_path.exists():
            print(f"    [WARNING] Directory does not exist: {folder_path}\n")
            continue

        files = get_supported_files(folder_path)
        total_files_overall += len(files)
        print(f"    Files Found: {len(files)}")

        sample_cols = []
        folder_dry_start = time.time()

        for idx, f in enumerate(files, start=1):
            file_sample_start = time.time()
            rows_sampled = 0
            try:
                # Fast sample from first line
                with open(f, "r", encoding="utf-8", errors="ignore") as fp:
                    header = fp.readline().strip()
                    sample_cols = [sanitize_identifier(c) for c in next(csv.reader([header]))]
                    # Count preview lines up to batch_size
                    for _ in range(min(batch_size, 5000)):
                        if not fp.readline():
                            break
                        rows_sampled += 1

                sample_duration = time.time() - file_sample_start
                print(
                    f"    - [{idx}/{len(files)}] {f.name:<45} (Sampled {rows_sampled:,} rows in {format_duration(sample_duration)})"
                )
            except Exception as err:
                print(f"    - [{idx}/{len(files)}] {f.name:<45} (Error: {err})")

        folder_elapsed = time.time() - folder_dry_start
        print(f"\n    Sample Columns ({len(sample_cols)}): {sample_cols[:8]}{'...' if len(sample_cols) > 8 else ''}")
        print(f"    Folder scan completed in: {format_duration(folder_elapsed)}\n")

    total_elapsed = time.time() - dry_run_start
    print(f"Total Folders: {len(folder_paths)}")
    print(f"Total Files to Process: {total_files_overall}")
    print(f"Total Dry-Run Time: {format_duration(total_elapsed)}")
    print("=" * 95 + "\n")


def import_folders_to_postgres():
    """Main pipeline to orchestrate reading from folders and dumping to PostgreSQL."""
    # 1. Parse Command Line Arguments
    parser = argparse.ArgumentParser(description="Batch import data folders into PostgreSQL.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect folders, validate files, and preview batches without writing to DB.",
    )
    args = parser.parse_args()

    # 2. Load .env Configuration
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        logger.info("Loaded configuration from %s", env_path.resolve())
    else:
        logger.warning(".env file not found at %s. Relying on system environment variables.", env_path.resolve())

    # 3. Read Database Configurations
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    schema_name = os.getenv("DB_SCHEMA", "jiostar_data")

    # Ingestion Parameters
    raw_folders = os.getenv("DATA_FOLDERS", "")
    # Optimal default batch size: 50,000 rows
    batch_size = int(os.getenv("BATCH_SIZE", "50000"))
    table_if_exists = os.getenv("TABLE_IF_EXISTS", "append").lower()
    preserve_raw = os.getenv("PRESERVE_RAW_STRINGS", "true").lower() in ("true", "1", "yes")

    # Parse and validate folder paths
    folder_strings = parse_folder_list(raw_folders)
    if not folder_strings:
        logger.error("No folders specified in DATA_FOLDERS within .env. Please configure DATA_FOLDERS.")
        sys.exit(1)

    folder_paths = [Path(p.strip()) for p in folder_strings]

    # Handle dry run
    if args.dry_run:
        run_dry_run(folder_paths, batch_size, schema_name)
        return

    # 4. Initialize Database Engine
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    logger.info("Connecting to PostgreSQL at %s:%s / %s...", db_host, db_port, db_name)

    engine = create_engine(db_url, pool_pre_ping=True)

    if not test_database_connection(engine):
        logger.error(
            "Could not connect to PostgreSQL. Please check credentials in .env and verify the database is running."
        )
        logger.error(
            "Tip: You can preview folder contents and batches without a database by running: python main.py --dry-run"
        )
        sys.exit(1)

    # 5. Create Schema
    ensure_schema_exists(engine, schema_name)

    # 6. Process Folders
    start_time = time.time()
    summary_results = []
    file_upload_stats = []

    # Obtain a dedicated raw connection for ultra-fast COPY streaming
    raw_conn = engine.raw_connection()

    try:
        for folder_path in folder_paths:
            folder_start_time = time.time()
            raw_folder_name = folder_path.name
            table_name = sanitize_identifier(raw_folder_name)

            logger.info("=" * 70)
            logger.info("Processing Folder: '%s' -> Table: '%s.%s'", folder_path, schema_name, table_name)
            logger.info("=" * 70)

            if not folder_path.exists():
                logger.warning("Folder does not exist: %s. Skipping.", folder_path)
                summary_results.append({
                    "folder": str(folder_path),
                    "table": table_name,
                    "status": "FOLDER_NOT_FOUND",
                    "files": 0,
                    "rows": 0,
                    "duration": "0.00s",
                })
                continue

            files = get_supported_files(folder_path)
            if not files:
                logger.warning("No supported tabular data files found in %s.", folder_path)
                summary_results.append({
                    "folder": str(folder_path),
                    "table": table_name,
                    "status": "NO_FILES_FOUND",
                    "files": 0,
                    "rows": 0,
                    "duration": "0.00s",
                })
                continue

            logger.info("Found %d data file(s) in '%s'.", len(files), folder_path)

            inspector = inspect(engine)
            table_exists = inspector.has_table(table_name, schema=schema_name)

            if table_exists and table_if_exists == "replace":
                with engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;'))
                table_exists = False
                logger.info("Dropped existing table '%s.%s' (TABLE_IF_EXISTS=replace).", schema_name, table_name)

            known_columns: Set[str] = set()
            if table_exists:
                known_columns = get_table_columns(engine, schema_name, table_name)

            total_folder_rows = 0
            total_folder_batches = 0

            for file_idx, file_path in enumerate(files, start=1):
                file_start_time = time.time()
                logger.info("[%d/%d] Starting upload: %s", file_idx, len(files), file_path.name)

                file_rows = 0
                file_batches = 0
                is_csv = file_path.suffix.lower() in {".csv", ".txt"}

                try:
                    if is_csv:
                        # Extract column header from CSV
                        with open(file_path, "r", encoding="utf-8", errors="replace") as fp:
                            header_line = fp.readline().strip()
                            raw_cols = next(csv.reader([header_line]))
                            clean_cols = [sanitize_identifier(c) for c in raw_cols]

                        # Create table if it doesn't exist yet
                        if not table_exists:
                            create_table_if_not_exists(engine, schema_name, table_name, clean_cols)
                            table_exists = True
                            known_columns = set(clean_cols)
                        else:
                            sync_table_columns(engine, schema_name, table_name, clean_cols, known_columns)

                        # Ultra-Fast Native COPY Streaming in Batches
                        file_rows, file_batches = copy_csv_in_batches(
                            raw_conn=raw_conn,
                            schema_name=schema_name,
                            table_name=table_name,
                            file_path=file_path,
                            clean_columns=clean_cols,
                            batch_size=batch_size,
                        )

                    else:
                        # Fallback for non-CSV files (Parquet, Excel, TSV) using psql_insert_copy
                        batch_generator = read_file_in_batches(
                            file_path=file_path,
                            batch_size=batch_size,
                            preserve_raw=preserve_raw,
                        )

                        for batch_df in batch_generator:
                            if batch_df.empty:
                                continue

                            batch_df.columns = [sanitize_identifier(c) for c in batch_df.columns]
                            clean_cols = list(batch_df.columns)

                            if not table_exists:
                                batch_df.to_sql(
                                    name=table_name,
                                    con=engine,
                                    schema=schema_name,
                                    if_exists="append",
                                    index=False,
                                    chunksize=batch_size,
                                    method=psql_insert_copy,
                                )
                                table_exists = True
                                known_columns = get_table_columns(engine, schema_name, table_name)
                            else:
                                sync_table_columns(engine, schema_name, table_name, clean_cols, known_columns)
                                batch_df.to_sql(
                                    name=table_name,
                                    con=engine,
                                    schema=schema_name,
                                    if_exists="append",
                                    index=False,
                                    chunksize=batch_size,
                                    method=psql_insert_copy,
                                )

                            b_rows = len(batch_df)
                            file_rows += b_rows
                            file_batches += 1

                    total_folder_rows += file_rows
                    total_folder_batches += file_batches

                    file_duration = time.time() - file_start_time
                    throughput = (file_rows / file_duration) if file_duration > 0 else 0
                    logger.info(
                        "    -> Completed '%s' in %s (%s rows, %d batches, %.0f rows/s).",
                        file_path.name,
                        format_duration(file_duration),
                        f"{file_rows:,}",
                        file_batches,
                        throughput,
                    )

                    file_upload_stats.append({
                        "file_name": file_path.name,
                        "table": f"{schema_name}.{table_name}",
                        "rows": f"{file_rows:,}",
                        "batches": file_batches,
                        "duration_sec": file_duration,
                        "duration_str": format_duration(file_duration),
                        "speed": f"{throughput:,.0f} rows/s",
                        "status": "SUCCESS",
                    })

                except Exception as file_err:
                    file_duration = time.time() - file_start_time
                    logger.error(
                        "Error processing file %s after %s: %s",
                        file_path.name,
                        format_duration(file_duration),
                        file_err,
                        exc_info=True,
                    )
                    file_upload_stats.append({
                        "file_name": file_path.name,
                        "table": f"{schema_name}.{table_name}",
                        "rows": f"{file_rows:,}",
                        "batches": file_batches,
                        "duration_sec": file_duration,
                        "duration_str": format_duration(file_duration),
                        "speed": "0 rows/s",
                        "status": "FAILED",
                    })

            elapsed = time.time() - folder_start_time
            summary_results.append({
                "folder": str(folder_path),
                "table": f"{schema_name}.{table_name}",
                "status": "SUCCESS",
                "files": len(files),
                "rows": f"{total_folder_rows:,}",
                "duration": format_duration(elapsed),
            })

    finally:
        raw_conn.close()

    # 7. Print Final Execution Summary Tables
    total_elapsed = time.time() - start_time

    # Per-File Upload Performance Report
    if file_upload_stats:
        print("\n" + "=" * 110)
        print(" PER-FILE UPLOAD PERFORMANCE (TIME TAKEN PER FILE)")
        print("=" * 110)
        print(
            f"{'File Name':<35} {'Target Table':<32} {'Rows':<12} {'Batches':<9} {'Time Taken':<12} {'Speed':<14}"
        )
        print("-" * 110)
        for stat in file_upload_stats:
            print(
                f"{stat['file_name'][:33]:<35} {stat['table'][:30]:<32} {stat['rows']:<12} "
                f"{stat['batches']:<9} {stat['duration_str']:<12} {stat['speed']:<14}"
            )
        print("=" * 110)

    # Folder-Level Aggregate Report
    print("\n" + "=" * 100)
    print(" FOLDER-LEVEL INGESTION SUMMARY")
    print("=" * 100)
    print(f"{'Folder':<40} {'Table Name':<30} {'Files':<8} {'Rows':<12} {'Duration':<12}")
    print("-" * 100)
    for res in summary_results:
        print(
            f"{res['folder'][:38]:<40} {res['table'][:28]:<30} {res['files']:<8} "
            f"{res['rows']:<12} {res['duration']:<12}"
        )
    print("=" * 100)
    print(f"Total Pipeline Duration: {format_duration(total_elapsed)}\n")


def main():
    import_folders_to_postgres()


if __name__ == "__main__":
    main()
