import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, TextIO, Tuple

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

DEFAULT_CSV_FIELD_SIZE_LIMIT = 64 * 1024 * 1024

SUPPORTED_ENTITIES = ["india", "uk", "singapore", "canada"]

ENTITY_PATTERNS = {
    "india": [r"(?:^|[\W_])(?:india|indian|in)(?:[\W_]|$)", r"bot_in_"],
    "uk": [r"(?:^|[\W_])(?:uk|united[-_ ]?kingdom|great[-_ ]?britain|gb)(?:[\W_]|$)", r"bot_uk_"],
    "singapore": [r"(?:^|[\W_])(?:singapore|sg)(?:[\W_]|$)", r"bot_sg_"],
    "canada": [r"(?:^|[\W_])(?:canada|ca)(?:[\W_]|$)", r"bot_ca_"],
}


def configure_logging() -> Path:
    """Creates colorized console logging and a fresh file sink for this run."""
    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_path = logs_dir / f"jiostar_{run_id}_{os.getpid()}.log"
    log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}"

    logger.remove()
    logger.add(
        sys.stdout,
        format=f"<green>{{time:YYYY-MM-DD HH:mm:ss.SSS}}</green> | "
        f"<level>{{level: <8}}</level> | <level>{{message}}</level>",
        level="INFO",
        colorize=True,
        backtrace=True,
        diagnose=False,
    )
    logger.add(
        str(log_path),
        format=log_format,
        level="DEBUG",
        colorize=False,
        encoding="utf-8",
        backtrace=True,
        diagnose=False,
    )
    logger.info("Run log: {}", log_path)
    return log_path


def get_csv_field_size_limit() -> int:
    """Reads and validates the configured CSV field-size limit in characters."""
    raw_limit = os.getenv("CSV_FIELD_SIZE_LIMIT", str(DEFAULT_CSV_FIELD_SIZE_LIMIT))
    try:
        field_size_limit = int(raw_limit)
    except ValueError as exc:
        raise ValueError("CSV_FIELD_SIZE_LIMIT must be a positive integer.") from exc
    if field_size_limit <= 0:
        raise ValueError("CSV_FIELD_SIZE_LIMIT must be a positive integer.")
    return field_size_limit


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


def detect_entity_from_path(path_str: str) -> Optional[str]:
    """Detects which of the 4 entities a folder path or link belongs to."""
    normalized = path_str.lower().strip()
    for entity, patterns in ENTITY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, normalized):
                return entity
    return None


def clean_path_string(path_str: str) -> str:
    """Normalizes URL-like paths (e.g. file://) and strips quotes."""
    p = path_str.strip().strip('"').strip("'")
    if p.startswith("file:///"):
        p = p[8:]
    elif p.startswith("file://"):
        p = p[7:]
    return p


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


def resolve_entity_folders() -> Dict[str, List[Path]]:
    """
    Resolves data folders/links for the 4 entities: India, UK, Singapore, Canada.
    Sources checked:
    1. Direct entity env vars: DATA_FOLDER_<COUNTRY> or DATA_LINK_<COUNTRY>
    2. Combined list / mapping in DATA_FOLDERS or DATA_LINKS:
       - JSON dict: {"india": "path/1", ...}
       - Key-value pairs: india=path1, uk=path2
       - Comma-separated list with auto-detection matching
    """
    entity_folders: Dict[str, List[Path]] = {entity: [] for entity in SUPPORTED_ENTITIES}

    # 1. Direct env vars per entity
    for entity in SUPPORTED_ENTITIES:
        env_keys = [
            f"DATA_FOLDER_{entity.upper()}",
            f"DATA_LINK_{entity.upper()}",
            f"FOLDER_{entity.upper()}",
            f"LINK_{entity.upper()}",
        ]
        for key in env_keys:
            val = os.getenv(key, "").strip()
            if val:
                for sub_p in parse_folder_list(val):
                    c = clean_path_string(sub_p)
                    if c:
                        p_obj = Path(c)
                        if p_obj not in entity_folders[entity]:
                            entity_folders[entity].append(p_obj)

    # 2. Combined DATA_FOLDERS or DATA_LINKS
    combined_raw = os.getenv("DATA_FOLDERS", "") or os.getenv("DATA_LINKS", "")
    if combined_raw.strip():
        raw_trimmed = combined_raw.strip()
        if raw_trimmed.startswith("{") and raw_trimmed.endswith("}"):
            try:
                data_dict = json.loads(raw_trimmed)
                for k, v in data_dict.items():
                    ent = detect_entity_from_path(k) or (k.lower() if k.lower() in SUPPORTED_ENTITIES else None)
                    if ent and ent in entity_folders:
                        val_paths = v if isinstance(v, list) else [v]
                        for vp in val_paths:
                            c = clean_path_string(str(vp))
                            if c:
                                p_obj = Path(c)
                                if p_obj not in entity_folders[ent]:
                                    entity_folders[ent].append(p_obj)
            except json.JSONDecodeError:
                pass
        else:
            items = parse_folder_list(raw_trimmed)
            for item in items:
                # Check for key=value or key:value
                if "=" in item or (":" in item and not (len(item) > 2 and item[1] == ":")):
                    sep = "=" if "=" in item else ":"
                    k, v = item.split(sep, 1)
                    ent = detect_entity_from_path(k) or (k.lower() if k.lower() in SUPPORTED_ENTITIES else None)
                    c = clean_path_string(v)
                    if ent and c:
                        p_obj = Path(c)
                        if p_obj not in entity_folders[ent]:
                            entity_folders[ent].append(p_obj)
                else:
                    c = clean_path_string(item)
                    if c:
                        ent = detect_entity_from_path(c)
                        if ent:
                            p_obj = Path(c)
                            if p_obj not in entity_folders[ent]:
                                entity_folders[ent].append(p_obj)
                        else:
                            logger.warning(
                                "Could not map folder/link '{}' to any entity (India, UK, Singapore, Canada).", c
                            )

    return entity_folders


def get_supported_files(folder_path: Path) -> List[Path]:
    """
    Recursively scans folder_path for supported tabular data files.
    Supported extensions: .csv, .tsv, .txt, .parquet, .xlsx, .xls
    Excludes .zip and temporary lock files.
    """
    supported_extensions = {".csv", ".tsv", ".txt", ".parquet", ".xlsx", ".xls"}
    files = []

    if folder_path.is_file() and folder_path.suffix.lower() in supported_extensions:
        return [folder_path]

    for root, _, filenames in os.walk(folder_path):
        for filename in sorted(filenames):
            if filename.startswith(".") or filename.startswith("~$"):
                continue  # Skip hidden/temporary lock files
            file_path = Path(root) / filename
            if file_path.suffix.lower() in supported_extensions:
                files.append(file_path)
            elif file_path.suffix.lower() == ".zip":
                logger.debug("Skipping archive (extract if data is inside): {}", file_path)

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
    logger.info("Schema '{}' verified/created.", schema_name)


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
            logger.info("Table '{}.{}': added new column '{}'.", schema_name, table_name, col)


def test_database_connection(engine: Engine) -> bool:
    """Validates the PostgreSQL connection before initiating batch processing."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        return True
    except Exception as exc:
        logger.exception("Failed to connect to PostgreSQL database: {}", exc)
        return False


def iter_limited_csv_lines(
    source: TextIO,
    file_path: Path,
    field_size_limit: int,
    nul_characters_removed: List[int],
) -> Generator[str, None, None]:
    """Removes NULs and bounds physical line size before CSV parsing."""
    line_number = 0
    pending_line: List[str] = []
    pending_length = 0
    chunk_size = min(64 * 1024, field_size_limit + 1)

    while True:
        chunk = source.readline(chunk_size)
        if not chunk:
            if pending_line:
                yield "".join(pending_line)
            return

        nul_characters_removed[0] += chunk.count("\x00")
        chunk = chunk.replace("\x00", "")
        pending_line.append(chunk)
        pending_length += len(chunk)

        if pending_length > field_size_limit:
            raise csv.Error(
                f"CSV field or physical line exceeded limit of {field_size_limit} characters "
                f"in {file_path} at line {line_number + 1}."
            )

        if chunk.endswith("\r") or chunk.endswith("\n"):
            line_number += 1
            full_line = "".join(pending_line)
            pending_line = []
            pending_length = 0
            yield full_line


def build_copy_csv_buffer(
    rows: List[List[str]],
    file_path: Path,
    expected_column_count: int,
    line_offset: int,
) -> io.StringIO:
    """Builds an RFC-compliant CSV buffer for PostgreSQL COPY."""
    s_buf = io.StringIO()
    writer = csv.writer(
        s_buf,
        delimiter=",",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    for row_idx, row in enumerate(rows, start=line_offset):
        if len(row) != expected_column_count:
            raise ValueError(
                f"Malformed row in {file_path} near line {row_idx}: "
                f"expected {expected_column_count} fields, found {len(row)}."
            )
        writer.writerow(row)
    s_buf.seek(0)
    return s_buf


def copy_csv_in_batches(
    raw_conn,
    schema_name: str,
    table_name: str,
    file_path: Path,
    clean_columns: List[str],
    batch_size: int = 100000,
    field_size_limit: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Ultra-Fast Batch Ingestion:
    Parses and sanitizes CSV rows into RFC-compliant COPY buffers.
    Achieves 50,000 - 100,000+ rows/second without DataFrame overhead.
    Returns (total_rows, total_batches).
    """
    if field_size_limit is None:
        field_size_limit = get_csv_field_size_limit()

    previous_limit = csv.field_size_limit()
    csv.field_size_limit(field_size_limit)

    col_clause = ", ".join([f'"{c}"' for c in clean_columns])
    copy_sql = (
        f'COPY "{schema_name}"."{table_name}" ({col_clause}) '
        f"FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"')"
    )

    total_rows = 0
    batch_count = 0
    nul_tracker = [0]

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as fp:
            line_stream = iter_limited_csv_lines(
                source=fp,
                file_path=file_path,
                field_size_limit=field_size_limit,
                nul_characters_removed=nul_tracker,
            )
            reader = csv.reader(line_stream)
            try:
                _ = next(reader)  # Skip header
            except StopIteration:
                return 0, 0

            batch_rows: List[List[str]] = []
            line_offset = 2

            with raw_conn.cursor() as cur:
                for row in reader:
                    batch_rows.append(row)
                    if len(batch_rows) >= batch_size:
                        batch_start = time.time()
                        s_buf = build_copy_csv_buffer(
                            rows=batch_rows,
                            file_path=file_path,
                            expected_column_count=len(clean_columns),
                            line_offset=line_offset,
                        )
                        cur.copy_expert(copy_sql, s_buf)
                        raw_conn.commit()

                        rows_in_batch = len(batch_rows)
                        total_rows += rows_in_batch
                        batch_count += 1
                        batch_duration = time.time() - batch_start
                        speed = rows_in_batch / batch_duration if batch_duration > 0 else 0
                        logger.info(
                            "      * Batch {}: {:,} rows committed via COPY in {:.2f}s ({:,.0f} rows/s)",
                            batch_count,
                            rows_in_batch,
                            batch_duration,
                            speed,
                        )
                        line_offset += rows_in_batch
                        batch_rows = []

                if batch_rows:
                    batch_start = time.time()
                    s_buf = build_copy_csv_buffer(
                        rows=batch_rows,
                        file_path=file_path,
                        expected_column_count=len(clean_columns),
                        line_offset=line_offset,
                    )
                    cur.copy_expert(copy_sql, s_buf)
                    raw_conn.commit()

                    rows_in_batch = len(batch_rows)
                    total_rows += rows_in_batch
                    batch_count += 1
                    batch_duration = time.time() - batch_start
                    speed = rows_in_batch / batch_duration if batch_duration > 0 else 0
                    logger.info(
                        "      * Batch {}: {:,} rows committed via COPY in {:.2f}s ({:,.0f} rows/s)",
                        batch_count,
                        rows_in_batch,
                        batch_duration,
                        speed,
                    )

        if nul_tracker[0] > 0:
            logger.warning(
                "Sanitized {} NUL (0x00) byte(s) while streaming {}.",
                nul_tracker[0],
                file_path.name,
            )

    finally:
        csv.field_size_limit(previous_limit)

    return total_rows, batch_count


def read_file_in_batches(
    file_path: Path,
    batch_size: int,
    preserve_raw: bool = True,
) -> Generator[pd.DataFrame, None, None]:
    """Fallback DataFrame batch generator for non-CSV files (Parquet, Excel, TSV)."""
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


def run_dry_run(entity_folders: Dict[str, List[Path]], batch_size: int, schema_name: str) -> None:
    """
    Executes a dry-run preview: displays the entity mapping, scans directories,
    previews tables, columns, and batches without connecting to PostgreSQL.
    """
    print("\n" + "=" * 105)
    print(" DRY RUN PREVIEW: ENTITY MAPPING & INGESTION PLAN")
    print("=" * 105)
    print(f"Target Schema: {schema_name}")
    print(f"Batch Size:    {batch_size:,} rows\n")

    total_files_overall = 0
    dry_run_start = time.time()

    for idx, entity in enumerate(SUPPORTED_ENTITIES, start=1):
        country_display = entity.title() if entity != "uk" else "UK"
        table_name = f"internal_{entity}_data"
        full_table = f"{schema_name}.{table_name}"
        folders = entity_folders.get(entity, [])

        print(f"[{idx}/4] Entity: {country_display}")
        print(f"      Target Table: {full_table}")

        if not folders:
            print("      Folder(s):    None configured (Skipped)\n")
            continue

        folder_str = ", ".join(str(f) for f in folders)
        print(f"      Folder(s):    {folder_str}")

        files: List[Path] = []
        for folder_path in folders:
            if not folder_path.exists():
                print(f"      [WARNING] Path does not exist: {folder_path}")
                continue
            files.extend(get_supported_files(folder_path))

        total_files_overall += len(files)
        print(f"      Files Found:  {len(files)}")

        if not files:
            print("      Status:       No data files found.\n")
            continue

        sample_cols: List[str] = []
        entity_start = time.time()

        for f_idx, f in enumerate(files, start=1):
            file_start = time.time()
            rows_sampled = 0
            try:
                with open(f, "r", encoding="utf-8", errors="ignore") as fp:
                    header = fp.readline().strip()
                    sample_cols = [sanitize_identifier(c) for c in next(csv.reader([header]))]
                    for _ in range(min(batch_size, 5000)):
                        if not fp.readline():
                            break
                        rows_sampled += 1

                sample_duration = time.time() - file_start
                print(
                    f"      - [{f_idx}/{len(files)}] {f.name:<45} (Sampled {rows_sampled:,} rows in {format_duration(sample_duration)})"
                )
            except Exception as err:
                print(f"      - [{f_idx}/{len(files)}] {f.name:<45} (Error: {err})")

        entity_elapsed = time.time() - entity_start
        print(f"\n      Sample Columns ({len(sample_cols)}): {sample_cols[:8]}{'...' if len(sample_cols) > 8 else ''}")
        print(f"      Entity scan completed in: {format_duration(entity_elapsed)}\n")

    total_elapsed = time.time() - dry_run_start
    print("=" * 105)
    print(f"Total Files to Process: {total_files_overall}")
    print(f"Total Dry-Run Time:     {format_duration(total_elapsed)}")
    print("=" * 105 + "\n")


def prompt_terminal_entity_inputs(
    env_entity_folders: Dict[str, List[Path]],
) -> Dict[str, List[Path]]:
    """
    Prompts the user in the terminal to input data folder links for the entities.
    Allows single entity input, all 4 entities input, or accepting .env defaults.
    """
    print("\n" + "=" * 80)
    print(" JIOSTAR DATA INGESTION - TERMINAL INPUT")
    print("=" * 80)
    print("How would you like to provide the entity folder links?")
    print("  [1] Enter link for a single entity (India / UK / Singapore / Canada)")
    print("  [2] Enter links for all 4 entities (prompted one by one)")
    print("  [3] Use saved links from .env directly")

    try:
        choice = input("\nEnter choice [1/2/3] (default: 2): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nUsing defaults from .env.")
        return env_entity_folders

    result: Dict[str, List[Path]] = {e: [] for e in SUPPORTED_ENTITIES}

    if choice == "1":
        print("\nSelect Entity:")
        print("  1. India      -> jiostar_data.internal_india_data")
        print("  2. UK         -> jiostar_data.internal_uk_data")
        print("  3. Singapore  -> jiostar_data.internal_singapore_data")
        print("  4. Canada     -> jiostar_data.internal_canada_data")
        try:
            e_choice = input("Enter number [1-4] or entity name: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return env_entity_folders

        mapping = {"1": "india", "2": "uk", "3": "singapore", "4": "canada"}
        selected = mapping.get(e_choice, e_choice)
        if selected not in SUPPORTED_ENTITIES:
            print(f"Invalid entity '{e_choice}'. Defaulting to all.")
            selected = None
        else:
            defaults = env_entity_folders.get(selected, [])
            default_val = ", ".join(str(p) for p in defaults) if defaults else "None"
            prompt_str = f"Enter folder path or link for {selected.upper()}"
            if default_val != "None":
                prompt_str += f" [default: {default_val}]"
            else:
                prompt_str += " [default: Skip]"
            try:
                user_val = input(f"{prompt_str}: ").strip()
            except (EOFError, KeyboardInterrupt):
                user_val = ""

            final_val = user_val if user_val else (default_val if default_val != "None" else "")
            if final_val and final_val != "Skip":
                for sub_p in parse_folder_list(final_val):
                    c = clean_path_string(sub_p)
                    if c:
                        result[selected].append(Path(c))
            return result

    if choice == "3":
        return env_entity_folders

    # Choice 2 (default): prompt all 4 entities
    print("\nEnter folder path or link for each entity (press Enter to accept default or skip):")
    for entity in SUPPORTED_ENTITIES:
        country_display = entity.title() if entity != "uk" else "UK"
        defaults = env_entity_folders.get(entity, [])
        default_val = ", ".join(str(p) for p in defaults) if defaults else "Skip"
        prompt_str = f"  * {country_display:<10} (Table: internal_{entity}_data) [default: {default_val}]"
        try:
            user_val = input(f"{prompt_str}: ").strip()
        except (EOFError, KeyboardInterrupt):
            user_val = ""

        final_val = user_val if user_val else (default_val if default_val != "Skip" else "")
        if final_val and final_val != "Skip":
            for sub_p in parse_folder_list(final_val):
                c = clean_path_string(sub_p)
                if c:
                    result[entity].append(Path(c))

    return result


def import_folders_to_postgres():
    """Main pipeline to orchestrate mapping entities and batch dumping to PostgreSQL."""
    configure_logging()

    # 1. Parse Command Line Arguments
    parser = argparse.ArgumentParser(description="Batch import data folders into PostgreSQL mapped by entity.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect entity mappings, validate files, and preview batches without writing to DB.",
    )
    parser.add_argument(
        "--entity",
        choices=["india", "uk", "singapore", "canada", "all"],
        help="Specify an entity to import directly from terminal.",
    )
    parser.add_argument(
        "--link",
        "--folder",
        dest="link",
        help="Specify the folder path or link for the entity directly.",
    )
    parser.add_argument(
        "--india",
        help="Directly specify the folder path or link for India.",
    )
    parser.add_argument(
        "--uk",
        help="Directly specify the folder path or link for UK.",
    )
    parser.add_argument(
        "--singapore",
        help="Directly specify the folder path or link for Singapore.",
    )
    parser.add_argument(
        "--canada",
        help="Directly specify the folder path or link for Canada.",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        default=None,
        help="Prompt interactively in the terminal for entity folder links.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Do not prompt in terminal; use values from .env or CLI flags.",
    )
    args = parser.parse_args()

    # 2. Load .env Configuration
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        logger.info("Loaded configuration from {}", env_path.resolve())
    else:
        logger.warning(".env file not found at {}. Relying on system environment variables.", env_path.resolve())

    # 3. Read Database Configurations
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    schema_name = os.getenv("DB_SCHEMA", "jiostar_data")

    # Ingestion Parameters
    batch_size = int(os.getenv("BATCH_SIZE", "100000"))
    table_if_exists = os.getenv("TABLE_IF_EXISTS", "append").lower()
    preserve_raw = os.getenv("PRESERVE_RAW_STRINGS", "true").lower() in ("true", "1", "yes")

    # 4. Resolve Entity Data Folders / Links for the 4 Entities
    env_entity_folders = resolve_entity_folders()

    # Check CLI overrides
    cli_specified = False
    entity_folders: Dict[str, List[Path]] = {e: [] for e in SUPPORTED_ENTITIES}

    for entity in SUPPORTED_ENTITIES:
        val = getattr(args, entity, None)
        if val:
            cli_specified = True
            for sub_p in parse_folder_list(val):
                c = clean_path_string(sub_p)
                if c:
                    entity_folders[entity].append(Path(c))

    if args.entity and args.link:
        cli_specified = True
        target_entities = SUPPORTED_ENTITIES if args.entity == "all" else [args.entity]
        for ent in target_entities:
            for sub_p in parse_folder_list(args.link):
                c = clean_path_string(sub_p)
                if c:
                    entity_folders[ent].append(Path(c))

    if cli_specified:
        logger.info("Using entity links provided via command-line arguments.")
    elif args.non_interactive or (not args.interactive and not sys.stdin.isatty()):
        entity_folders = env_entity_folders
    else:
        # Prompt user in terminal
        entity_folders = prompt_terminal_entity_inputs(env_entity_folders)

    total_configured_folders = sum(len(f_list) for f_list in entity_folders.values())
    if total_configured_folders == 0:
        logger.error(
            "No data folders/links mapped for any entity (India, UK, Singapore, Canada). "
            "Please configure in terminal prompt, CLI flags, or DATA_FOLDER_* in .env."
        )
        sys.exit(1)

    # Handle dry run
    if args.dry_run:
        run_dry_run(entity_folders, batch_size, schema_name)
        return

    # 5. Initialize Database Engine
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    logger.info("Connecting to PostgreSQL at {}:{} / {}...", db_host, db_port, db_name)

    engine = create_engine(db_url, pool_pre_ping=True)

    if not test_database_connection(engine):
        logger.error(
            "Could not connect to PostgreSQL. Please check credentials in .env and verify the database is running."
        )
        logger.error(
            "Tip: You can preview entity mappings and batches without a database by running: python main.py --dry-run"
        )
        sys.exit(1)

    # 6. Create Target Schema (jiostar_data)
    ensure_schema_exists(engine, schema_name)

    # 7. Process Entities (India, UK, Singapore, Canada)
    start_time = time.time()
    summary_results = []
    file_upload_stats = []

    raw_conn = engine.raw_connection()

    try:
        for entity in SUPPORTED_ENTITIES:
            country_display = entity.title() if entity != "uk" else "UK"
            table_name = f"internal_{entity}_data"
            full_table = f"{schema_name}.{table_name}"
            folders = entity_folders.get(entity, [])

            if not folders:
                logger.info("Entity '{}': No folder or link configured. Skipping.", country_display)
                continue

            entity_start_time = time.time()
            logger.info("=" * 80)
            logger.info("Processing Entity: '{}' -> Table: '{}'", country_display, full_table)
            logger.info("=" * 80)

            # Discover all files for this entity
            files: List[Path] = []
            for folder_path in folders:
                if not folder_path.exists():
                    logger.warning("Folder does not exist for entity '{}': {}", country_display, folder_path)
                    continue
                folder_files = get_supported_files(folder_path)
                logger.info("  Mapped folder '{}': found {} file(s).", folder_path, len(folder_files))
                files.extend(folder_files)

            if not files:
                logger.warning("No supported tabular data files found for entity '{}'. Skipping.", country_display)
                summary_results.append({
                    "entity": country_display,
                    "table": full_table,
                    "folders": ", ".join(str(f) for f in folders),
                    "status": "NO_FILES_FOUND",
                    "files": 0,
                    "rows": "0",
                    "duration": "0.00s",
                })
                continue

            # Check if table exists
            inspector = inspect(engine)
            table_exists = inspector.has_table(table_name, schema=schema_name)

            if table_exists and table_if_exists == "replace":
                with engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;'))
                table_exists = False
                logger.info("Dropped existing table '{}' (TABLE_IF_EXISTS=replace).", full_table)

            known_columns: Set[str] = set()
            if table_exists:
                known_columns = get_table_columns(engine, schema_name, table_name)

            total_entity_rows = 0
            total_entity_batches = 0

            # Ingest all files into schema_name.internal_<country_name>_data
            for file_idx, file_path in enumerate(files, start=1):
                file_start_time = time.time()
                logger.info("[{}/{}] Starting upload: {}", file_idx, len(files), file_path.name)

                file_rows = 0
                file_batches = 0
                is_csv = file_path.suffix.lower() in {".csv", ".txt"}

                try:
                    if is_csv:
                        # Extract column header
                        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as fp:
                            raw_cols = next(csv.reader(fp), [])
                            clean_cols = [sanitize_identifier(c) for c in raw_cols]

                        # Create table if it doesn't exist yet
                        if not table_exists:
                            create_table_if_not_exists(engine, schema_name, table_name, clean_cols)
                            table_exists = True
                            known_columns = set(clean_cols)
                            logger.info("Created table '{}' with {} columns.", full_table, len(clean_cols))
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
                                logger.info("Created table '{}' with {} columns.", full_table, len(clean_cols))
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

                    total_entity_rows += file_rows
                    total_entity_batches += file_batches

                    file_duration = time.time() - file_start_time
                    throughput = (file_rows / file_duration) if file_duration > 0 else 0
                    logger.info(
                        "    -> Completed '{}' in {} ({:,} rows, {} batches, {:.0f} rows/s).",
                        file_path.name,
                        format_duration(file_duration),
                        file_rows,
                        file_batches,
                        throughput,
                    )

                    file_upload_stats.append({
                        "file_name": file_path.name,
                        "entity": country_display,
                        "table": full_table,
                        "rows": f"{file_rows:,}",
                        "batches": file_batches,
                        "duration_sec": file_duration,
                        "duration_str": format_duration(file_duration),
                        "speed": f"{throughput:,.0f} rows/s",
                        "status": "SUCCESS",
                    })

                except Exception as file_err:
                    raw_conn.rollback()
                    file_duration = time.time() - file_start_time
                    logger.exception(
                        "Error processing file {} after {}: {}",
                        file_path.name,
                        format_duration(file_duration),
                        file_err,
                    )
                    file_upload_stats.append({
                        "file_name": file_path.name,
                        "entity": country_display,
                        "table": full_table,
                        "rows": f"{file_rows:,}",
                        "batches": file_batches,
                        "duration_sec": file_duration,
                        "duration_str": format_duration(file_duration),
                        "speed": "0 rows/s",
                        "status": "FAILED",
                    })

            elapsed = time.time() - entity_start_time
            summary_results.append({
                "entity": country_display,
                "table": full_table,
                "folders": ", ".join(str(f) for f in folders),
                "status": "SUCCESS",
                "files": len(files),
                "rows": f"{total_entity_rows:,}",
                "duration": format_duration(elapsed),
            })

    finally:
        raw_conn.close()

    # 8. Print Final Execution Summary Tables
    total_elapsed = time.time() - start_time

    # Per-File Upload Performance Report
    if file_upload_stats:
        print("\n" + "=" * 115)
        print(" PER-FILE UPLOAD PERFORMANCE (TIME TAKEN PER FILE)")
        print("=" * 115)
        print(
            f"{'File Name':<35} {'Entity':<12} {'Target Table':<32} {'Rows':<12} {'Batches':<9} {'Time Taken':<12} {'Speed':<14}"
        )
        print("-" * 115)
        for stat in file_upload_stats:
            print(
                f"{stat['file_name'][:33]:<35} {stat['entity'][:10]:<12} {stat['table'][:30]:<32} "
                f"{stat['rows']:<12} {stat['batches']:<9} {stat['duration_str']:<12} {stat['speed']:<14}"
            )
        print("=" * 115)

    # Entity-Level Aggregate Report
    print("\n" + "=" * 110)
    print(" ENTITY-LEVEL INGESTION SUMMARY")
    print("=" * 110)
    print(f"{'Entity':<12} {'Target Table':<34} {'Files':<8} {'Rows':<14} {'Duration':<12} {'Status':<10}")
    print("-" * 110)
    for res in summary_results:
        print(
            f"{res['entity']:<12} {res['table'][:32]:<34} {res['files']:<8} "
            f"{res['rows']:<14} {res['duration']:<12} {res['status']:<10}"
        )
    print("=" * 110)
    print(f"Total Pipeline Duration: {format_duration(total_elapsed)}\n")


def main():
    try:
        import_folders_to_postgres()
    except Exception:
        logger.exception("Pipeline terminated unexpectedly.")
        raise


if __name__ == "__main__":
    main()
