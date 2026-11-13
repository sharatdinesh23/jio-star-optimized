# JioStar PostgreSQL Batch Data Ingestion

This pipeline reads tabular data files (`.csv`, `.tsv`, `.parquet`, `.xlsx`) from multiple folders specified in `.env`, streams and processes them in configurable batches, and dumps them into a PostgreSQL database under the schema `jiostar_data`.

Each destination table name automatically matches the name of its source folder (e.g., folder `december_internal_ca_data` imports into table `jiostar_data.december_internal_ca_data`).

---

## Features

- **Ultra-Fast PostgreSQL COPY Streaming**: Replaces slow multi-row `INSERT` queries with PostgreSQL's native `COPY FROM STDIN` protocol. Ingestion speed increases from ~2,000 rows/sec to **50,000 - 100,000+ rows/sec** (4-5 lakh records load in ~5-10 seconds instead of 4 minutes).
- **Multi-Folder Ingestion**: Configure multiple folder paths in `.env` (comma-separated, semicolon-separated, or JSON list).
- **Batch Processing**: Streams files in configurable chunks (default: 50,000 rows per batch) to keep memory consumption low.
- **Dynamic Schema Evolution**: Automatically creates the `jiostar_data` schema and tables. Dynamically detects and alters tables to add new columns if subsequent files or batches contain extra attributes.
- **Safe Raw Data Typing**: Preserves leading zeros (e.g., account IDs, order codes, zip codes) and raw strings without lossy type inferences.
- **Dry-Run Mode**: Inspect folders, preview tables, discover files, and validate columns without needing an active database connection.
- **Per-File Upload Tracking**: Displays real-time upload duration, batch count, and throughput (rows/sec) for every individual file processed.
- **Detailed Summary Reports**: Prints both a dedicated per-file upload performance table and a folder-level summary table upon completion.
- **Execution Summary**: Provides a clear console report with file counts, row counts, and duration upon completion.

---

## Quick Start

### 1. Configure `.env`

Copy `.env.example` to `.env` (if not already created) and fill in your PostgreSQL credentials:

```ini
# PostgreSQL Connection Credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password_here

# Target Schema
DB_SCHEMA=jiostar_data

# Data Folders to Import (comma-separated paths)
DATA_FOLDERS=Data/december_internal_ca_data,Data/december_internal_india_data,Data/december_internal_sg_data

# Batch Ingestion Settings
BATCH_SIZE=5000
TABLE_IF_EXISTS=append
PRESERVE_RAW_STRINGS=true
```

### 2. Preview with Dry-Run Mode

Verify that all folders, files, and columns can be read properly without writing to the database:

```bash
python main.py --dry-run
```

### 3. Run Ingestion

Execute the batch ingestion into PostgreSQL:

```bash
python main.py
```
*(Or with `uv`: `uv run main.py`)*

---

## Schema & Table Mapping

| Source Folder | Target PostgreSQL Table |
| :--- | :--- |
| `Data/december_internal_ca_data` | `jiostar_data.december_internal_ca_data` |
| `Data/december_internal_india_data` | `jiostar_data.december_internal_india_data` |
| `Data/december_internal_sg_data` | `jiostar_data.december_internal_sg_data` |

---

## Dependencies

- Python >= 3.12
- `pandas`
- `sqlalchemy`
- `psycopg2-binary`
- `python-dotenv`

