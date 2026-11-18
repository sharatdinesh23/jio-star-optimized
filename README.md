# JioStar PostgreSQL Batch Data Ingestion

This pipeline reads tabular data files (`.csv`, `.tsv`, `.parquet`, `.xlsx`) from folders/links configured in `.env` for **4 key entities**:
1. **India**
2. **UK**
3. **Singapore**
4. **Canada**

Each entity's data is batch-imported into its dedicated table under the specified PostgreSQL schema (default: `jiostar_data`) using the exact format:
$$\text{schema\_name}.\text{internal\_<country\_name>\_data}$$

---

## Entity-to-Table Mapping

| Entity | Target Table (`schema_name = jiostar_data`) | Default / Mapped Folder |
| :--- | :--- | :--- |
| **India** | `jiostar_data.internal_india_data` | `Data/december_internal_india_data` |
| **UK** | `jiostar_data.internal_uk_data` | Configurable in `.env` (`DATA_FOLDER_UK`) |
| **Singapore** | `jiostar_data.internal_singapore_data` | `Data/december_internal_sg_data` |
| **Canada** | `jiostar_data.internal_canada_data` | `Data/december_internal_ca_data` |

---

## Features

- **Entity-Based Auto-Mapping**: Configure paths via dedicated country variables (`DATA_FOLDER_INDIA`, `DATA_FOLDER_UK`, etc.) or via `DATA_FOLDERS` with automatic entity detection.
- **Ultra-Fast PostgreSQL COPY Streaming**: Direct `COPY FROM STDIN` streaming enables loading 4–5 lakh records in ~5–10 seconds.
- **Multi-File Batching**: Multiple daily or monthly files inside an entity folder are automatically merged into the entity's target table.
- **Dynamic Schema Evolution**: New columns appearing in subsequent files are automatically added to the table via `ALTER TABLE ADD COLUMN`.
- **Per-File & Entity Progress Tracking**: Real-time batch progress logging, file timing, and summary tables.

---

## Configuration (`.env`)

Set your database credentials and specify folder paths/links for each entity:

```ini
# PostgreSQL Connection Credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=sharath23

# Target Schema
DB_SCHEMA=jiostar_data

# Entity Data Folders / Links (4 Entities)
DATA_FOLDER_INDIA=Data/december_internal_india_data
DATA_FOLDER_UK=
DATA_FOLDER_SINGAPORE=Data/december_internal_sg_data
DATA_FOLDER_CANADA=Data/december_internal_ca_data

# Batch Ingestion Settings
BATCH_SIZE=100000
TABLE_IF_EXISTS=append
PRESERVE_RAW_STRINGS=true
CSV_FIELD_SIZE_LIMIT=67108864
```

---

## Terminal Input & Usage

### 1. Interactive Terminal Prompt (Default)
When you run `python main.py` in your terminal, the script interactively prompts you to choose how to provide the folder links:
```text
======================================================================
 JIOSTAR DATA INGESTION - TERMINAL INPUT
======================================================================
How would you like to provide the entity folder links?
  [1] Enter link for a single entity (India / UK / Singapore / Canada)
  [2] Enter links for all 4 entities (prompted one by one)
  [3] Use saved links from .env directly
Enter choice [1/2/3] (default: 2): 
```
- **Option 1**: Select a specific country (e.g. India) and enter its link.
- **Option 2**: Prompt for each of the 4 countries (press Enter to accept `.env` default or type a new link).
- **Option 3**: Use saved links directly from `.env`.

---

### 2. Command-Line Direct Terminal Input
You can also supply entity links directly as terminal flags:

- **Single Entity**:
  ```bash
  python main.py --entity india --link "Data/december_internal_india_data"
  ```
- **Multiple Specific Entities**:
  ```bash
  python main.py --india "Data/december_internal_india_data" --uk "path/to/uk_data"
  ```
- **Dry-Run Preview**:
  ```bash
  python main.py --dry-run
  ```
- **Non-Interactive (Bypass Prompt)**:
  ```bash
  python main.py --non-interactive
  ```
