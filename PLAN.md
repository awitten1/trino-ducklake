# Trino DuckLake Connector — Implementation Plan

## Architecture

The connector connects **directly** to the DuckLake metadata database (DuckDB file, PostgreSQL, etc.) via JDBC and executes the raw SQL queries from `queries.md` itself. It does **not** use the DuckDB `ducklake` extension — that extension is for DuckDB to act as a DuckLake client, not relevant here.

- **Metadata**: JDBC connection to the underlying metadata database. Config property `ducklake.metadata-connection-string` is a full JDBC URL (e.g. `jdbc:duckdb:/path/to/catalog.ducklake` or `jdbc:postgresql://host/db`).
- **Data reading**: An in-memory DuckDB connection using `read_parquet()` to read Parquet data files.
- **Writes** (future): A read-write JDBC connection to the metadata DB plus DuckDB's `COPY ... TO ... (FORMAT PARQUET)` for writing data files.

### Key structural notes from `my_ducklake.ducklake`

- `ducklake_metadata` is a **key-value table** (`key`, `value`, `scope`, `scope_id`). Get the data path with `SELECT value FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL`.
- **Path construction**: A relative data file path is assembled as `data_path + schema.path + table.path + data_file.path`. Each of `ducklake_schema` and `ducklake_table` has its own `path` and `path_is_relative` columns.
- **DuckDB column type names** use lowercase aliases like `int64`, `float64` — the type mapping must handle these.

---

## Phase 1 — DuckLakeConnectionManager, DuckLakeTypeMapping, DuckLakeClient

### `DuckLakeConnectionManager` (created)

A `@Singleton` with two factory methods:
- `openMetadataConnection()` — opens a JDBC connection to the metadata database. For `jdbc:duckdb:` URLs, opens with `duckdb.read_only=true`.
- `openDataConnection()` — opens an in-memory DuckDB connection (`jdbc:duckdb:`) for `read_parquet()` queries.

Each caller owns the returned connection and must close it.

### `DuckLakeTypeMapping` (created)

Static method `toTrinoType(String duckLakeType)` mapping DuckLake/DuckDB type strings to Trino `Type` instances. Handles both canonical names (`BIGINT`, `DOUBLE`) and internal/alias names (`int64`, `float64`, `INT8`, etc.). Falls back to `VarcharType.VARCHAR` for unknown or complex types (LIST, STRUCT, MAP).

### Internal records (created)

- `DuckLakeTableInfo` — carries tableId, schemaId, schema/table names, schema/table paths and their relativity flags
- `DuckLakeColumnInfo` — carries columnId, columnOrder, columnName, columnType (raw string), nullsAllowed
- `DuckLakeDataFileInfo` — carries dataFileId, resolved absolute dataFilePath, optional resolved deleteFilePath, rowIdStart, recordCount

### `DuckLakeClient` (implemented)

Key methods:
- `getCurrentSnapshotId(conn)` — `SELECT max(snapshot_id) FROM ducklake_snapshot`
- `getDataPath()` — `SELECT value FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL`
- `getSchemaNames()` — snapshot-scoped query on `ducklake_schema`
- `listTables(Optional<String>)` — snapshot-scoped join of `ducklake_table` + `ducklake_schema`
- `getTableInfoWithSnapshot(schemaName, tableName)` — resolves tableId + snapshotId atomically in one connection
- `getColumnInfos(tableId, snapshotId)` — snapshot-scoped query on `ducklake_column` (top-level only)
- `getDataFiles(tableId, snapshotId, tableInfo)` — data files + left-join delete files, resolves absolute paths

**Path resolution**: `full_path = data_path + schema.path + table.path + file.path` (when `path_is_relative = true`).

---

## Phase 2 — DuckLakeTableHandle & DuckLakeColumnHandle

### `DuckLakeTableHandle` (updated)

Added `long tableId` and `long snapshotId`. Both are `@JsonProperty` for cross-node serialization. The snapshotId is captured once in `getTableHandle()` to guarantee snapshot-consistent reads across all splits.

### `DuckLakeColumnHandle` (updated)

Added `long columnId` for future use in file-level statistics pruning (Phase 7).

### `DuckLakeMetadata` (updated)

- `getTableHandle()` calls `getTableInfoWithSnapshot()` to resolve tableId + snapshotId atomically.
- `getColumnHandles()` uses `getColumnInfos(tableId, snapshotId)` and builds handles with the full `DuckLakeColumnInfo`.

---

## Phase 3 — DuckLakeSplit

### `DuckLakeSplit` (rewritten)

Converted from singleton enum to a real `ConnectorSplit` value object. Fields:
- `String dataFilePath` — absolute resolved path to the Parquet data file
- `Optional<String> deleteFilePath` — absolute path to the delete file, if any
- `long rowIdStart` — starting global row ID for this file (needed for delete filtering)
- `long recordCount`

---

## Phase 4 — DuckLakeSplitManager

### `DuckLakeSplitManager` (updated)

Injects `DuckLakeClient`. In `getSplits()`:
1. Resolves `DuckLakeTableInfo` (for path context)
2. Calls `client.getDataFiles(tableId, snapshotId, tableInfo)`
3. Maps each `DuckLakeDataFileInfo` to a `DuckLakeSplit`
4. Returns a `FixedSplitSource`

---

## Phase 5 — DuckLakeRecordCursor

### `DuckLakeRecordCursor` (created)

Implements `RecordCursor`. Each instance:
- Opens its own in-memory DuckDB connection (thread-safe: one connection per cursor)
- Builds a SQL query: `SELECT <projected_cols> FROM read_parquet('<path>')`
- If a delete file is present, adds `file_row_number=true` and a `WHERE (row_id_start + file_row_number) NOT IN (SELECT row_id FROM read_parquet('<delete_path>'))` clause
- Implements `getBoolean/getLong/getDouble/getSlice/getObject/isNull` with type-aware extraction

Type-specific `getLong` handling:
- `DateType` → epoch days via `java.sql.Date.toLocalDate().toEpochDay()`
- `TimestampType` → epoch microseconds
- `TimestampWithTimeZoneType` → `packDateTimeWithZone(millis, UTC_KEY)`
- `RealType` → `Float.floatToIntBits(f) & 0xFFFFFFFFL`

### `DuckLakeRecordSet` (updated)

Constructor now takes `DuckLakeSplit`, `List<DuckLakeColumnHandle>`, and `DuckLakeConnectionManager`. `cursor()` constructs and returns `DuckLakeRecordCursor`.

### `DuckLakeRecordSetProvider` (updated)

Injects `DuckLakeConnectionManager`. Passes the `DuckLakeSplit` (not the table handle) through to `DuckLakeRecordSet`.

---

## Phase 6 — Write Support (future)

### 6a: DDL

Override in `DuckLakeMetadata`:
- `createSchema` / `dropSchema` → snapshot + INSERT/UPDATE on `ducklake_schema`
- `createTable` / `dropTable` → snapshot + INSERT/UPDATEs on `ducklake_table`, `ducklake_column`, stats tables

### 6b: INSERT

- `DuckLakePageSink` — buffers Pages, writes Parquet via `COPY ... TO (FORMAT PARQUET)`, collects per-file stats
- `DuckLakePageSinkProvider`
- `DuckLakeMetadata.finishInsert()` — registers data files in metadata and creates new snapshot

### 6c/6d: DELETE / UPDATE

- DELETE: write positional delete file (list of row IDs), register in `ducklake_delete_file`
- UPDATE: DELETE old rows + INSERT new rows in same snapshot

---

## Phase 7 — File Pruning / Predicate Pushdown (future)

- `DuckLakeMetadata.applyFilter()` captures `TupleDomain` into `DuckLakeTableHandle`
- `DuckLakeClient.getDataFiles()` filters by `ducklake_file_column_stats` (min/max range queries per column)

---

## New Classes Summary

| Class | Phase | Status |
|---|---|---|
| `DuckLakeConnectionManager` | 1 | Done |
| `DuckLakeTypeMapping` | 1 | Done |
| `DuckLakeTableInfo`, `DuckLakeColumnInfo`, `DuckLakeDataFileInfo` | 1 | Done |
| `DuckLakeRecordCursor` | 5 | Done |
| `DuckLakeOutputTableHandle`, `DuckLakeInsertTableHandle`, `DuckLakeDeleteTableHandle` | 6b/6c | Future |
| `DuckLakeWrittenFile` | 6b | Future |
| `DuckLakePageSink`, `DuckLakePageSinkProvider` | 6b | Future |

---

## Configuration

`ducklake.metadata-connection-string` must be a **JDBC URL**:

```properties
connector.name=ducklake
# SQLite metadata backend (bundled):
ducklake.metadata-connection-string=jdbc:sqlite:/path/to/metadata.sqlite

# DuckDB metadata backend (bundled):
ducklake.metadata-connection-string=jdbc:duckdb:/path/to/catalog.ducklake

# PostgreSQL metadata backend (requires postgres JDBC driver on classpath):
# ducklake.metadata-connection-string=jdbc:postgresql://localhost/ducklake_catalog
```

### Relative data paths

If `data_path` in `ducklake_metadata` is a relative path (e.g. `data_files/`), it is resolved against the directory containing the metadata file. This works automatically for `jdbc:sqlite:` and `jdbc:duckdb:` URLs. For server-based databases (PostgreSQL), the data path must be absolute.

---

## Key Implementation Notes

1. **DuckDB thread safety** — Each `RecordCursor` opens its own dedicated DuckDB connection. Never share a DuckDB JDBC connection across threads.
2. **Snapshot isolation** — `snapshotId` is captured once in `getTableHandle()` and carried through all handles/splits.
3. **Path resolution** — Full path = `data_path + schema.path + table.path + file.path` when `path_is_relative = true`. Always read `data_path` from `ducklake_metadata` table, not from config.
4. **ducklake_metadata is a key-value table** — use `WHERE key = 'data_path' AND scope IS NULL`, not a column named `data_path`.
