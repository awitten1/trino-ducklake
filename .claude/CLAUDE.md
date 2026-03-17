# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
./mvnw clean package -DskipTests
```

The plugin JAR and its dependencies are assembled into `target/ducklake/`.

## Run locally

```bash
./run-trino.sh
```

This builds the connector, downloads Trino 479 if needed, installs the plugin, generates a minimal config, and starts Trino at `http://localhost:8080`. The catalog config is written to `trino-server-479/etc/catalog/ducklake.properties`.

To generate test data (creates `metadata.sqlite` + `data_files/`):

```bash
./generate-ducklake-database.sh
```

## Architecture

This is a Trino connector SPI implementation for DuckLake — a data lake format backed by a JDBC metadata database (SQLite, DuckDB, or PostgreSQL) and Parquet data files.

### Data flow

1. `DuckLakeMetadata` calls `DuckLakeClient` to list schemas/tables/columns by querying the metadata DB.
2. For query execution, `DuckLakeSplitManager` calls `DuckLakeClient.getDataFiles()` to enumerate one `DuckLakeSplit` per Parquet data file.
3. `DuckLakePageSourceProvider` creates a `DuckLakeParquetPageSource` per split, which reads the file via Trino's native Parquet reader (`ParquetReader`).
4. Page sources are optionally wrapped in `DuckLakeConstraintFilteringPageSource` (predicate filtering) and `DuckLakeDeleteFilteringPageSource` (positional deletes).

### Key design decisions

**Snapshot isolation**: A `snapshotId` is captured once in `getTableHandle()` and carried through all handles and splits, ensuring consistent reads across split enumeration and data reading.

**Single connection per operation**: `DuckLakeClient` methods open one metadata connection and reuse it for all sub-queries within a single logical operation (e.g., `getDataFiles` fetches data path, table info, and file stats on the same connection).

**Classloader isolation workaround**: `DuckLakeConnectionManager` directly instantiates JDBC drivers (`new org.sqlite.JDBC().connect(...)`, `new org.duckdb.DuckDBDriver().connect(...)`) instead of using `DriverManager`. This is intentional — Trino's plugin classloader isolation prevents `DriverManager` from finding drivers loaded in the plugin classloader.

**Delete file path semantics**: DuckDB stores `file_path` values in positional delete files relative to the **metadata base directory** (e.g., `data_files/main/x/filename.parquet`), not relative to `data_path`. `DuckLakeDeleteFileReader.normalizeDeleteTargetLocation` uses `metadataBaseDirectory` as the base — this is correct.

**Predicate pushdown**: `applyFilter()` stores a `TupleDomain<ColumnHandle>` on `DuckLakeTableHandle`. `DuckLakeClient.getAllowedDataFileIds()` uses per-file min/max/null stats from `ducklake_file_column_stats` to prune data files before split creation.

**`contains_null` semantics**: The `contains_null` column in `ducklake_table_column_stats` is a boolean flag. Only `false` means definitively 0 nulls; `true` or NULL means unknown — do not set `nullsFraction` to 1.0 in that case.

### Configuration

| Property | Description |
|----------|-------------|
| `ducklake.metadata-connection-string` | JDBC URL for the metadata database (SQLite, DuckDB, or PostgreSQL) |
| `fs.native-local.enabled=true` | Required for local filesystem access |
| `local.location=/` | Required for local filesystem access |

`data_path` is read from `ducklake_metadata` inside the metadata database — it is not a connector property.
