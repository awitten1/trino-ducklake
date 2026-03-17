#!/bin/bash
set -eu

usage() {
    echo "Usage: $0 [sqlite|postgres|s3]"
    echo ""
    echo "Generate a DuckLake test database with sample data."
    echo ""
    echo "Modes:"
    echo "  sqlite    (default) Metadata in metadata.sqlite, data in data_files/"
    echo "  postgres  Metadata in PostgreSQL, data in data_files_pg/"
    echo "            Requires: docker container 'ducklake-test-pg' on port 5433"
    echo "            Start it with:"
    echo "              docker run -d --name ducklake-test-pg \\"
    echo "                -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=ducklake_meta \\"
    echo "                -p 5433:5432 postgres:16-alpine"
    echo "  s3        Metadata in PostgreSQL, data in S3"
    echo "            Requires: S3_BUCKET env var (e.g. my-bucket)"
    echo "            Optional: S3_PATH prefix (e.g. ducklake/test), METADATA_CONN"
    echo "            Uses AWS credential chain for authentication"
    exit 1
}

MODE="${1:-sqlite}"

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-testpass}"

# Ensure a PostgreSQL database exists, creating it if needed.
ensure_pg_database() {
    local dbname="$1"
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -tc \
        "SELECT 1 FROM pg_database WHERE datname = '${dbname}'" | grep -q 1 \
        || PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c \
        "CREATE DATABASE ${dbname}"
}

# SQL statements shared across all modes
SEED_SQL="
create or replace table x as select i as col1, i**2 as col2 from range(1000) r(i);
insert into x select i + 2000, null from range(1000) r(i);
delete from x where col1 < 100;
checkpoint;
"

case "$MODE" in
    sqlite)
        duckdb -c "
install ducklake; load ducklake;
attach 'ducklake:sqlite:metadata.sqlite' as my_ducklake (DATA_PATH 'data_files/');
use my_ducklake;
${SEED_SQL}
"
        ;;
    postgres)
        ensure_pg_database ducklake_meta
        DATA_DIR="$(pwd)/data_files_pg"
        mkdir -p "$DATA_DIR"
        duckdb -c "
install ducklake; load ducklake;
install postgres; load postgres;
attach 'ducklake:postgres:host=${PG_HOST} port=${PG_PORT} dbname=ducklake_meta user=${PG_USER} password=${PG_PASSWORD}' as my_ducklake (DATA_PATH '$DATA_DIR/');
use my_ducklake;
${SEED_SQL}
"
        ;;
    s3)
        if [ -z "${S3_BUCKET:-}" ]; then
            echo "Error: S3_BUCKET env var is required for s3 mode"
            echo "  Example: S3_BUCKET=my-bucket $0 s3"
            exit 1
        fi
        ensure_pg_database ducklake_meta_s3
        S3_DATA_PATH="s3://${S3_BUCKET}/${S3_PATH:+${S3_PATH}/}"
        PG_CONN="${METADATA_CONN:-host=${PG_HOST} port=${PG_PORT} dbname=ducklake_meta_s3 user=${PG_USER} password=${PG_PASSWORD}}"
        duckdb -c "
install ducklake; load ducklake;
install postgres; load postgres;
install aws; load aws;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);
attach 'ducklake:postgres:${PG_CONN}' as my_ducklake (DATA_PATH '${S3_DATA_PATH}');
use my_ducklake;
${SEED_SQL}
"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown mode: $MODE"
        usage
        ;;
esac
