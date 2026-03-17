#!/bin/bash
set -eu

usage() {
    echo "Usage: $0 [sqlite|postgres]"
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
    exit 1
}

MODE="${1:-sqlite}"

case "$MODE" in
    sqlite)
        duckdb -c "
install ducklake; load ducklake;
attach 'ducklake:sqlite:metadata.sqlite' as my_ducklake (DATA_PATH 'data_files/');
use my_ducklake;
create or replace table x as select i as col1, i**2 as col2 from range(1000) r(i);
insert into x select i + 2000, null from range(1000) r(i);
delete from x where col1 < 100;
checkpoint;
"
        ;;
    postgres)
        DATA_DIR="$(pwd)/data_files_pg"
        mkdir -p "$DATA_DIR"
        duckdb -c "
install ducklake; load ducklake;
install postgres; load postgres;
attach 'ducklake:postgres:host=localhost port=5433 dbname=ducklake_meta user=postgres password=testpass' as my_ducklake (DATA_PATH '$DATA_DIR/');
use my_ducklake;
create or replace table x as select i as col1, i**2 as col2 from range(1000) r(i);
insert into x select i + 2000, null from range(1000) r(i);
delete from x where col1 < 100;
checkpoint;
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
