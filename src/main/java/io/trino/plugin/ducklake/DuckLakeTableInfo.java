package io.trino.plugin.ducklake;

record DuckLakeTableInfo(
        long tableId,
        long schemaId,
        String schemaName,
        String tableName,
        String schemaPath,
        boolean schemaPathIsRelative,
        String tablePath,
        boolean tablePathIsRelative)
{}
