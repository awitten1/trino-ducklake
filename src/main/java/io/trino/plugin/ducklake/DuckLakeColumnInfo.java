package io.trino.plugin.ducklake;

record DuckLakeColumnInfo(
        long columnId,
        int columnOrder,
        String columnName,
        String columnType,
        boolean nullsAllowed)
{}
