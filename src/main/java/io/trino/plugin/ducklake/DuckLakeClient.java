package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DuckLakeClient
{
    private final DuckLakeConfig config;

    @Inject
    public DuckLakeClient(DuckLakeConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        // TODO: Initialize DuckDB connection using config.getMetadataConnectionString()
    }

    public List<String> getSchemaNames()
    {
        // TODO: Query DuckLake for schemas
        return List.of();
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        // TODO: List tables from DuckLake
        return List.of();
    }

    public boolean tableExists(String schemaName, String tableName)
    {
        // TODO: Check if table exists in DuckLake
        return false;
    }

    public List<ColumnMetadata> getColumns(String schemaName, String tableName)
    {
        // TODO: Fetch column metadata from DuckLake
        return null;
    }
}
