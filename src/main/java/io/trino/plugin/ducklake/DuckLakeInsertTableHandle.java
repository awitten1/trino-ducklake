package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DuckLakeInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final long tableId;
    private final List<DuckLakeColumnHandle> columns;
    private final String dataPath;
    private final String schemaPath;
    private final String tablePath;

    @JsonCreator
    public DuckLakeInsertTableHandle(
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columns") List<DuckLakeColumnHandle> columns,
            @JsonProperty("dataPath") String dataPath,
            @JsonProperty("schemaPath") String schemaPath,
            @JsonProperty("tablePath") String tablePath)
    {
        this.tableId = tableId;
        this.columns = requireNonNull(columns, "columns is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.schemaPath = schemaPath;
        this.tablePath = tablePath;
    }

    @JsonProperty
    public long getTableId() { return tableId; }

    @JsonProperty
    public List<DuckLakeColumnHandle> getColumns() { return columns; }

    @JsonProperty
    public String getDataPath() { return dataPath; }

    @JsonProperty
    public String getSchemaPath() { return schemaPath; }

    @JsonProperty
    public String getTablePath() { return tablePath; }
}
