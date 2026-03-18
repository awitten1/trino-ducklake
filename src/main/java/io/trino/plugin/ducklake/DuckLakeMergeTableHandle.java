package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class DuckLakeMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final DuckLakeTableHandle tableHandle;
    private final List<DuckLakeColumnHandle> columns;
    private final String dataPath;
    private final String schemaPath;
    private final String tablePath;
    private final List<DuckLakeMergeFileHandle> activeFiles;

    @JsonCreator
    public DuckLakeMergeTableHandle(
            @JsonProperty("tableHandle") DuckLakeTableHandle tableHandle,
            @JsonProperty("columns") List<DuckLakeColumnHandle> columns,
            @JsonProperty("dataPath") String dataPath,
            @JsonProperty("schemaPath") String schemaPath,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("activeFiles") List<DuckLakeMergeFileHandle> activeFiles)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.schemaPath = schemaPath;
        this.tablePath = tablePath;
        this.activeFiles = List.copyOf(requireNonNull(activeFiles, "activeFiles is null"));
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty("tableHandle")
    public DuckLakeTableHandle getDuckLakeTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<DuckLakeColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getDataPath()
    {
        return dataPath;
    }

    @JsonProperty
    public String getSchemaPath()
    {
        return schemaPath;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public List<DuckLakeMergeFileHandle> getActiveFiles()
    {
        return activeFiles;
    }
}
