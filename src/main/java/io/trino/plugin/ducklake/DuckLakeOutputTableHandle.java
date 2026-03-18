package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DuckLakeOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<DuckLakeColumnHandle> columns;
    private final long tableId;
    private final long snapshotId;
    private final String tableUuid;
    private final String dataPath;
    private final String schemaPath;
    private final String tablePath;

    @JsonCreator
    public DuckLakeOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<DuckLakeColumnHandle> columns,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("tableUuid") String tableUuid,
            @JsonProperty("dataPath") String dataPath,
            @JsonProperty("schemaPath") String schemaPath,
            @JsonProperty("tablePath") String tablePath)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.tableId = tableId;
        this.snapshotId = snapshotId;
        this.tableUuid = requireNonNull(tableUuid, "tableUuid is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.schemaPath = schemaPath;
        this.tablePath = tablePath;
    }

    @JsonProperty
    public String getSchemaName() { return schemaName; }

    @JsonProperty
    public String getTableName() { return tableName; }

    @JsonProperty
    public List<DuckLakeColumnHandle> getColumns() { return columns; }

    @JsonProperty
    public long getTableId() { return tableId; }

    @JsonProperty
    public long getSnapshotId() { return snapshotId; }

    @JsonProperty
    public String getTableUuid() { return tableUuid; }

    @JsonProperty
    public String getDataPath() { return dataPath; }

    @JsonProperty
    public String getSchemaPath() { return schemaPath; }

    @JsonProperty
    public String getTablePath() { return tablePath; }
}
