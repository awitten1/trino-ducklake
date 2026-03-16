package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class DuckLakeTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final long tableId;
    private final long snapshotId;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public DuckLakeTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableId = tableId;
        this.snapshotId = snapshotId;
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public DuckLakeTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new DuckLakeTableHandle(schemaName, tableName, tableId, snapshotId, constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableId, snapshotId, constraint);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DuckLakeTableHandle other = (DuckLakeTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                this.tableId == other.tableId &&
                this.snapshotId == other.snapshotId &&
                Objects.equals(this.constraint, other.constraint);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName + "@snapshot=" + snapshotId + ", constraint=" + constraint;
    }
}
