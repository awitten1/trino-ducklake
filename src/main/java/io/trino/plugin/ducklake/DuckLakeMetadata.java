package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DuckLakeMetadata
        implements ConnectorMetadata
{
    private final DuckLakeClient duckLakeClient;

    @Inject
    public DuckLakeMetadata(DuckLakeClient duckLakeClient)
    {
        this.duckLakeClient = requireNonNull(duckLakeClient, "duckLakeClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return duckLakeClient.getSchemaNames();
    }

    @Override
    public DuckLakeTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return duckLakeClient.getTableInfoWithSnapshot(tableName.getSchemaName(), tableName.getTableName())
                .map(tis -> new DuckLakeTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        tis.tableInfo().tableId(),
                        tis.snapshotId()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        return getTableMetadata(handle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return duckLakeClient.listTables(optionalSchemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle duckLakeTableHandle = (DuckLakeTableHandle) tableHandle;

        List<DuckLakeColumnInfo> columnInfos = duckLakeClient.getColumnInfos(
                duckLakeTableHandle.getTableId(),
                duckLakeTableHandle.getSnapshotId());

        if (columnInfos == null) {
            throw new TableNotFoundException(duckLakeTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (DuckLakeColumnInfo info : columnInfos) {
            columnHandles.put(
                    info.columnName(),
                    new DuckLakeColumnHandle(
                            info.columnName(),
                            DuckLakeTypeMapping.toTrinoType(info.columnType()),
                            index,
                            info.columnId()));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DuckLakeColumnHandle) columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = duckLakeClient.getColumns(tableName.getSchemaName(), tableName.getTableName());
        if (columns == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }
}
