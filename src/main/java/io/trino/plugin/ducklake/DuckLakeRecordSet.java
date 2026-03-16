package io.trino.plugin.ducklake;

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DuckLakeRecordSet
        implements RecordSet
{
    private final DuckLakeSplit split;
    private final List<DuckLakeColumnHandle> columnHandles;
    private final DuckLakeConnectionManager connectionManager;

    public DuckLakeRecordSet(
            DuckLakeSplit split,
            List<DuckLakeColumnHandle> columnHandles,
            DuckLakeConnectionManager connectionManager)
    {
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnHandles.stream()
                .map(DuckLakeColumnHandle::getColumnType)
                .toList();
    }

    @Override
    public RecordCursor cursor()
    {
        return new DuckLakeRecordCursor(split, columnHandles, connectionManager);
    }
}
