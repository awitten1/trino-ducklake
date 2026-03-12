package io.trino.plugin.ducklake;

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DuckLakeRecordSet
        implements RecordSet
{
    private final DuckLakeTableHandle tableHandle;
    private final List<DuckLakeColumnHandle> columnHandles;

    public DuckLakeRecordSet(DuckLakeTableHandle tableHandle, List<DuckLakeColumnHandle> columnHandles)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
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
        // TODO: implement cursor that reads data from DuckLake via DuckDB
        throw new UnsupportedOperationException("not yet implemented");
    }
}
