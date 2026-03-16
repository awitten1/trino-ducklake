package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DuckLakeRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DuckLakeConnectionManager connectionManager;

    @Inject
    public DuckLakeRecordSetProvider(DuckLakeConnectionManager connectionManager)
    {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        DuckLakeSplit duckLakeSplit = (DuckLakeSplit) split;

        ImmutableList.Builder<DuckLakeColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((DuckLakeColumnHandle) handle);
        }

        return new DuckLakeRecordSet(duckLakeSplit, handles.build(), connectionManager);
    }
}
