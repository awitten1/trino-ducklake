package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class DuckLakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DuckLakeConnectionManager connectionManager;

    @Inject
    public DuckLakePageSinkProvider(TrinoFileSystemFactory fileSystemFactory, DuckLakeConnectionManager connectionManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        DuckLakeOutputTableHandle handle = (DuckLakeOutputTableHandle) outputTableHandle;
        return new DuckLakePageSink(
                fileSystemFactory.create(session),
                handle.getDataPath(),
                handle.getSchemaPath(),
                handle.getTablePath(),
                handle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        DuckLakeInsertTableHandle handle = (DuckLakeInsertTableHandle) insertTableHandle;
        return new DuckLakePageSink(
                fileSystemFactory.create(session),
                handle.getDataPath(),
                handle.getSchemaPath(),
                handle.getTablePath(),
                handle.getColumns());
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        DuckLakeMergeTableHandle handle = (DuckLakeMergeTableHandle) mergeTableHandle;
        return new DuckLakeMergeSink(
                fileSystemFactory,
                session,
                connectionManager,
                handle);
    }
}
