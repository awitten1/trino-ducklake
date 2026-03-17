package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;


import static java.util.Objects.requireNonNull;

public class DuckLakeSplitManager
        implements ConnectorSplitManager
{
    private final DuckLakeClient client;

    @Inject
    public DuckLakeSplitManager(DuckLakeClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) connectorTableHandle;

        List<DuckLakeDataFileInfo> dataFiles = client.getDataFiles(tableHandle, tableHandle.getConstraint());

        List<DuckLakeSplit> splits = dataFiles.stream()
                .map(file -> new DuckLakeSplit(
                        file.dataFilePath(),
                        file.deleteFilePath(),
                        file.rowIdStart(),
                        file.recordCount()))
                .toList();

        return new FixedSplitSource(splits);
    }
}
