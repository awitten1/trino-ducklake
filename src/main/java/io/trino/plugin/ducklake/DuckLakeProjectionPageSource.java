package io.trino.plugin.ducklake;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static io.trino.spi.connector.SourcePage.create;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DuckLakeProjectionPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<ColumnHandle> requestedColumns;
    private final Map<String, Integer> readChannelByName;
    private final boolean hasRowNumberChannel;
    private final long dataFileId;
    private final String dataFilePath;

    public DuckLakeProjectionPageSource(
            ConnectorPageSource delegate,
            List<ColumnHandle> requestedColumns,
            List<DuckLakeColumnHandle> readColumns,
            boolean hasRowNumberChannel,
            long dataFileId,
            String dataFilePath)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.requestedColumns = List.copyOf(requireNonNull(requestedColumns, "requestedColumns is null"));
        this.hasRowNumberChannel = hasRowNumberChannel;
        this.dataFileId = dataFileId;
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.readChannelByName = new HashMap<>();
        for (int index = 0; index < readColumns.size(); index++) {
            readChannelByName.put(readColumns.get(index).getColumnName(), index);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page = delegate.getNextSourcePage();
        if (page == null) {
            return null;
        }

        Block[] outputBlocks = new Block[requestedColumns.size()];
        int rowNumberChannel = hasRowNumberChannel ? page.getChannelCount() - 1 : -1;
        for (int index = 0; index < requestedColumns.size(); index++) {
            ColumnHandle column = requestedColumns.get(index);
            if (column instanceof DuckLakeColumnHandle duckLakeColumn) {
                outputBlocks[index] = page.getBlock(readChannelByName.get(duckLakeColumn.getColumnName()));
            }
            else if (column instanceof DuckLakeMergeRowIdHandle) {
                outputBlocks[index] = buildMergeRowIdBlock(page, rowNumberChannel);
            }
            else {
                throw new IllegalArgumentException("Unsupported column handle: " + column);
            }
        }
        return create(new Page(page.getPositionCount(), outputBlocks));
    }

    private Block buildMergeRowIdBlock(SourcePage page, int rowNumberChannel)
    {
        Block rowNumbers = page.getBlock(rowNumberChannel);
        int positionCount = page.getPositionCount();
        BlockBuilder positions = BIGINT.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            BIGINT.writeLong(positions, BIGINT.getLong(rowNumbers, position));
        }

        Block dataFileIdBlock = RunLengthEncodedBlock.create(BIGINT, dataFileId, positionCount);
        Block dataFilePathBlock = RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice(dataFilePath), positionCount);
        return RowBlock.fromFieldBlocks(positionCount, new Block[] {
                dataFileIdBlock,
                dataFilePathBlock,
                positions.build()});
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
