package io.trino.plugin.ducklake;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.util.OptionalLong;

import static io.trino.spi.connector.SourcePage.create;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class DuckLakeDeleteFilteringPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final LongOpenHashSet deletedRowIds;
    private final long rowIdStart;

    public DuckLakeDeleteFilteringPageSource(
            ConnectorPageSource delegate,
            LongOpenHashSet deletedRowIds,
            long rowIdStart)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.deletedRowIds = requireNonNull(deletedRowIds, "deletedRowIds is null");
        this.rowIdStart = rowIdStart;
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
        while (true) {
            SourcePage page = delegate.getNextSourcePage();
            if (page == null) {
                return null;
            }

            int rowNumberChannel = page.getChannelCount() - 1;
            Block rowNumbers = page.getBlock(rowNumberChannel);
            int[] retained = new int[page.getPositionCount()];
            int retainedCount = 0;
            for (int position = 0; position < page.getPositionCount(); position++) {
                long rowId = rowIdStart + BIGINT.getLong(rowNumbers, position);
                if (!deletedRowIds.contains(rowId)) {
                    retained[retainedCount++] = position;
                }
            }

            if (retainedCount == 0) {
                continue;
            }

            if (retainedCount < page.getPositionCount()) {
                page.selectPositions(retained, 0, retainedCount);
            }
            return create(page.getColumns(allChannels(page.getChannelCount())));
        }
    }

    private static int[] allChannels(int channelCount)
    {
        int[] channels = new int[channelCount];
        for (int index = 0; index < channelCount; index++) {
            channels[index] = index;
        }
        return channels;
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
