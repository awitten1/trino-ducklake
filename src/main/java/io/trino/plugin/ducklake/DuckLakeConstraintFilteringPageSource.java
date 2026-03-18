package io.trino.plugin.ducklake;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.connector.SourcePage.create;
import static java.util.Objects.requireNonNull;

public class DuckLakeConstraintFilteringPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<DuckLakeColumnHandle> readColumns;
    private final Map<Integer, Domain> domainsByChannel;

    public DuckLakeConstraintFilteringPageSource(
            ConnectorPageSource delegate,
            List<DuckLakeColumnHandle> readColumns,
            Map<Integer, Domain> domainsByChannel)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.readColumns = List.copyOf(requireNonNull(readColumns, "readColumns is null"));
        this.domainsByChannel = Map.copyOf(requireNonNull(domainsByChannel, "domainsByChannel is null"));
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

            int[] retained = new int[page.getPositionCount()];
            int retainedCount = 0;
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (matches(page, position)) {
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

    private boolean matches(SourcePage page, int position)
    {
        for (Map.Entry<Integer, Domain> entry : domainsByChannel.entrySet()) {
            int channel = entry.getKey();
            Domain domain = entry.getValue();
            Optional<Object> value = DuckLakeDomainUtils.getDomainValue(readColumns.get(channel).getColumnType(), page.getBlock(channel), position);
            if (!domain.includesNullableValue(value.orElse(null))) {
                return false;
            }
        }
        return true;
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
