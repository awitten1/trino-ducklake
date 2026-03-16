package io.trino.plugin.ducklake;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class DuckLakeParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader reader;
    private final ParquetDataSource dataSource;
    private final AggregatedMemoryContext memoryContext;
    private boolean finished;

    public DuckLakeParquetPageSource(ParquetReader reader, ParquetDataSource dataSource, AggregatedMemoryContext memoryContext)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(reader.lastBatchStartRow());
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        try {
            SourcePage page = reader.nextPage();
            if (page == null) {
                finished = true;
            }
            return page;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryContext.getBytes();
    }

    @Override
    public Metrics getMetrics()
    {
        return reader.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        IOException failure = null;
        try {
            reader.close();
        }
        catch (IOException e) {
            failure = e;
        }
        try {
            dataSource.close();
        }
        catch (IOException e) {
            if (failure == null) {
                failure = e;
            }
        }
        memoryContext.close();
        if (failure != null) {
            throw failure;
        }
    }
}
