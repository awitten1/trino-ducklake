package io.trino.plugin.ducklake;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class DuckLakeParquetDataSource
        extends AbstractParquetDataSource
{
    private final TrinoInput input;

    public DuckLakeParquetDataSource(TrinoInputFile file, ParquetReaderOptions options)
            throws IOException
    {
        super(new ParquetDataSourceId(file.location().toString()), file.length(), options);
        this.input = requireNonNull(file, "file is null").newInput();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        return input.readTail(length);
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.readFully(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public Metrics getMetrics()
    {
        return input.getMetrics();
    }
}
