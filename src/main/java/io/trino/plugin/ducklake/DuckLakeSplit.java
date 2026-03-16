package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class DuckLakeSplit
        implements ConnectorSplit
{
    private final String dataFilePath;
    private final Optional<String> deleteFilePath;
    private final long rowIdStart;
    private final long recordCount;

    @JsonCreator
    public DuckLakeSplit(
            @JsonProperty("dataFilePath") String dataFilePath,
            @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
            @JsonProperty("rowIdStart") long rowIdStart,
            @JsonProperty("recordCount") long recordCount)
    {
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.deleteFilePath = requireNonNull(deleteFilePath, "deleteFilePath is null");
        this.rowIdStart = rowIdStart;
        this.recordCount = recordCount;
    }

    @JsonProperty
    public String getDataFilePath()
    {
        return dataFilePath;
    }

    @JsonProperty
    public Optional<String> getDeleteFilePath()
    {
        return deleteFilePath;
    }

    @JsonProperty
    public long getRowIdStart()
    {
        return rowIdStart;
    }

    @JsonProperty
    public long getRecordCount()
    {
        return recordCount;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return dataFilePath;
    }
}
