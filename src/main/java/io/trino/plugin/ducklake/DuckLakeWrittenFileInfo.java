package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record DuckLakeWrittenFileInfo(
        @JsonProperty("path") String path,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("footerSize") long footerSize,
        @JsonProperty("columnStats") List<ColumnStats> columnStats)
{
    @JsonCreator
    public DuckLakeWrittenFileInfo {}

    public record ColumnStats(
            @JsonProperty("columnId") long columnId,
            @JsonProperty("valueCount") long valueCount,
            @JsonProperty("nullCount") long nullCount,
            @JsonProperty("minValue") String minValue,
            @JsonProperty("maxValue") String maxValue)
    {}
}
