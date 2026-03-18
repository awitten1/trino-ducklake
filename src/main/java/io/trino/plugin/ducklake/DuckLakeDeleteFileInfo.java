package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record DuckLakeDeleteFileInfo(
        @JsonProperty("dataFileId") long dataFileId,
        @JsonProperty("path") String path,
        @JsonProperty("deleteCount") long deleteCount,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("footerSize") long footerSize)
{
    @JsonCreator
    public DuckLakeDeleteFileInfo {}
}
