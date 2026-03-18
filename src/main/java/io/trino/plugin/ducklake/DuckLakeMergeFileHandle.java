package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DuckLakeMergeFileHandle(
        @JsonProperty("dataFileId") long dataFileId,
        @JsonProperty("dataFilePath") String dataFilePath,
        @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
        @JsonProperty("rowIdStart") long rowIdStart)
{
    @JsonCreator
    public DuckLakeMergeFileHandle
    {
        requireNonNull(dataFilePath, "dataFilePath is null");
        deleteFilePath = requireNonNull(deleteFilePath, "deleteFilePath is null");
    }
}
