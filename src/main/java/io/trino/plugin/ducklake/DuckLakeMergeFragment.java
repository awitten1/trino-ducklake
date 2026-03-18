package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record DuckLakeMergeFragment(
        @JsonProperty("insertedFiles") List<DuckLakeWrittenFileInfo> insertedFiles,
        @JsonProperty("deleteFiles") List<DuckLakeDeleteFileInfo> deleteFiles)
{
    @JsonCreator
    public DuckLakeMergeFragment {}
}
