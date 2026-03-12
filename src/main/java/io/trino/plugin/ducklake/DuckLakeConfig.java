package io.trino.plugin.ducklake;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class DuckLakeConfig
{
    private String metadataConnectionString;
    private String dataPath;

    @NotNull
    public String getMetadataConnectionString()
    {
        return metadataConnectionString;
    }

    @Config("ducklake.metadata-connection-string")
    public DuckLakeConfig setMetadataConnectionString(String metadataConnectionString)
    {
        this.metadataConnectionString = metadataConnectionString;
        return this;
    }

    public Optional<String> getDataPath()
    {
        return Optional.ofNullable(dataPath);
    }

    @Config("ducklake.data-path")
    public DuckLakeConfig setDataPath(String dataPath)
    {
        this.dataPath = dataPath;
        return this;
    }
}
