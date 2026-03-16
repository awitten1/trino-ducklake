package io.trino.plugin.ducklake;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class DuckLakeConfig
{
    private String metadataConnectionString;

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
}
