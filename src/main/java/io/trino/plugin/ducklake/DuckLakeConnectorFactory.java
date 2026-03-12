package io.trino.plugin.ducklake;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DuckLakeConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "ducklake";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(),
                new DuckLakeModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(DuckLakeConnector.class);
    }
}
