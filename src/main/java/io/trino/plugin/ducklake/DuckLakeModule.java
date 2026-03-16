package io.trino.plugin.ducklake;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DuckLakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DuckLakeConnector.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeConnectionManager.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeClient.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DuckLakeConfig.class);
    }
}
