package io.trino.plugin.ducklake;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum DuckLakeTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
