package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.RowType;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class DuckLakeMergeRowIdHandle
        implements ColumnHandle
{
    public static final RowType TYPE = RowType.rowType(
            RowType.field("data_file_id", BIGINT),
            RowType.field("data_file_path", VARCHAR),
            RowType.field("row_position", BIGINT));

    @JsonCreator
    public DuckLakeMergeRowIdHandle(@JsonProperty("marker") String marker) {}

    @JsonProperty
    public String getMarker()
    {
        return "ducklake-merge-row-id";
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof DuckLakeMergeRowIdHandle;
    }

    @Override
    public int hashCode()
    {
        return DuckLakeMergeRowIdHandle.class.hashCode();
    }

    @Override
    public String toString()
    {
        return "$ducklake_merge_row_id";
    }
}
