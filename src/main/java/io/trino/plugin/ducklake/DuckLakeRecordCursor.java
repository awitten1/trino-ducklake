package io.trino.plugin.ducklake;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Objects.requireNonNull;

public class DuckLakeRecordCursor
        implements RecordCursor
{
    private final List<DuckLakeColumnHandle> columnHandles;
    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final long recordCount;

    public DuckLakeRecordCursor(
            DuckLakeSplit split,
            List<DuckLakeColumnHandle> columnHandles,
            DuckLakeConnectionManager connectionManager)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.recordCount = split.getRecordCount();

        try {
            this.connection = connectionManager.openDataConnection();
            this.statement = connection.createStatement();
            String sql = buildQuery(split, columnHandles);
            this.resultSet = statement.executeQuery(sql);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to open Parquet file: " + split.getDataFilePath(), e);
        }
    }

    private static String buildQuery(DuckLakeSplit split, List<DuckLakeColumnHandle> columnHandles)
    {
        String columnList = columnHandles.stream()
                .map(h -> "\"" + h.getColumnName().replace("\"", "\"\"") + "\"")
                .collect(Collectors.joining(", "));
        if (columnList.isEmpty()) {
            columnList = "1";
        }

        String escapedPath = split.getDataFilePath().replace("'", "''");

        StringBuilder sql = new StringBuilder("SELECT ").append(columnList);

        if (split.getDeleteFilePath().isPresent()) {
            // Include file_row_number for delete filtering
            sql.append(" FROM read_parquet('").append(escapedPath).append("', file_row_number=true)");
            String escapedDeletePath = split.getDeleteFilePath().get().replace("'", "''");
            sql.append(" WHERE (").append(split.getRowIdStart())
                    .append(" + file_row_number) NOT IN (SELECT row_id FROM read_parquet('")
                    .append(escapedDeletePath).append("'))");
        }
        else {
            sql.append(" FROM read_parquet('").append(escapedPath).append("')");
        }

        return sql.toString();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            return resultSet.next();
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to advance cursor", e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        try {
            return resultSet.getBoolean(field + 1);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read boolean at field " + field, e);
        }
    }

    @Override
    public long getLong(int field)
    {
        Type type = getType(field);
        try {
            if (type instanceof DateType) {
                java.sql.Date date = resultSet.getDate(field + 1);
                return date == null ? 0 : date.toLocalDate().toEpochDay();
            }
            if (type instanceof TimestampType) {
                Timestamp ts = resultSet.getTimestamp(field + 1);
                if (ts == null) {
                    return 0;
                }
                return ts.toInstant().getEpochSecond() * 1_000_000L + ts.getNanos() / 1_000L;
            }
            if (type instanceof TimestampWithTimeZoneType) {
                Timestamp ts = resultSet.getTimestamp(field + 1);
                if (ts == null) {
                    return 0;
                }
                long millis = ts.getTime();
                return packDateTimeWithZone(millis, UTC_KEY);
            }
            if (type instanceof RealType) {
                float f = resultSet.getFloat(field + 1);
                return Float.floatToIntBits(f) & 0xFFFFFFFFL;
            }
            if (type instanceof DecimalType decimalType && !decimalType.isShort()) {
                // Long decimals are returned as Slice; getLong shouldn't be called for them
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "getLong called for long decimal type");
            }
            return resultSet.getLong(field + 1);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read long at field " + field, e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        try {
            return resultSet.getDouble(field + 1);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read double at field " + field, e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        try {
            if (type instanceof VarbinaryType) {
                byte[] bytes = resultSet.getBytes(field + 1);
                return bytes == null ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(bytes);
            }
            String value = resultSet.getString(field + 1);
            return value == null ? Slices.EMPTY_SLICE : Slices.utf8Slice(value);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read slice at field " + field, e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        try {
            return resultSet.getObject(field + 1);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read object at field " + field, e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        try {
            resultSet.getObject(field + 1);
            return resultSet.wasNull();
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to check null at field " + field, e);
        }
    }

    @Override
    public void close()
    {
        try {
            resultSet.close();
        }
        catch (SQLException ignored) {
        }
        try {
            statement.close();
        }
        catch (SQLException ignored) {
        }
        try {
            connection.close();
        }
        catch (SQLException ignored) {
        }
    }
}
