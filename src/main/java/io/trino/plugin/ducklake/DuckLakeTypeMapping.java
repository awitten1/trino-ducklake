package io.trino.plugin.ducklake;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DuckLakeTypeMapping
{
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(?:DECIMAL|NUMERIC)\\((\\d+)(?:,\\s*(\\d+))?\\)");

    private DuckLakeTypeMapping() {}

    public static Type toTrinoType(String duckLakeType)
    {
        if (duckLakeType == null) {
            return VarcharType.VARCHAR;
        }
        String upper = duckLakeType.toUpperCase(Locale.ROOT).trim();

        // DECIMAL(p, s) or NUMERIC(p, s)
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(upper);
        if (decimalMatcher.matches()) {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = decimalMatcher.group(2) != null ? Integer.parseInt(decimalMatcher.group(2)) : 0;
            return DecimalType.createDecimalType(precision, scale);
        }

        return switch (upper) {
            case "BOOLEAN", "BOOL", "LOGICAL" -> BooleanType.BOOLEAN;

            // Integers — both DuckDB byte-count aliases (INT1/INT2/INT4/INT8)
            // and bit-width names (INT8/INT16/INT32/INT64) used by DuckLake
            case "TINYINT", "INT1" -> TinyintType.TINYINT;
            case "SMALLINT", "INT2", "SHORT" -> SmallintType.SMALLINT;
            case "INTEGER", "INT", "INT4", "INT32", "SIGNED" -> IntegerType.INTEGER;
            case "BIGINT", "INT8", "LONG", "INT64" -> BigintType.BIGINT;
            case "HUGEINT", "INT128" -> DecimalType.createDecimalType(38, 0);

            // Unsigned integers — map to next larger signed type
            case "UTINYINT", "UINT8" -> SmallintType.SMALLINT;
            case "USMALLINT", "UINT16" -> IntegerType.INTEGER;
            case "UINTEGER", "UINT32" -> BigintType.BIGINT;
            case "UBIGINT", "UINT64" -> DecimalType.createDecimalType(20, 0);

            // Floating point
            case "FLOAT", "FLOAT4", "REAL", "FLOAT32" -> RealType.REAL;
            case "DOUBLE", "FLOAT8", "FLOAT64" -> DoubleType.DOUBLE;

            // Strings
            case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR" -> VarcharType.VARCHAR;

            // Binary
            case "BLOB", "BYTEA", "BINARY", "VARBINARY" -> VarbinaryType.VARBINARY;

            // Temporal
            case "DATE" -> DateType.DATE;
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "DATETIME",
                    "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" -> TimestampType.TIMESTAMP_MICROS;
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;

            // UUID
            case "UUID" -> UuidType.UUID;

            // JSON and others — fall back to VARCHAR
            case "JSON", "INTERVAL" -> VarcharType.VARCHAR;

            default -> {
                // Handle array types like "INTEGER[]" or "INT64[]"
                if (upper.endsWith("[]")) {
                    yield VarcharType.VARCHAR; // complex types as JSON string for now
                }
                // Handle LIST(type), STRUCT(...), MAP(...)
                if (upper.startsWith("LIST(") || upper.startsWith("STRUCT(") || upper.startsWith("MAP(")) {
                    yield VarcharType.VARCHAR;
                }
                yield VarcharType.VARCHAR;
            }
        };
    }
}
