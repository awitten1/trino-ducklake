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

    public static String toDuckLakeType(Type trinoType)
    {
        if (trinoType instanceof BooleanType) {
            return "boolean";
        }
        if (trinoType instanceof TinyintType) {
            return "int8";
        }
        if (trinoType instanceof SmallintType) {
            return "int16";
        }
        if (trinoType instanceof IntegerType) {
            return "int32";
        }
        if (trinoType instanceof BigintType) {
            return "int64";
        }
        if (trinoType instanceof RealType) {
            return "float32";
        }
        if (trinoType instanceof DoubleType) {
            return "float64";
        }
        if (trinoType instanceof DecimalType decimalType) {
            return "decimal(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
        }
        if (trinoType instanceof VarcharType) {
            return "varchar";
        }
        if (trinoType instanceof VarbinaryType) {
            return "blob";
        }
        if (trinoType instanceof DateType) {
            return "date";
        }
        if (trinoType instanceof TimestampWithTimeZoneType) {
            return "timestamptz";
        }
        if (trinoType instanceof TimestampType) {
            return "timestamp";
        }
        if (trinoType instanceof UuidType) {
            return "uuid";
        }
        return "varchar";
    }

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

            // DuckLake canonical names use bit-width (INT8=8-bit, INT16=16-bit, etc.)
            case "INT8", "TINYINT", "INT1" -> TinyintType.TINYINT;
            case "INT16", "SMALLINT", "INT2", "SHORT" -> SmallintType.SMALLINT;
            case "INT32", "INTEGER", "INT", "INT4", "SIGNED" -> IntegerType.INTEGER;
            case "INT64", "BIGINT", "LONG" -> BigintType.BIGINT;
            case "INT128", "HUGEINT" -> DecimalType.createDecimalType(38, 0);

            // Unsigned integers — map to next larger signed type
            case "UINT8", "UTINYINT" -> SmallintType.SMALLINT;
            case "UINT16", "USMALLINT" -> IntegerType.INTEGER;
            case "UINT32", "UINTEGER" -> BigintType.BIGINT;
            case "UINT64", "UBIGINT" -> DecimalType.createDecimalType(20, 0);

            // Floating point
            case "FLOAT32", "FLOAT", "FLOAT4", "REAL" -> RealType.REAL;
            case "FLOAT64", "DOUBLE", "FLOAT8" -> DoubleType.DOUBLE;

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
