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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDuckLakeTypeMapping
{
    @Test
    void testToTrinoType_booleans()
    {
        assertEquals(BooleanType.BOOLEAN, DuckLakeTypeMapping.toTrinoType("BOOLEAN"));
        assertEquals(BooleanType.BOOLEAN, DuckLakeTypeMapping.toTrinoType("BOOL"));
        assertEquals(BooleanType.BOOLEAN, DuckLakeTypeMapping.toTrinoType("LOGICAL"));
    }

    @Test
    void testToTrinoType_integers()
    {
        assertEquals(TinyintType.TINYINT, DuckLakeTypeMapping.toTrinoType("INT8"));
        assertEquals(TinyintType.TINYINT, DuckLakeTypeMapping.toTrinoType("TINYINT"));
        assertEquals(TinyintType.TINYINT, DuckLakeTypeMapping.toTrinoType("INT1"));
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("INT16"));
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("SMALLINT"));
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("INT2"));
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("SHORT"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("INT32"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("INTEGER"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("INT"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("INT4"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("SIGNED"));
        assertEquals(BigintType.BIGINT, DuckLakeTypeMapping.toTrinoType("INT64"));
        assertEquals(BigintType.BIGINT, DuckLakeTypeMapping.toTrinoType("BIGINT"));
        assertEquals(BigintType.BIGINT, DuckLakeTypeMapping.toTrinoType("LONG"));
    }

    @Test
    void testToTrinoType_hugeint()
    {
        assertEquals(DecimalType.createDecimalType(38, 0), DuckLakeTypeMapping.toTrinoType("INT128"));
        assertEquals(DecimalType.createDecimalType(38, 0), DuckLakeTypeMapping.toTrinoType("HUGEINT"));
    }

    @Test
    void testToTrinoType_unsignedIntegers()
    {
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("UINT8"));
        assertEquals(SmallintType.SMALLINT, DuckLakeTypeMapping.toTrinoType("UTINYINT"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("UINT16"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("USMALLINT"));
        assertEquals(BigintType.BIGINT, DuckLakeTypeMapping.toTrinoType("UINT32"));
        assertEquals(BigintType.BIGINT, DuckLakeTypeMapping.toTrinoType("UINTEGER"));
        assertEquals(DecimalType.createDecimalType(20, 0), DuckLakeTypeMapping.toTrinoType("UINT64"));
        assertEquals(DecimalType.createDecimalType(20, 0), DuckLakeTypeMapping.toTrinoType("UBIGINT"));
    }

    @Test
    void testToTrinoType_floats()
    {
        assertEquals(RealType.REAL, DuckLakeTypeMapping.toTrinoType("FLOAT32"));
        assertEquals(RealType.REAL, DuckLakeTypeMapping.toTrinoType("FLOAT"));
        assertEquals(RealType.REAL, DuckLakeTypeMapping.toTrinoType("FLOAT4"));
        assertEquals(RealType.REAL, DuckLakeTypeMapping.toTrinoType("REAL"));
        assertEquals(DoubleType.DOUBLE, DuckLakeTypeMapping.toTrinoType("FLOAT64"));
        assertEquals(DoubleType.DOUBLE, DuckLakeTypeMapping.toTrinoType("DOUBLE"));
        assertEquals(DoubleType.DOUBLE, DuckLakeTypeMapping.toTrinoType("FLOAT8"));
    }

    @Test
    void testToTrinoType_decimal()
    {
        assertEquals(DecimalType.createDecimalType(10, 2), DuckLakeTypeMapping.toTrinoType("DECIMAL(10,2)"));
        assertEquals(DecimalType.createDecimalType(10, 2), DuckLakeTypeMapping.toTrinoType("DECIMAL(10, 2)"));
        assertEquals(DecimalType.createDecimalType(38, 0), DuckLakeTypeMapping.toTrinoType("DECIMAL(38,0)"));
        assertEquals(DecimalType.createDecimalType(5, 0), DuckLakeTypeMapping.toTrinoType("DECIMAL(5)"));
        assertEquals(DecimalType.createDecimalType(10, 2), DuckLakeTypeMapping.toTrinoType("NUMERIC(10,2)"));
        assertEquals(DecimalType.createDecimalType(5, 0), DuckLakeTypeMapping.toTrinoType("NUMERIC(5)"));
    }

    @Test
    void testToTrinoType_strings()
    {
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("VARCHAR"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("TEXT"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("STRING"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("CHAR"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("BPCHAR"));
    }

    @Test
    void testToTrinoType_binary()
    {
        assertEquals(VarbinaryType.VARBINARY, DuckLakeTypeMapping.toTrinoType("BLOB"));
        assertEquals(VarbinaryType.VARBINARY, DuckLakeTypeMapping.toTrinoType("BYTEA"));
        assertEquals(VarbinaryType.VARBINARY, DuckLakeTypeMapping.toTrinoType("BINARY"));
        assertEquals(VarbinaryType.VARBINARY, DuckLakeTypeMapping.toTrinoType("VARBINARY"));
    }

    @Test
    void testToTrinoType_temporal()
    {
        assertEquals(DateType.DATE, DuckLakeTypeMapping.toTrinoType("DATE"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP WITHOUT TIME ZONE"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("DATETIME"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP_S"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP_MS"));
        assertEquals(TimestampType.TIMESTAMP_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP_NS"));
        assertEquals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMP WITH TIME ZONE"));
        assertEquals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS, DuckLakeTypeMapping.toTrinoType("TIMESTAMPTZ"));
    }

    @Test
    void testToTrinoType_uuid()
    {
        assertEquals(UuidType.UUID, DuckLakeTypeMapping.toTrinoType("UUID"));
    }

    @Test
    void testToTrinoType_complexAndFallback()
    {
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("JSON"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("INTERVAL"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("INTEGER[]"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("LIST(INT)"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("STRUCT(a INT, b VARCHAR)"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("MAP(VARCHAR, INT)"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType(null));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("UNKNOWN_TYPE"));
    }

    @Test
    void testToTrinoType_caseInsensitive()
    {
        assertEquals(BooleanType.BOOLEAN, DuckLakeTypeMapping.toTrinoType("boolean"));
        assertEquals(BooleanType.BOOLEAN, DuckLakeTypeMapping.toTrinoType("Boolean"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("integer"));
        assertEquals(IntegerType.INTEGER, DuckLakeTypeMapping.toTrinoType("int32"));
        assertEquals(VarcharType.VARCHAR, DuckLakeTypeMapping.toTrinoType("varchar"));
    }

    @Test
    void testToDuckLakeType_allTypes()
    {
        assertEquals("boolean", DuckLakeTypeMapping.toDuckLakeType(BooleanType.BOOLEAN));
        assertEquals("int8", DuckLakeTypeMapping.toDuckLakeType(TinyintType.TINYINT));
        assertEquals("int16", DuckLakeTypeMapping.toDuckLakeType(SmallintType.SMALLINT));
        assertEquals("int32", DuckLakeTypeMapping.toDuckLakeType(IntegerType.INTEGER));
        assertEquals("int64", DuckLakeTypeMapping.toDuckLakeType(BigintType.BIGINT));
        assertEquals("float32", DuckLakeTypeMapping.toDuckLakeType(RealType.REAL));
        assertEquals("float64", DuckLakeTypeMapping.toDuckLakeType(DoubleType.DOUBLE));
        assertEquals("decimal(10, 2)", DuckLakeTypeMapping.toDuckLakeType(DecimalType.createDecimalType(10, 2)));
        assertEquals("decimal(38, 0)", DuckLakeTypeMapping.toDuckLakeType(DecimalType.createDecimalType(38, 0)));
        assertEquals("varchar", DuckLakeTypeMapping.toDuckLakeType(VarcharType.VARCHAR));
        assertEquals("blob", DuckLakeTypeMapping.toDuckLakeType(VarbinaryType.VARBINARY));
        assertEquals("date", DuckLakeTypeMapping.toDuckLakeType(DateType.DATE));
        assertEquals("timestamp", DuckLakeTypeMapping.toDuckLakeType(TimestampType.TIMESTAMP_MICROS));
        assertEquals("timestamptz", DuckLakeTypeMapping.toDuckLakeType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS));
        assertEquals("uuid", DuckLakeTypeMapping.toDuckLakeType(UuidType.UUID));
    }

    @Test
    void testRoundTrip()
    {
        Type[] types = {
                BooleanType.BOOLEAN,
                TinyintType.TINYINT,
                SmallintType.SMALLINT,
                IntegerType.INTEGER,
                BigintType.BIGINT,
                RealType.REAL,
                DoubleType.DOUBLE,
                DecimalType.createDecimalType(10, 2),
                VarcharType.VARCHAR,
                VarbinaryType.VARBINARY,
                DateType.DATE,
                TimestampType.TIMESTAMP_MICROS,
                TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS,
                UuidType.UUID,
        };

        for (Type type : types) {
            String duckLakeType = DuckLakeTypeMapping.toDuckLakeType(type);
            Type roundTripped = DuckLakeTypeMapping.toTrinoType(duckLakeType);
            assertEquals(type, roundTripped, "Round-trip failed for " + type);
        }
    }
}
