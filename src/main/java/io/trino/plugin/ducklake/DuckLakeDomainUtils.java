package io.trino.plugin.ducklake;

import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

public final class DuckLakeDomainUtils
{
    private DuckLakeDomainUtils() {}

    public static boolean isSupportedPredicateType(Type type)
    {
        return type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof DateType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof VarcharType ||
                type instanceof VarbinaryType ||
                (type instanceof DecimalType decimalType && decimalType.isShort());
    }

    public static Optional<Object> getDomainValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return Optional.empty();
        }
        if (type instanceof BooleanType) {
            return Optional.of(type.getBoolean(block, position));
        }
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType || type instanceof DateType) {
            return Optional.of(type.getLong(block, position));
        }
        if (type instanceof RealType) {
            return Optional.of(type.getLong(block, position));
        }
        if (type instanceof DoubleType) {
            return Optional.of(type.getDouble(block, position));
        }
        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            return Optional.of(type.getSlice(block, position));
        }
        if (type instanceof DecimalType decimalType && decimalType.isShort()) {
            return Optional.of(type.getLong(block, position));
        }
        return Optional.empty();
    }
}
