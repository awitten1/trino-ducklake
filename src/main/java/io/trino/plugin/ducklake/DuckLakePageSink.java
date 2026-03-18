package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
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
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.ConnectorPageSink.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class DuckLakePageSink
        implements ConnectorPageSink
{
    private static final JsonCodec<DuckLakeWrittenFileInfo> FILE_INFO_CODEC = JsonCodec.jsonCodec(DuckLakeWrittenFileInfo.class);
    private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024; // 128MB

    private final TrinoFileSystem fileSystem;
    private final String dataPath;
    private final String schemaPath;
    private final String tablePath;
    private final List<DuckLakeColumnHandle> columns;
    private final List<Type> types;
    private final List<String> columnNames;

    private final List<DuckLakeWrittenFileInfo> writtenFiles = new ArrayList<>();
    private ParquetWriter currentWriter;
    private String currentFilePath;
    private String currentRelativePath;
    private OutputStream currentOutputStream;
    private long currentFileRowCount;

    // Per-column stats for current file
    private long[] nullCounts;
    private long[] valueCounts;
    private String[] minValues;
    private String[] maxValues;

    public DuckLakePageSink(
            TrinoFileSystem fileSystem,
            String dataPath,
            String schemaPath,
            String tablePath,
            List<DuckLakeColumnHandle> columns)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.schemaPath = schemaPath;
        this.tablePath = tablePath;
        this.columns = requireNonNull(columns, "columns is null");
        this.types = columns.stream().map(DuckLakeColumnHandle::getColumnType).toList();
        this.columnNames = columns.stream().map(DuckLakeColumnHandle::getColumnName).toList();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        try {
            if (currentWriter == null) {
                openNewFile();
            }

            currentWriter.write(page);
            currentFileRowCount += page.getPositionCount();
            updateColumnStats(page);

            if (currentWriter.getWrittenBytes() >= TARGET_FILE_SIZE) {
                closeCurrentFile();
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to write Parquet data", e);
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            if (currentWriter != null) {
                closeCurrentFile();
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to finish writing Parquet data", e);
        }

        ImmutableList.Builder<Slice> fragments = ImmutableList.builder();
        for (DuckLakeWrittenFileInfo fileInfo : writtenFiles) {
            fragments.add(Slices.utf8Slice(FILE_INFO_CODEC.toJson(fileInfo)));
        }
        return CompletableFuture.completedFuture(fragments.build());
    }

    @Override
    public void abort()
    {
        try {
            if (currentWriter != null) {
                currentWriter.close();
                currentWriter = null;
            }
        }
        catch (IOException ignored) {
        }

        // Best-effort cleanup of written files
        for (DuckLakeWrittenFileInfo fileInfo : writtenFiles) {
            try {
                fileSystem.deleteFile(toLocation(resolveFullPath(fileInfo.path())));
            }
            catch (IOException ignored) {
            }
        }
    }

    private void openNewFile()
            throws IOException
    {
        String fileName = UUID.randomUUID() + ".parquet";
        String tableDirectoryPath = buildTableDirectoryPath();
        String fullPath = tableDirectoryPath + fileName;

        currentRelativePath = fileName;
        currentFilePath = fullPath;
        currentFileRowCount = 0;

        // Initialize per-column stats
        int columnCount = columns.size();
        nullCounts = new long[columnCount];
        valueCounts = new long[columnCount];
        minValues = new String[columnCount];
        maxValues = new String[columnCount];

        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                types,
                columnNames,
                false,
                false);
        MessageType messageType = schemaConverter.getMessageType();
        Map<List<String>, Type> primitiveTypes = schemaConverter.getPrimitiveTypes();

        ParquetWriterOptions writerOptions = ParquetWriterOptions.builder().build();

        ensureTableDirectoryExists(tableDirectoryPath);

        Location location = toLocation(currentFilePath);
        currentOutputStream = fileSystem.newOutputFile(location).create();

        currentWriter = new ParquetWriter(
                currentOutputStream,
                messageType,
                primitiveTypes,
                writerOptions,
                CompressionCodec.SNAPPY,
                "trino-ducklake",
                Optional.empty(),
                Optional.empty());
    }

    private void ensureTableDirectoryExists(String tableDirectoryPath)
            throws IOException
    {
        fileSystem.createDirectory(toLocation(tableDirectoryPath));
    }

    private String buildTableDirectoryPath()
    {
        StringBuilder fullPath = new StringBuilder(dataPath);
        if (schemaPath != null && !schemaPath.isEmpty()) {
            fullPath.append(schemaPath);
            if (!schemaPath.endsWith("/")) {
                fullPath.append("/");
            }
        }
        if (tablePath != null && !tablePath.isEmpty()) {
            fullPath.append(tablePath);
            if (!tablePath.endsWith("/")) {
                fullPath.append("/");
            }
        }
        return fullPath.toString();
    }

    private void closeCurrentFile()
            throws IOException
    {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        long fileSizeBytes = currentWriter.getWrittenBytes();
        long footerSize = 0; // ParquetWriter doesn't expose footer size separately

        List<DuckLakeWrittenFileInfo.ColumnStats> columnStatsList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnStatsList.add(new DuckLakeWrittenFileInfo.ColumnStats(
                    columns.get(i).getColumnId(),
                    valueCounts[i],
                    nullCounts[i],
                    minValues[i],
                    maxValues[i]));
        }

        writtenFiles.add(new DuckLakeWrittenFileInfo(
                currentRelativePath,
                currentFileRowCount,
                fileSizeBytes,
                footerSize,
                columnStatsList));

        currentWriter = null;
        currentOutputStream = null;
    }

    private void updateColumnStats(Page page)
    {
        int positionCount = page.getPositionCount();
        for (int channel = 0; channel < columns.size(); channel++) {
            Block block = page.getBlock(channel);
            Type type = types.get(channel);
            valueCounts[channel] += positionCount;

            for (int position = 0; position < positionCount; position++) {
                if (block.isNull(position)) {
                    nullCounts[channel]++;
                    continue;
                }
                String valueStr = extractValueAsString(type, block, position);
                if (valueStr != null) {
                    if (minValues[channel] == null || compareStringValues(type, valueStr, minValues[channel]) < 0) {
                        minValues[channel] = valueStr;
                    }
                    if (maxValues[channel] == null || compareStringValues(type, valueStr, maxValues[channel]) > 0) {
                        maxValues[channel] = valueStr;
                    }
                }
            }
        }
    }

    private static String extractValueAsString(Type type, Block block, int position)
    {
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            return String.valueOf(type.getLong(block, position));
        }
        if (type instanceof RealType) {
            int intBits = (int) type.getLong(block, position);
            return String.valueOf(Float.intBitsToFloat(intBits));
        }
        if (type instanceof DoubleType) {
            return String.valueOf(type.getDouble(block, position));
        }
        if (type instanceof DateType) {
            return String.valueOf(type.getLong(block, position));
        }
        if (type instanceof BooleanType) {
            return String.valueOf(type.getBoolean(block, position));
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return String.valueOf(type.getLong(block, position));
            }
        }
        // For other types, skip stats
        return null;
    }

    private static int compareStringValues(Type type, String a, String b)
    {
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType || type instanceof DateType) {
            return Long.compare(Long.parseLong(a), Long.parseLong(b));
        }
        if (type instanceof RealType || type instanceof DoubleType) {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        }
        if (type instanceof DecimalType) {
            return new java.math.BigDecimal(a).compareTo(new java.math.BigDecimal(b));
        }
        return a.compareTo(b);
    }

    private String resolveFullPath(String relativePath)
    {
        return dataPath + relativePath;
    }

    private static Location toLocation(String path)
    {
        if (path.startsWith("/")) {
            return Location.of(Path.of(path).toUri().toString());
        }
        return Location.of(path);
    }
}
