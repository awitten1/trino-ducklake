package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DuckLakeMergeSink
        implements ConnectorMergeSink
{
    private static final JsonCodec<DuckLakeMergeFragment> MERGE_FRAGMENT_CODEC = JsonCodec.jsonCodec(DuckLakeMergeFragment.class);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final TrinoFileSystem fileSystem;
    private final ConnectorSession session;
    private final DuckLakeConnectionManager connectionManager;
    private final DuckLakeMergeTableHandle handle;
    private final DuckLakePageSink insertPageSink;
    private final Map<Long, DeleteRows> deleteRowsByFileId = new LinkedHashMap<>();
    private final Map<Long, DuckLakeMergeFileHandle> activeFilesById;

    public DuckLakeMergeSink(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            DuckLakeConnectionManager connectionManager,
            DuckLakeMergeTableHandle handle)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileSystem = fileSystemFactory.create(session);
        this.session = requireNonNull(session, "session is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.handle = requireNonNull(handle, "handle is null");
        this.insertPageSink = new DuckLakePageSink(
                fileSystem,
                handle.getDataPath(),
                handle.getSchemaPath(),
                handle.getTablePath(),
                handle.getColumns());
        this.activeFilesById = new HashMap<>();
        for (DuckLakeMergeFileHandle activeFile : handle.getActiveFiles()) {
            activeFilesById.put(activeFile.dataFileId(), activeFile);
        }
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int dataColumnCount = handle.getColumns().size();
        int operationChannel = page.getChannelCount() - 3;
        int rowIdChannel = page.getChannelCount() - 1;

        for (int position = 0; position < page.getPositionCount(); position++) {
            int operation = TINYINT.getByte(page.getBlock(operationChannel), position);
            switch (operation) {
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> storeDelete(page.getBlock(rowIdChannel), position);
                case UPDATE_INSERT_OPERATION_NUMBER -> appendInsertRow(page, dataColumnCount, position);
                case INSERT_OPERATION_NUMBER, UPDATE_OPERATION_NUMBER ->
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported merge operation for DuckLake: " + operation);
                default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown merge operation: " + operation);
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return insertPageSink.getCompletedBytes();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            List<DuckLakeDeleteFileInfo> deleteFiles = writeDeleteFiles();
            Collection<Slice> insertedFragments = insertPageSink.finish().join();
            List<DuckLakeWrittenFileInfo> insertedFiles = new ArrayList<>(insertedFragments.size());
            JsonCodec<DuckLakeWrittenFileInfo> fileInfoCodec = JsonCodec.jsonCodec(DuckLakeWrittenFileInfo.class);
            for (Slice fragment : insertedFragments) {
                insertedFiles.add(fileInfoCodec.fromJson(fragment.getBytes()));
            }
            DuckLakeMergeFragment fragment = new DuckLakeMergeFragment(insertedFiles, deleteFiles);
            return CompletableFuture.completedFuture(ImmutableList.of(Slices.utf8Slice(MERGE_FRAGMENT_CODEC.toJson(fragment))));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to finish DuckLake merge", e);
        }
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }

    private void storeDelete(Block rowIdBlock, int position)
    {
        List<Block> fields = RowBlock.getRowFieldsFromBlock(rowIdBlock);
        long dataFileId = BIGINT.getLong(fields.get(0), position);
        String dataFilePath = VARCHAR.getSlice(fields.get(1), position).toStringUtf8();
        long rowPosition = BIGINT.getLong(fields.get(2), position);
        deleteRowsByFileId.computeIfAbsent(dataFileId, ignored -> new DeleteRows(dataFilePath)).positions.add(rowPosition);
    }

    private void appendInsertRow(Page page, int dataColumnCount, int position)
    {
        insertPageSink.appendPage(page.getSingleValuePage(position).getColumns(buildVisibleChannels(dataColumnCount)));
    }

    private int[] buildVisibleChannels(int dataColumnCount)
    {
        int[] channels = new int[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            channels[i] = i;
        }
        return channels;
    }

    private List<DuckLakeDeleteFileInfo> writeDeleteFiles()
            throws IOException
    {
        DuckLakeDeleteFileReader deleteFileReader = new DuckLakeDeleteFileReader(
                fileSystemFactory,
                io.trino.parquet.ParquetReaderOptions.defaultOptions(),
                connectionManager.getMetadataBaseDirectory());
        List<DuckLakeDeleteFileInfo> deleteFiles = new ArrayList<>();
        for (Map.Entry<Long, DeleteRows> entry : deleteRowsByFileId.entrySet()) {
            long dataFileId = entry.getKey();
            DeleteRows deleteRows = entry.getValue();
            DuckLakeMergeFileHandle activeFile = activeFilesById.get(dataFileId);
            if (activeFile == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Missing active file metadata for data_file_id=" + dataFileId);
            }

            if (activeFile.deleteFilePath().isPresent()) {
                long rowIdStart = activeFile.rowIdStart();
                deleteFileReader.readDeletedRowIds(session, activeFile.deleteFilePath().orElseThrow(), rowIdStart)
                        .forEach((long logicalRowId) -> deleteRows.positions.add(logicalRowId - rowIdStart));
            }

            List<Long> sortedPositions = new ArrayList<>(deleteRows.positions);
            sortedPositions.sort(Long::compareTo);
            deleteFiles.add(writeDeleteFile(dataFileId, sortedPositions));
        }
        return deleteFiles;
    }

    private DuckLakeDeleteFileInfo writeDeleteFile(long dataFileId, List<Long> positions)
            throws IOException
    {
        String fileName = "ducklake-" + UUID.randomUUID() + "-deletes.parquet";
        String tableDirectoryPath = buildTableDirectoryPath();
        String fullPath = tableDirectoryPath + fileName;
        Location location = toLocation(fullPath);
        fileSystem.createDirectory(toLocation(tableDirectoryPath));

        List<io.trino.spi.type.Type> types = List.of(BIGINT);
        List<String> columnNames = List.of("pos");
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columnNames, false, false);
        MessageType messageType = schemaConverter.getMessageType();
        Map<List<String>, io.trino.spi.type.Type> primitiveTypes = schemaConverter.getPrimitiveTypes();
        ParquetWriterOptions writerOptions = ParquetWriterOptions.builder().build();

        try (OutputStream outputStream = fileSystem.newOutputFile(location).create();
                ParquetWriter writer = new ParquetWriter(
                        outputStream,
                        messageType,
                        primitiveTypes,
                        writerOptions,
                        CompressionCodec.SNAPPY,
                        "trino-ducklake",
                        Optional.empty(),
                        Optional.empty())) {
            Page page = buildDeletePage(positions);
            writer.write(page);
            writer.close();
            return new DuckLakeDeleteFileInfo(dataFileId, fileName, positions.size(), writer.getWrittenBytes(), 0);
        }
    }

    private static Page buildDeletePage(List<Long> positions)
    {
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, positions.size());
        for (Long position : positions) {
            BIGINT.writeLong(positionBuilder, position);
        }
        return new Page(positionBuilder.build());
    }

    private String buildTableDirectoryPath()
    {
        StringBuilder fullPath = new StringBuilder(handle.getDataPath());
        if (handle.getSchemaPath() != null && !handle.getSchemaPath().isEmpty()) {
            fullPath.append(handle.getSchemaPath());
            if (!handle.getSchemaPath().endsWith("/")) {
                fullPath.append("/");
            }
        }
        if (handle.getTablePath() != null && !handle.getTablePath().isEmpty()) {
            fullPath.append(handle.getTablePath());
            if (!handle.getTablePath().endsWith("/")) {
                fullPath.append("/");
            }
        }
        return fullPath.toString();
    }

    private static Location toLocation(String path)
    {
        if (path.startsWith("/")) {
            return Location.of(Path.of(path).toUri().toString());
        }
        return Location.of(path);
    }

    private static final class DeleteRows
    {
        private final String dataFilePath;
        private final java.util.Set<Long> positions = new java.util.LinkedHashSet<>();

        private DeleteRows(String dataFilePath)
        {
            this.dataFilePath = dataFilePath;
        }
    }
}
