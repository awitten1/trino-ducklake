package io.trino.plugin.ducklake;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DuckLakeDeleteFileReader
{
    private static final DateTimeZone UTC = DateTimeZone.UTC;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ParquetReaderOptions parquetReaderOptions;
    private final Optional<String> metadataBaseDirectory;

    public DuckLakeDeleteFileReader(
            TrinoFileSystemFactory fileSystemFactory,
            ParquetReaderOptions parquetReaderOptions,
            String metadataBaseDirectory)
    {
        this.fileSystemFactory = fileSystemFactory;
        this.parquetReaderOptions = parquetReaderOptions;
        this.metadataBaseDirectory = Optional.ofNullable(metadataBaseDirectory);
    }

    public LongOpenHashSet readDeletedRowIds(
            ConnectorSession session,
            String deleteFilePath,
            String dataFilePath,
            long rowIdStart)
            throws IOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Location location = toLocation(deleteFilePath);
        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        String normalizedDataFileLocation = normalizeDataFileLocation(dataFilePath);
        ParquetDataSource dataSource = new DuckLakeParquetDataSource(inputFile, parquetReaderOptions);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();
            org.apache.parquet.schema.Type filePathType = getParquetTypeByName("file_path", fileSchema);
            org.apache.parquet.schema.Type posType = getParquetTypeByName("pos", fileSchema);
            if (filePathType == null || posType == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Delete file is missing expected file_path/pos columns: " + deleteFilePath);
            }

            MessageType requestedSchema = new MessageType(fileSchema.getName(), filePathType, posType);
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, io.trino.spi.predicate.TupleDomain.all(), descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata,
                    List.of(io.trino.spi.predicate.TupleDomain.all()),
                    List.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    DOMAIN_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            ColumnIO filePathColumnIo = lookupColumnByName(messageColumn, "file_path");
            ColumnIO posColumnIo = lookupColumnByName(messageColumn, "pos");
            Field filePathField = constructField(VARCHAR, filePathColumnIo)
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported file_path column in delete file: " + deleteFilePath));
            Field posField = constructField(BIGINT, posColumnIo)
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported pos column in delete file: " + deleteFilePath));
            ParquetReader reader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    List.of(
                            new Column("file_path", filePathField),
                            new Column("pos", posField)),
                    false,
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> new TrinoException(GENERIC_INTERNAL_ERROR, "Failed reading delete file " + deleteFilePath, exception),
                    Optional.of(parquetPredicate),
                    Optional.empty(),
                    parquetMetadata.getDecryptionContext());

            try (reader) {
                LongOpenHashSet deletedRows = new LongOpenHashSet();
                while (true) {
                    SourcePage page = reader.nextPage();
                    if (page == null) {
                        break;
                    }
                    Block filePathBlock = page.getBlock(0);
                    Block posBlock = page.getBlock(1);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        String deleteFileTarget = VARCHAR.getSlice(filePathBlock, position).toStringUtf8();
                        if (normalizeDeleteTargetLocation(deleteFileTarget).equals(normalizedDataFileLocation)) {
                            deletedRows.add(rowIdStart + BIGINT.getLong(posBlock, position));
                        }
                    }
                }
                return deletedRows;
            }
        }
        finally {
            try {
                dataSource.close();
            }
            finally {
                memoryContext.close();
            }
        }
    }

    private static Location toLocation(String path)
    {
        if (path.startsWith("/")) {
            return Location.of(Path.of(path).toUri().toString());
        }
        return Location.of(path);
    }

    private String normalizeDataFileLocation(String dataFilePath)
    {
        return normalizeLocation(toLocation(dataFilePath));
    }

    private String normalizeDeleteTargetLocation(String deleteFileTarget)
    {
        if (deleteFileTarget.startsWith("/") || deleteFileTarget.contains(":")) {
            return normalizeLocation(toLocation(deleteFileTarget));
        }
        if (metadataBaseDirectory.isPresent()) {
            return normalizeLocation(toLocation(Path.of(metadataBaseDirectory.orElseThrow(), deleteFileTarget).toString()));
        }
        return deleteFileTarget;
    }

    private static String normalizeLocation(Location location)
    {
        return location.toString();
    }
}
