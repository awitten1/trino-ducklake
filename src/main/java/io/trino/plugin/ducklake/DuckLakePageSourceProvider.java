package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DuckLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final DateTimeZone UTC = DateTimeZone.UTC;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ParquetReaderOptions parquetReaderOptions;
    private final DuckLakeConnectionManager connectionManager;

    @Inject
    public DuckLakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory, DuckLakeConnectionManager connectionManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.parquetReaderOptions = ParquetReaderOptions.defaultOptions();
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        DuckLakeSplit duckLakeSplit = (DuckLakeSplit) split;
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) table;
        List<DuckLakeColumnHandle> duckLakeColumns = columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .toList();
        List<DuckLakeColumnHandle> readColumns = buildReadColumns(duckLakeColumns, tableHandle.getConstraint());
        Map<Integer, Domain> filterDomainsByChannel = buildFilterDomainsByChannel(readColumns, tableHandle.getConstraint());

        if (duckLakeColumns.isEmpty() && duckLakeSplit.getDeleteFilePath().isEmpty() && filterDomainsByChannel.isEmpty()) {
            return new FixedPageSource(emptyPages(duckLakeSplit.getRecordCount()));
        }

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            DuckLakeParquetPageSource basePageSource = createParquetPageSource(
                    fileSystem,
                    duckLakeSplit.getDataFilePath(),
                    readColumns,
                    duckLakeSplit.getDeleteFilePath().isPresent());
            ConnectorPageSource pageSource;

            if (duckLakeSplit.getDeleteFilePath().isEmpty()) {
                pageSource = basePageSource;
            }
            else {
                DuckLakeDeleteFileReader deleteFileReader = new DuckLakeDeleteFileReader(
                        fileSystemFactory,
                        parquetReaderOptions,
                        connectionManager.getMetadataBaseDirectory());
                pageSource = new DuckLakeDeleteFilteringPageSource(
                        basePageSource,
                        deleteFileReader.readDeletedRowIds(
                                session,
                                duckLakeSplit.getDeleteFilePath().orElseThrow(),
                                duckLakeSplit.getDataFilePath(),
                                duckLakeSplit.getRowIdStart()),
                        duckLakeSplit.getRowIdStart(),
                        readColumns.size());
            }

            if (!filterDomainsByChannel.isEmpty() || readColumns.size() != duckLakeColumns.size()) {
                pageSource = new DuckLakeConstraintFilteringPageSource(
                        pageSource,
                        readColumns,
                        duckLakeColumns.size(),
                        filterDomainsByChannel);
            }
            return pageSource;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create page source for " + duckLakeSplit.getDataFilePath(), e);
        }
    }

    private static List<DuckLakeColumnHandle> buildReadColumns(List<DuckLakeColumnHandle> visibleColumns, TupleDomain<ColumnHandle> constraint)
    {
        LinkedHashMap<String, DuckLakeColumnHandle> readColumns = new LinkedHashMap<>();
        for (DuckLakeColumnHandle column : visibleColumns) {
            readColumns.put(column.getColumnName(), column);
        }
        constraint.getDomains().ifPresent(domains -> domains.keySet().stream()
                .map(DuckLakeColumnHandle.class::cast)
                .filter(column -> DuckLakeDomainUtils.isSupportedPredicateType(column.getColumnType()))
                .forEach(column -> readColumns.putIfAbsent(column.getColumnName(), column)));
        return List.copyOf(readColumns.values());
    }

    private static Map<Integer, Domain> buildFilterDomainsByChannel(List<DuckLakeColumnHandle> readColumns, TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.isAll() || constraint.isNone()) {
            return Map.of();
        }

        Map<Integer, Domain> domainsByChannel = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow();
        for (int channel = 0; channel < readColumns.size(); channel++) {
            DuckLakeColumnHandle column = readColumns.get(channel);
            Domain domain = domains.get(column);
            if (domain != null && DuckLakeDomainUtils.isSupportedPredicateType(column.getColumnType())) {
                domainsByChannel.put(channel, domain);
            }
        }
        return Map.copyOf(domainsByChannel);
    }

    private DuckLakeParquetPageSource createParquetPageSource(
            TrinoFileSystem fileSystem,
            String dataFilePath,
            List<DuckLakeColumnHandle> duckLakeColumns,
            boolean appendRowNumberColumn)
            throws IOException
    {
        Location location = toLocation(dataFilePath);
        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        ParquetDataSource dataSource = new DuckLakeParquetDataSource(inputFile, parquetReaderOptions);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetadata.getSchema();

        MessageType requestedSchema = buildRequestedSchema(fileSchema, duckLakeColumns);
        MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
        TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.all();
        TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);
        List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                0,
                inputFile.length(),
                dataSource,
                parquetMetadata,
                ImmutableList.of(parquetTupleDomain),
                ImmutableList.of(parquetPredicate),
                descriptorsByPath,
                UTC,
                DOMAIN_COMPACTION_THRESHOLD,
                parquetReaderOptions);

        List<Column> parquetColumns = buildParquetColumns(messageColumn, duckLakeColumns);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = new ParquetReader(
                Optional.ofNullable(fileMetadata.getCreatedBy()),
                parquetColumns,
                appendRowNumberColumn,
                rowGroups,
                dataSource,
                UTC,
                memoryContext,
                parquetReaderOptions,
                exception -> new TrinoException(GENERIC_INTERNAL_ERROR, "Failed reading Parquet file " + dataFilePath, exception),
                Optional.of(parquetPredicate),
                Optional.empty(),
                parquetMetadata.getDecryptionContext());

        return new DuckLakeParquetPageSource(reader, dataSource, memoryContext);
    }

    private static Location toLocation(String path)
    {
        if (path.startsWith("/")) {
            return Location.of(Path.of(path).toUri().toString());
        }
        if (path.startsWith("file:") || path.startsWith("local:")) {
            return Location.of(path);
        }
        throw new TrinoException(NOT_SUPPORTED, "Only local DuckLake data files are currently supported: " + path);
    }

    private static MessageType buildRequestedSchema(MessageType fileSchema, List<DuckLakeColumnHandle> columns)
    {
        ImmutableList.Builder<org.apache.parquet.schema.Type> fields = ImmutableList.builder();
        for (DuckLakeColumnHandle column : columns) {
            org.apache.parquet.schema.Type parquetType = getParquetTypeByName(column.getColumnName(), fileSchema);
            if (parquetType == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Column not found in Parquet file: " + column.getColumnName());
            }
            fields.add(parquetType);
        }
        return new MessageType(fileSchema.getName(), fields.build());
    }

    private static List<Column> buildParquetColumns(MessageColumnIO messageColumn, List<DuckLakeColumnHandle> columns)
    {
        ImmutableList.Builder<Column> parquetColumns = ImmutableList.builder();
        for (DuckLakeColumnHandle column : columns) {
            ColumnIO columnIo = lookupColumnByName(messageColumn, column.getColumnName());
            if (columnIo == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Column not found in projected Parquet schema: " + column.getColumnName());
            }
            Field field = constructField(column.getColumnType(), columnIo)
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported Parquet field for column: " + column.getColumnName()));
            parquetColumns.add(new Column(column.getColumnName(), field));
        }
        return parquetColumns.build();
    }

    private static List<Page> emptyPages(long rowCount)
    {
        List<Page> pages = new ArrayList<>();
        long remaining = rowCount;
        while (remaining > 0) {
            int batchSize = toIntExact(Math.min(remaining, 8_192));
            pages.add(new Page(batchSize));
            remaining -= batchSize;
        }
        return pages;
    }
}
