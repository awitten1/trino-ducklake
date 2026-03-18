package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DuckLakeMetadata
        implements ConnectorMetadata
{
    private static final JsonCodec<DuckLakeWrittenFileInfo> FILE_INFO_CODEC = JsonCodec.jsonCodec(DuckLakeWrittenFileInfo.class);
    private static final JsonCodec<DuckLakeMergeFragment> MERGE_FRAGMENT_CODEC = JsonCodec.jsonCodec(DuckLakeMergeFragment.class);

    private final DuckLakeClient duckLakeClient;

    @Inject
    public DuckLakeMetadata(DuckLakeClient duckLakeClient)
    {
        this.duckLakeClient = requireNonNull(duckLakeClient, "duckLakeClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return duckLakeClient.getSchemaNames();
    }

    @Override
    public DuckLakeTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return duckLakeClient.getTableInfoWithSnapshot(tableName.getSchemaName(), tableName.getTableName())
                .map(tis -> new DuckLakeTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        tis.tableInfo().tableId(),
                        tis.snapshotId(),
                        io.trino.spi.predicate.TupleDomain.all()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        return getTableMetadata(handle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return duckLakeClient.listTables(optionalSchemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle duckLakeTableHandle = (DuckLakeTableHandle) tableHandle;

        List<DuckLakeColumnInfo> columnInfos = duckLakeClient.getColumnInfos(
                duckLakeTableHandle.getTableId(),
                duckLakeTableHandle.getSnapshotId());

        if (columnInfos == null) {
            throw new TableNotFoundException(duckLakeTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (DuckLakeColumnInfo info : columnInfos) {
            columnHandles.put(
                    info.columnName(),
                    new DuckLakeColumnHandle(
                            info.columnName(),
                            DuckLakeTypeMapping.toTrinoType(info.columnType()),
                            index,
                            info.columnId()));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        if (columnHandle instanceof DuckLakeMergeRowIdHandle) {
            return new ColumnMetadata("$ducklake_merge_row_id", DuckLakeMergeRowIdHandle.TYPE);
        }
        return ((DuckLakeColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        Map<ColumnHandle, io.trino.spi.predicate.Domain> supported = constraint.getSummary().getDomains()
                .map(domains -> domains.entrySet().stream()
                        .filter(entry -> entry.getKey() instanceof DuckLakeColumnHandle columnHandle && DuckLakeDomainUtils.isSupportedPredicateType(columnHandle.getColumnType()))
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))
                .orElseGet(ImmutableMap::of);
        io.trino.spi.predicate.TupleDomain<ColumnHandle> newConstraint = handle.getConstraint()
                .intersect(io.trino.spi.predicate.TupleDomain.withColumnDomains(supported));
        if (newConstraint.equals(handle.getConstraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(newConstraint),
                io.trino.spi.predicate.TupleDomain.all(),
                constraint.getExpression(),
                false));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return duckLakeClient.getTableStatistics((DuckLakeTableHandle) tableHandle);
    }

    // -------------------------------------------------------------------------
    // Schema DDL
    // -------------------------------------------------------------------------

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        duckLakeClient.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        duckLakeClient.dropSchema(schemaName, cascade);
    }

    // -------------------------------------------------------------------------
    // Table DDL
    // -------------------------------------------------------------------------

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        if (saveMode == SaveMode.IGNORE) {
            // Check if table exists; if so, silently return
            if (duckLakeClient.getTableInfoWithSnapshot(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
                return;
            }
        }
        duckLakeClient.createTable(tableName.getSchemaName(), tableName.getTableName(), tableMetadata.getColumns());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        duckLakeClient.dropTable((DuckLakeTableHandle) tableHandle);
    }

    // -------------------------------------------------------------------------
    // INSERT
    // -------------------------------------------------------------------------

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeClient.InsertContext ctx = duckLakeClient.getInsertContext(handle);
        return new DuckLakeInsertTableHandle(
                handle.getTableId(),
                ctx.columns(),
                ctx.dataPath(),
                ctx.schemaPath(),
                ctx.tablePath());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DuckLakeInsertTableHandle handle = (DuckLakeInsertTableHandle) insertHandle;
        List<DuckLakeWrittenFileInfo> files = parseFragments(fragments);
        duckLakeClient.finishInsert(handle.getTableId(), files);
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new DuckLakeMergeRowIdHandle("ducklake-merge-row-id");
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeClient.InsertContext ctx = duckLakeClient.getInsertContext(handle);
        List<DuckLakeMergeFileHandle> activeFiles = duckLakeClient.getDataFiles(handle, io.trino.spi.predicate.TupleDomain.all()).stream()
                .map(file -> new DuckLakeMergeFileHandle(
                        file.dataFileId(),
                        file.dataFilePath(),
                        file.deleteFilePath(),
                        file.rowIdStart()))
                .toList();
        return new DuckLakeMergeTableHandle(
                handle,
                ctx.columns(),
                ctx.dataPath(),
                ctx.schemaPath(),
                ctx.tablePath(),
                activeFiles);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DuckLakeMergeTableHandle handle = (DuckLakeMergeTableHandle) mergeTableHandle;
        List<DuckLakeWrittenFileInfo> insertedFiles = new ArrayList<>();
        List<DuckLakeDeleteFileInfo> deleteFiles = new ArrayList<>();
        for (Slice fragment : fragments) {
            DuckLakeMergeFragment mergeFragment = MERGE_FRAGMENT_CODEC.fromJson(fragment.getBytes());
            insertedFiles.addAll(mergeFragment.insertedFiles());
            deleteFiles.addAll(mergeFragment.deleteFiles());
        }
        duckLakeClient.finishMerge(handle.getDuckLakeTableHandle(), insertedFiles, deleteFiles);
    }

    // -------------------------------------------------------------------------
    // CREATE TABLE AS SELECT (CTAS)
    // -------------------------------------------------------------------------

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName tableName = tableMetadata.getTable();
        if (duckLakeClient.getTableInfoWithSnapshot(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
        }
        String dataPath = duckLakeClient.getDataPath();
        String schemaPath = duckLakeClient.getSchemaPath(tableName.getSchemaName());
        List<DuckLakeColumnHandle> temporaryColumns = new ArrayList<>();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);
            temporaryColumns.add(new DuckLakeColumnHandle(
                    column.getName(),
                    column.getType(),
                    i,
                    i));
        }

        return new DuckLakeOutputTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                temporaryColumns,
                0,
                0, // snapshotId not needed for CTAS handle
                dataPath,
                schemaPath,
                tableName.getTableName());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DuckLakeOutputTableHandle handle = (DuckLakeOutputTableHandle) tableHandle;
        List<DuckLakeWrittenFileInfo> files = parseFragments(fragments);
        duckLakeClient.createTableAndInsert(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumns().stream()
                        .map(DuckLakeColumnHandle::getColumnMetadata)
                        .toList(),
                files);
        return Optional.empty();
    }

    private List<DuckLakeWrittenFileInfo> parseFragments(Collection<Slice> fragments)
    {
        List<DuckLakeWrittenFileInfo> files = new ArrayList<>();
        for (Slice fragment : fragments) {
            files.add(FILE_INFO_CODEC.fromJson(fragment.getBytes()));
        }
        return files;
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = duckLakeClient.getColumns(tableName.getSchemaName(), tableName.getTableName());
        if (columns == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }
}
