package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.BooleanType;

import io.trino.spi.connector.ConnectorTableMetadata;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class DuckLakeClient
{
    private final DuckLakeConnectionManager connectionManager;

    @Inject
    public DuckLakeClient(DuckLakeConnectionManager connectionManager)
    {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    // These queries are taken from https://ducklake.select/docs/stable/specification/queries

    // -------------------------------------------------------------------------
    // Snapshot
    // -------------------------------------------------------------------------

    private long getCurrentSnapshotId(Connection conn)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                """
                SELECT snapshot_id
                FROM ducklake_snapshot
                WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)
                """)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long id = rs.getLong(1);
                    if (rs.wasNull()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "DuckLake catalog has no snapshots");
                    }
                    return id;
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "DuckLake catalog has no snapshots");
            }
        }
    }

    private String queryDataPath(Connection conn)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT value FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL")) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String path = rs.getString(1);
                    // Resolve relative paths against the directory containing the metadata file
                    if (!path.startsWith("/") && !path.contains(":")) {
                        String baseDir = connectionManager.getMetadataBaseDirectory();
                        if (baseDir != null) {
                            path = baseDir + path;
                        }
                    }
                    if (!path.endsWith("/")) {
                        path = path + "/";
                    }
                    return path;
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "No data_path entry in ducklake_metadata");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Schemas
    // -------------------------------------------------------------------------

    public List<String> getSchemaNames()
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT schema_name FROM ducklake_schema " +
                    "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                stmt.setLong(1, snapshotId);
                stmt.setLong(2, snapshotId);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<String> schemas = new ArrayList<>();
                    while (rs.next()) {
                        schemas.add(rs.getString(1));
                    }
                    return schemas;
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list schemas", e);
        }
    }

    // -------------------------------------------------------------------------
    // Tables
    // -------------------------------------------------------------------------

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            String sql = """
                    SELECT s.schema_name, t.table_name
                    FROM ducklake_table t
                    JOIN ducklake_schema s ON t.schema_id = s.schema_id
                    WHERE (? IS NULL OR s.schema_name = ?)
                      AND ? >= t.begin_snapshot AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
                      AND ? >= s.begin_snapshot AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
                    """;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, schemaName.orElse(null));
                stmt.setString(2, schemaName.orElse(null));
                stmt.setLong(3, snapshotId);
                stmt.setLong(4, snapshotId);
                stmt.setLong(5, snapshotId);
                stmt.setLong(6, snapshotId);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<SchemaTableName> tables = new ArrayList<>();
                    while (rs.next()) {
                        tables.add(new SchemaTableName(rs.getString(1), rs.getString(2)));
                    }
                    return tables;
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list tables", e);
        }
    }

    /**
     * Returns both tableInfo and the snapshotId together, so callers capture a
     * consistent snapshot in a single connection.
     */
    public Optional<TableInfoWithSnapshot> getTableInfoWithSnapshot(String schemaName, String tableName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            return queryTableInfo(conn, schemaName, tableName, snapshotId)
                    .map(info -> new TableInfoWithSnapshot(info, snapshotId));
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get table info", e);
        }
    }

    record TableInfoWithSnapshot(DuckLakeTableInfo tableInfo, long snapshotId) {}

    private Optional<DuckLakeTableInfo> queryTableInfo(Connection conn, String schemaName, String tableName, long snapshotId)
            throws SQLException
    {
        String sql = """
                SELECT t.table_id, t.schema_id, s.schema_name,
                       s.path AS schema_path, s.path_is_relative AS schema_path_is_relative,
                       t.path AS table_path, t.path_is_relative AS table_path_is_relative
                FROM ducklake_table t
                JOIN ducklake_schema s ON t.schema_id = s.schema_id
                WHERE s.schema_name = ? AND t.table_name = ?
                  AND ? >= t.begin_snapshot AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
                  AND ? >= s.begin_snapshot AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);
            stmt.setLong(6, snapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DuckLakeTableInfo(
                            rs.getLong("table_id"),
                            rs.getLong("schema_id"),
                            rs.getString("schema_name"),
                            tableName,
                            rs.getString("schema_path"),
                            rs.getBoolean("schema_path_is_relative"),
                            rs.getString("table_path"),
                            rs.getBoolean("table_path_is_relative")));
                }
                return Optional.empty();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Columns
    // -------------------------------------------------------------------------

    /**
     * Returns ColumnMetadata list for use in ConnectorTableMetadata (schema display).
     */
    public List<ColumnMetadata> getColumns(String schemaName, String tableName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            Optional<DuckLakeTableInfo> tableInfo = queryTableInfo(conn, schemaName, tableName, snapshotId);
            if (tableInfo.isEmpty()) {
                return null;
            }
            return queryColumnInfos(conn, tableInfo.get().tableId(), snapshotId).stream()
                    .map(info -> new ColumnMetadata(info.columnName(), DuckLakeTypeMapping.toTrinoType(info.columnType())))
                    .toList();
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get columns", e);
        }
    }

    /**
     * Returns full column info including columnId, used when building column handles.
     */
    public List<DuckLakeColumnInfo> getColumnInfos(long tableId, long snapshotId)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            return queryColumnInfos(conn, tableId, snapshotId);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get column infos", e);
        }
    }

    private List<DuckLakeColumnInfo> queryColumnInfos(Connection conn, long tableId, long snapshotId)
            throws SQLException
    {
        String sql = """
                SELECT column_id, column_order, column_name, column_type, nulls_allowed
                FROM ducklake_column
                WHERE table_id = ? AND parent_column IS NULL
                  AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
                ORDER BY column_order
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                List<DuckLakeColumnInfo> columns = new ArrayList<>();
                while (rs.next()) {
                    columns.add(new DuckLakeColumnInfo(
                            rs.getLong("column_id"),
                            rs.getInt("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getBoolean("nulls_allowed")));
                }
                return columns;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Data files
    // -------------------------------------------------------------------------

    public List<DuckLakeDataFileInfo> getDataFiles(DuckLakeTableHandle tableHandle, TupleDomain<ColumnHandle> constraint)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            String dataPath = queryDataPath(conn);
            long tableId = tableHandle.getTableId();
            long snapshotId = tableHandle.getSnapshotId();
            DuckLakeTableInfo tableInfo = queryTableInfo(conn, tableHandle.getSchemaName(), tableHandle.getTableName(), snapshotId)
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Table not found: " + tableHandle.toSchemaTableName()));
            Optional<Set<Long>> allowedDataFileIds = getAllowedDataFileIds(conn, tableId, constraint);
            if (allowedDataFileIds.isPresent() && allowedDataFileIds.orElseThrow().isEmpty()) {
                return List.of();
            }
            String sql = """
                    SELECT df.data_file_id, df.path, df.path_is_relative,
                           df.row_id_start, df.record_count,
                           del.path AS delete_path, del.path_is_relative AS delete_path_is_relative
                    FROM ducklake_data_file df
                    LEFT JOIN (
                        SELECT data_file_id, path, path_is_relative
                        FROM ducklake_delete_file
                        WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
                    ) del ON df.data_file_id = del.data_file_id
                    WHERE df.table_id = ?
                      AND ? >= df.begin_snapshot AND (? < df.end_snapshot OR df.end_snapshot IS NULL)
                    ORDER BY df.file_order
                    """;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, snapshotId);
                stmt.setLong(2, snapshotId);
                stmt.setLong(3, tableId);
                stmt.setLong(4, snapshotId);
                stmt.setLong(5, snapshotId);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<DuckLakeDataFileInfo> files = new ArrayList<>();
                    while (rs.next()) {
                        long dataFileId = rs.getLong("data_file_id");
                        if (allowedDataFileIds.isPresent() && !allowedDataFileIds.orElseThrow().contains(dataFileId)) {
                            continue;
                        }
                        String filePath = rs.getString("path");
                        boolean fileRelative = rs.getBoolean("path_is_relative");
                        String resolvedData = resolvePath(
                                dataPath,
                                tableInfo.schemaPath(), tableInfo.schemaPathIsRelative(),
                                tableInfo.tablePath(), tableInfo.tablePathIsRelative(),
                                filePath, fileRelative);

                        String deletePath = rs.getString("delete_path");
                        Optional<String> resolvedDelete = Optional.empty();
                        if (deletePath != null) {
                            boolean deleteRelative = rs.getBoolean("delete_path_is_relative");
                            resolvedDelete = Optional.of(resolvePath(
                                    dataPath,
                                    tableInfo.schemaPath(), tableInfo.schemaPathIsRelative(),
                                    tableInfo.tablePath(), tableInfo.tablePathIsRelative(),
                                    deletePath, deleteRelative));
                        }

                        files.add(new DuckLakeDataFileInfo(
                                dataFileId,
                                resolvedData,
                                resolvedDelete,
                                rs.getLong("row_id_start"),
                                rs.getLong("record_count")));
                    }
                    return files;
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get data files", e);
        }
    }

    public TableStatistics getTableStatistics(DuckLakeTableHandle tableHandle)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            TableStatistics.Builder builder = TableStatistics.builder();
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?")) {
                stmt.setLong(1, tableHandle.getTableId());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        builder.setRowCount(Estimate.of(rs.getLong("record_count")));
                    }
                }
            }

            Map<String, DuckLakeColumnHandle> columnHandles = new HashMap<>();
            int index = 0;
            for (DuckLakeColumnInfo info : queryColumnInfos(conn, tableHandle.getTableId(), tableHandle.getSnapshotId())) {
                columnHandles.put(info.columnName(), new DuckLakeColumnHandle(
                        info.columnName(),
                        DuckLakeTypeMapping.toTrinoType(info.columnType()),
                        index++,
                        info.columnId()));
            }

            String sql = """
                    SELECT c.column_name, c.column_type, s.contains_null, s.min_value, s.max_value
                    FROM ducklake_table_column_stats s
                    JOIN ducklake_column c ON s.table_id = c.table_id AND s.column_id = c.column_id
                    WHERE s.table_id = ? AND c.parent_column IS NULL
                      AND ? >= c.begin_snapshot AND (? < c.end_snapshot OR c.end_snapshot IS NULL)
                    """;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, tableHandle.getTableId());
                stmt.setLong(2, tableHandle.getSnapshotId());
                stmt.setLong(3, tableHandle.getSnapshotId());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        DuckLakeColumnHandle columnHandle = columnHandles.get(rs.getString("column_name"));
                        if (columnHandle == null) {
                            continue;
                        }
                        ColumnStatistics.Builder columnStats = ColumnStatistics.builder();
                        boolean containsNull = rs.getBoolean("contains_null");
                        if (!rs.wasNull() && !containsNull) {
                            columnStats.setNullsFraction(Estimate.of(0.0));
                        }
                        buildDoubleRange(columnHandle.getColumnType(), rs.getString("min_value"), rs.getString("max_value"))
                                .ifPresent(columnStats::setRange);
                        builder.setColumnStatistics(columnHandle, columnStats.build());
                    }
                }
            }
            return builder.build();
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get table statistics", e);
        }
    }

    private Optional<Set<Long>> getAllowedDataFileIds(Connection conn, long tableId, TupleDomain<ColumnHandle> constraint)
            throws SQLException
    {
        if (constraint.isAll() || constraint.isNone()) {
            return constraint.isNone() ? Optional.of(Set.of()) : Optional.empty();
        }

        Set<Long> allowed = null;
        String sql = """
                SELECT data_file_id, min_value, max_value, null_count, value_count
                FROM ducklake_file_column_stats
                WHERE table_id = ? AND column_id = ?
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().orElseThrow().entrySet()) {
                if (!(entry.getKey() instanceof DuckLakeColumnHandle columnHandle)) {
                    continue;
                }
                Domain domain = entry.getValue();
                stmt.setLong(1, tableId);
                stmt.setLong(2, columnHandle.getColumnId());
                Set<Long> matchingForColumn = new HashSet<>();
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        if (mayMatch(domain, rs.getString("min_value"), rs.getString("max_value"), rs.getLong("null_count"), rs.getLong("value_count"))) {
                            matchingForColumn.add(rs.getLong("data_file_id"));
                        }
                    }
                }
                if (allowed == null) {
                    allowed = matchingForColumn;
                }
                else {
                    allowed.retainAll(matchingForColumn);
                }
                if (allowed.isEmpty()) {
                    return Optional.of(Set.of());
                }
            }
        }
        return Optional.ofNullable(allowed);
    }

    private boolean mayMatch(Domain domain, String minValue, String maxValue, long nullCount, long valueCount)
    {
        if (domain.isOnlyNull()) {
            return nullCount > 0;
        }
        if (domain.isNullAllowed() && nullCount > 0) {
            return true;
        }
        if (minValue == null || maxValue == null) {
            return true;
        }
        Optional<Domain> statsDomain = buildStatsDomain(domain.getType(), minValue, maxValue);
        return statsDomain.isEmpty() || domain.overlaps(statsDomain.orElseThrow());
    }

    private Optional<Domain> buildStatsDomain(Type type, String minValue, String maxValue)
    {
        Optional<Object> min = parseStatValue(type, minValue);
        Optional<Object> max = parseStatValue(type, maxValue);
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Domain.create(
                io.trino.spi.predicate.ValueSet.ofRanges(Range.range(type, min.orElseThrow(), true, max.orElseThrow(), true)),
                false));
    }

    private Optional<DoubleRange> buildDoubleRange(Type type, String minValue, String maxValue)
    {
        Optional<Object> min = parseStatValue(type, minValue);
        Optional<Object> max = parseStatValue(type, maxValue);
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }
        return toDouble(type, min.orElseThrow())
                .flatMap(low -> toDouble(type, max.orElseThrow()).map(high -> new DoubleRange(low, high)));
    }

    private Optional<Object> parseStatValue(Type type, String value)
    {
        if (value == null) {
            return Optional.empty();
        }
        try {
            if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType || type instanceof DateType) {
                return Optional.of(Long.parseLong(value));
            }
            if (type instanceof RealType) {
                return Optional.of((long) Float.floatToIntBits(Float.parseFloat(value)));
            }
            if (type instanceof DoubleType) {
                return Optional.of(Double.parseDouble(value));
            }
            if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = new BigDecimal(value);
                if (decimalType.isShort()) {
                    return Optional.of(decimal.movePointRight(decimalType.getScale()).longValueExact());
                }
                return Optional.empty();
            }
            if (type instanceof VarcharType) {
                return Optional.of(io.airlift.slice.Slices.utf8Slice(value));
            }
        }
        catch (RuntimeException ignored) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<Double> toDouble(Type type, Object value)
    {
        if (value instanceof Long longValue) {
            if (type instanceof RealType) {
                return Optional.of((double) Float.intBitsToFloat(longValue.intValue()));
            }
            return Optional.of((double) longValue);
        }
        if (value instanceof Double doubleValue) {
            return Optional.of(doubleValue);
        }
        return Optional.empty();
    }

    public String getDataPath()
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            return queryDataPath(conn);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get data path", e);
        }
    }

    public String getSchemaPath(String schemaName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long currentSnapshotId = getCurrentSnapshotId(conn);
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT path FROM ducklake_schema WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                stmt.setString(1, schemaName);
                stmt.setLong(2, currentSnapshotId);
                stmt.setLong(3, currentSnapshotId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new TrinoException(SCHEMA_NOT_FOUND, "Schema does not exist: " + schemaName);
                    }
                    return rs.getString("path");
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get schema path: " + schemaName, e);
        }
    }

    // -------------------------------------------------------------------------
    // Write operations
    // -------------------------------------------------------------------------

    record SnapshotInfo(long snapshotId, long nextCatalogId, long nextFileId, long schemaVersion) {}

    private SnapshotInfo createSnapshot(Connection conn, String changes, long catalogIdIncrement, long fileIdIncrement, boolean schemaChanged)
            throws SQLException
    {
        // Read current snapshot info
        String selectSql = connectionManager.isPostgresql()
                ? "SELECT snapshot_id, next_catalog_id, next_file_id, schema_version FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot) FOR UPDATE"
                : "SELECT snapshot_id, next_catalog_id, next_file_id, schema_version FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)";

        long currentSnapshotId;
        long nextCatalogId;
        long nextFileId;
        long schemaVersion;

        try (PreparedStatement stmt = conn.prepareStatement(selectSql);
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "DuckLake catalog has no snapshots");
            }
            currentSnapshotId = rs.getLong("snapshot_id");
            nextCatalogId = rs.getLong("next_catalog_id");
            nextFileId = rs.getLong("next_file_id");
            schemaVersion = rs.getLong("schema_version");
        }

        long newSnapshotId = currentSnapshotId + 1;
        long newNextCatalogId = nextCatalogId + catalogIdIncrement;
        long newNextFileId = nextFileId + fileIdIncrement;
        long newSchemaVersion = schemaChanged ? schemaVersion + 1 : schemaVersion;

        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_snapshot (snapshot_time, snapshot_id, schema_version, next_catalog_id, next_file_id) VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?)")) {
            stmt.setLong(1, newSnapshotId);
            stmt.setLong(2, newSchemaVersion);
            stmt.setLong(3, newNextCatalogId);
            stmt.setLong(4, newNextFileId);
            stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES (?, ?)")) {
            stmt.setLong(1, newSnapshotId);
            stmt.setString(2, changes);
            stmt.executeUpdate();
        }

        return new SnapshotInfo(newSnapshotId, nextCatalogId, nextFileId, newSchemaVersion);
    }

    // -------------------------------------------------------------------------
    // Schema DDL
    // -------------------------------------------------------------------------

    public void createSchema(String schemaName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                // Check if schema already exists
                long currentSnapshotId = getCurrentSnapshotId(conn);
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    stmt.setString(1, schemaName);
                    stmt.setLong(2, currentSnapshotId);
                    stmt.setLong(3, currentSnapshotId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            throw new TrinoException(ALREADY_EXISTS, "Schema already exists: " + schemaName);
                        }
                    }
                }

                SnapshotInfo snapshot = createSnapshot(conn, "CREATE_SCHEMA", 1, 0, true);
                long schemaId = snapshot.nextCatalogId();

                try (PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) VALUES (?, ?, ?, NULL, ?, ?, true)")) {
                    stmt.setLong(1, schemaId);
                    setUuid(stmt, 2, UUID.randomUUID());
                    stmt.setLong(3, snapshot.snapshotId());
                    stmt.setString(4, schemaName);
                    stmt.setString(5, schemaName + "/");
                    stmt.executeUpdate();
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create schema: " + schemaName, e);
        }
    }

    public void dropSchema(String schemaName, boolean cascade)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                long currentSnapshotId = getCurrentSnapshotId(conn);

                // Find the schema
                long schemaId;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    stmt.setString(1, schemaName);
                    stmt.setLong(2, currentSnapshotId);
                    stmt.setLong(3, currentSnapshotId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new TrinoException(SCHEMA_NOT_FOUND, "Schema does not exist: " + schemaName);
                        }
                        schemaId = rs.getLong("schema_id");
                    }
                }

                // Check if schema has tables
                if (!cascade) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "SELECT table_id FROM ducklake_table WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1")) {
                        stmt.setLong(1, schemaId);
                        stmt.setLong(2, currentSnapshotId);
                        stmt.setLong(3, currentSnapshotId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema is not empty: " + schemaName);
                            }
                        }
                    }
                }

                SnapshotInfo snapshot = createSnapshot(conn, "DROP_SCHEMA", 0, 0, true);

                try (PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE ducklake_schema SET end_snapshot = ? WHERE schema_id = ? AND end_snapshot IS NULL")) {
                    stmt.setLong(1, snapshot.snapshotId());
                    stmt.setLong(2, schemaId);
                    stmt.executeUpdate();
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to drop schema: " + schemaName, e);
        }
    }

    // -------------------------------------------------------------------------
    // Table DDL
    // -------------------------------------------------------------------------

    public void createTable(String schemaName, String tableName, List<ColumnMetadata> columns)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                long currentSnapshotId = getCurrentSnapshotId(conn);

                // Find schema
                long schemaId;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    stmt.setString(1, schemaName);
                    stmt.setLong(2, currentSnapshotId);
                    stmt.setLong(3, currentSnapshotId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new TrinoException(SCHEMA_NOT_FOUND, "Schema does not exist: " + schemaName);
                        }
                        schemaId = rs.getLong("schema_id");
                    }
                }

                // Check if table already exists
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT table_id FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    stmt.setLong(1, schemaId);
                    stmt.setString(2, tableName);
                    stmt.setLong(3, currentSnapshotId);
                    stmt.setLong(4, currentSnapshotId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + schemaName + "." + tableName);
                        }
                    }
                }

                // Need 1 ID for table + 1 per column
                long catalogIdIncrement = 1 + columns.size();
                SnapshotInfo snapshot = createSnapshot(conn, "CREATE_TABLE", catalogIdIncrement, 0, true);
                long tableId = snapshot.nextCatalogId();

                createTableInternal(conn, tableId, schemaId, snapshot.snapshotId(), tableName, columns, snapshot.nextCatalogId() + 1);

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create table: " + schemaName + "." + tableName, e);
        }
    }

    /**
     * Creates a table and returns the tableId and column handles. Used by both createTable and beginCreateTable.
     */
    record CreateTableResult(long tableId, List<DuckLakeColumnHandle> columnHandles, String schemaPath, String tablePath) {}

    CreateTableResult createTableAndInsert(String schemaName, String tableName, List<ColumnMetadata> columns, List<DuckLakeWrittenFileInfo> files)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                long currentSnapshotId = getCurrentSnapshotId(conn);

                // Find schema
                long schemaId;
                String schemaPath;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT schema_id, path FROM ducklake_schema WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    stmt.setString(1, schemaName);
                    stmt.setLong(2, currentSnapshotId);
                    stmt.setLong(3, currentSnapshotId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new TrinoException(SCHEMA_NOT_FOUND, "Schema does not exist: " + schemaName);
                        }
                        schemaId = rs.getLong("schema_id");
                        schemaPath = rs.getString("path");
                    }
                }

                long catalogIdIncrement = 1 + columns.size();
                SnapshotInfo snapshot = createSnapshot(conn, "CREATE_TABLE", catalogIdIncrement, files.size(), true);
                long tableId = snapshot.nextCatalogId();
                long firstColumnId = snapshot.nextCatalogId() + 1;

                String tablePath = createTableInternal(conn, tableId, schemaId, snapshot.snapshotId(), tableName, columns, firstColumnId);

                // Build column handles
                List<DuckLakeColumnHandle> columnHandles = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    ColumnMetadata col = columns.get(i);
                    columnHandles.add(new DuckLakeColumnHandle(
                            col.getName(),
                            col.getType(),
                            i,
                            firstColumnId + i));
                }

                if (!files.isEmpty()) {
                    // Remap temporary ordinal column IDs to actual database column IDs
                    List<DuckLakeWrittenFileInfo> remappedFiles = remapColumnIds(files, firstColumnId);
                    registerInsertedFiles(conn, tableId, snapshot, remappedFiles);
                }

                conn.commit();
                return new CreateTableResult(tableId, columnHandles, schemaPath, tablePath);
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create table: " + schemaName + "." + tableName, e);
        }
    }

    private String createTableInternal(Connection conn, long tableId, long schemaId, long snapshotId, String tableName, List<ColumnMetadata> columns, long firstColumnId)
            throws SQLException
    {
        // Insert table — path must have trailing slash for correct concatenation with file paths
        String tablePath = tableName + "/";
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) VALUES (?, ?, ?, NULL, ?, ?, ?, true)")) {
            stmt.setLong(1, tableId);
            setUuid(stmt, 2, UUID.randomUUID());
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, schemaId);
            stmt.setString(5, tableName);
            stmt.setString(6, tablePath);
            stmt.executeUpdate();
        }

        // Insert columns
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type, default_value_dialect) VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL)")) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnMetadata col = columns.get(i);
                stmt.setLong(1, firstColumnId + i);
                stmt.setLong(2, snapshotId);
                stmt.setLong(3, tableId);
                stmt.setInt(4, i);
                stmt.setString(5, col.getName());
                stmt.setString(6, DuckLakeTypeMapping.toDuckLakeType(col.getType()));
                stmt.setBoolean(7, true);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }

        // Insert initial table stats
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) VALUES (?, 0, 0, 0)")) {
            stmt.setLong(1, tableId);
            stmt.executeUpdate();
        }

        // Insert initial column stats
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value) VALUES (?, ?, false, false, NULL, NULL)")) {
            for (int i = 0; i < columns.size(); i++) {
                stmt.setLong(1, tableId);
                stmt.setLong(2, firstColumnId + i);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }

        return tablePath;
    }

    public void dropTable(DuckLakeTableHandle tableHandle)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                SnapshotInfo snapshot = createSnapshot(conn, "DROP_TABLE", 0, 0, true);
                long tableId = tableHandle.getTableId();
                long snapshotId = snapshot.snapshotId();

                String[] tables = {
                        "ducklake_table",
                        "ducklake_partition_info",
                        "ducklake_column",
                        "ducklake_column_tag",
                        "ducklake_data_file",
                        "ducklake_delete_file"
                };
                for (String table : tables) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "UPDATE " + table + " SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                        stmt.setLong(1, snapshotId);
                        stmt.setLong(2, tableId);
                        stmt.executeUpdate();
                    }
                }
                try (PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE ducklake_tag SET end_snapshot = ? WHERE object_id = ? AND end_snapshot IS NULL")) {
                    stmt.setLong(1, snapshotId);
                    stmt.setLong(2, tableId);
                    stmt.executeUpdate();
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to drop table", e);
        }
    }

    // -------------------------------------------------------------------------
    // INSERT / CTAS - file registration
    // -------------------------------------------------------------------------

    public record InsertContext(String dataPath, String schemaPath, String tablePath, List<DuckLakeColumnHandle> columns) {}

    public InsertContext getInsertContext(DuckLakeTableHandle tableHandle)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            String dataPath = queryDataPath(conn);
            DuckLakeTableInfo tableInfo = queryTableInfo(conn, tableHandle.getSchemaName(), tableHandle.getTableName(), tableHandle.getSnapshotId())
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Table not found: " + tableHandle.toSchemaTableName()));
            List<DuckLakeColumnInfo> columnInfos = queryColumnInfos(conn, tableHandle.getTableId(), tableHandle.getSnapshotId());
            List<DuckLakeColumnHandle> columnHandles = new ArrayList<>();
            int index = 0;
            for (DuckLakeColumnInfo info : columnInfos) {
                columnHandles.add(new DuckLakeColumnHandle(
                        info.columnName(),
                        DuckLakeTypeMapping.toTrinoType(info.columnType()),
                        index++,
                        info.columnId()));
            }
            return new InsertContext(dataPath, tableInfo.schemaPath(), tableInfo.tablePath(), columnHandles);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get insert context", e);
        }
    }

    public void finishInsert(long tableId, List<DuckLakeWrittenFileInfo> files)
    {
        if (files.isEmpty()) {
            return;
        }

        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                SnapshotInfo snapshot = createSnapshot(conn, "INSERT", 0, files.size(), false);
                registerInsertedFiles(conn, tableId, snapshot, files);

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to register inserted files", e);
        }
    }

    public void finishMerge(DuckLakeTableHandle tableHandle, List<DuckLakeWrittenFileInfo> insertedFiles, List<DuckLakeDeleteFileInfo> deleteFiles)
    {
        if (insertedFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }

        try (Connection conn = connectionManager.openMetadataConnection()) {
            conn.setAutoCommit(false);
            try {
                String changeType = insertedFiles.isEmpty() ? "DELETE" : (deleteFiles.isEmpty() ? "INSERT" : "UPDATE");
                SnapshotInfo snapshot = createSnapshot(conn, changeType, 0, insertedFiles.size() + deleteFiles.size(), false);
                long nextFileId = snapshot.nextFileId();

                if (!insertedFiles.isEmpty()) {
                    nextFileId = registerInsertedFiles(conn, tableHandle.getTableId(), snapshot, insertedFiles).nextFileId();
                }
                if (!deleteFiles.isEmpty()) {
                    registerDeleteFiles(conn, tableHandle.getTableId(), snapshot, nextFileId, deleteFiles);
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to finish merge for " + tableHandle.toSchemaTableName(), e);
        }
    }

    private static List<DuckLakeWrittenFileInfo> remapColumnIds(List<DuckLakeWrittenFileInfo> files, long firstColumnId)
    {
        List<DuckLakeWrittenFileInfo> remapped = new ArrayList<>(files.size());
        for (DuckLakeWrittenFileInfo file : files) {
            List<DuckLakeWrittenFileInfo.ColumnStats> remappedStats = new ArrayList<>(file.columnStats().size());
            for (DuckLakeWrittenFileInfo.ColumnStats stats : file.columnStats()) {
                remappedStats.add(new DuckLakeWrittenFileInfo.ColumnStats(
                        firstColumnId + stats.columnId(),
                        stats.valueCount(),
                        stats.nullCount(),
                        stats.minValue(),
                        stats.maxValue()));
            }
            remapped.add(new DuckLakeWrittenFileInfo(
                    file.path(),
                    file.recordCount(),
                    file.fileSizeBytes(),
                    file.footerSize(),
                    remappedStats));
        }
        return remapped;
    }

    private record InsertRegistrationResult(long nextRowId, long totalRecords, long totalFileSize, long nextFileId) {}

    private InsertRegistrationResult registerInsertedFiles(Connection conn, long tableId, SnapshotInfo snapshot, List<DuckLakeWrittenFileInfo> files)
            throws SQLException
    {
        long nextRowId = getNextRowId(conn, tableId);
        long fileId = snapshot.nextFileId();
        long totalRecords = 0;
        long totalFileSize = 0;

        for (DuckLakeWrittenFileInfo file : files) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) VALUES (?, ?, ?, NULL, ?, ?, true, 'parquet', ?, ?, NULL, ?, NULL, NULL, NULL, NULL)")) {
                stmt.setLong(1, fileId);
                stmt.setLong(2, tableId);
                stmt.setLong(3, snapshot.snapshotId());
                stmt.setLong(4, fileId);
                stmt.setString(5, file.path());
                stmt.setLong(6, file.recordCount());
                stmt.setLong(7, file.fileSizeBytes());
                stmt.setLong(8, nextRowId);
                stmt.executeUpdate();
            }

            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, false, NULL)")) {
                for (DuckLakeWrittenFileInfo.ColumnStats colStats : file.columnStats()) {
                    stmt.setLong(1, fileId);
                    stmt.setLong(2, tableId);
                    stmt.setLong(3, colStats.columnId());
                    stmt.setLong(4, colStats.valueCount());
                    stmt.setLong(5, colStats.nullCount());
                    stmt.setString(6, colStats.minValue());
                    stmt.setString(7, colStats.maxValue());
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }

            for (DuckLakeWrittenFileInfo.ColumnStats colStats : file.columnStats()) {
                updateTableColumnStats(conn, tableId, colStats);
            }

            nextRowId += file.recordCount();
            totalRecords += file.recordCount();
            totalFileSize += file.fileSizeBytes();
            fileId++;
        }

        try (PreparedStatement stmt = conn.prepareStatement(
                "UPDATE ducklake_table_stats SET record_count = record_count + ?, next_row_id = ?, file_size_bytes = file_size_bytes + ? WHERE table_id = ?")) {
            stmt.setLong(1, totalRecords);
            stmt.setLong(2, nextRowId);
            stmt.setLong(3, totalFileSize);
            stmt.setLong(4, tableId);
            stmt.executeUpdate();
        }

        return new InsertRegistrationResult(nextRowId, totalRecords, totalFileSize, fileId);
    }

    private void registerDeleteFiles(Connection conn, long tableId, SnapshotInfo snapshot, long firstDeleteFileId, List<DuckLakeDeleteFileInfo> deleteFiles)
            throws SQLException
    {
        for (DuckLakeDeleteFileInfo deleteFile : deleteFiles) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND data_file_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, snapshot.snapshotId());
                stmt.setLong(2, tableId);
                stmt.setLong(3, deleteFile.dataFileId());
                stmt.executeUpdate();
            }
        }
        long deleteFileId = firstDeleteFileId;
        for (DuckLakeDeleteFileInfo deleteFile : deleteFiles) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key, partial_max) VALUES (?, ?, ?, NULL, ?, ?, true, 'parquet', ?, ?, NULL, NULL, NULL)")) {
                stmt.setLong(1, deleteFileId);
                stmt.setLong(2, tableId);
                stmt.setLong(3, snapshot.snapshotId());
                stmt.setLong(4, deleteFile.dataFileId());
                stmt.setString(5, deleteFile.path());
                stmt.setLong(6, deleteFile.deleteCount());
                stmt.setLong(7, deleteFile.fileSizeBytes());
                stmt.executeUpdate();
            }
            deleteFileId++;
        }
    }

    private long getNextRowId(Connection conn, long tableId)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT next_row_id FROM ducklake_table_stats WHERE table_id = ?")) {
            stmt.setLong(1, tableId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "No table stats found for table_id=" + tableId);
                }
                return rs.getLong("next_row_id");
            }
        }
    }

    private void updateTableColumnStats(Connection conn, long tableId, DuckLakeWrittenFileInfo.ColumnStats colStats)
            throws SQLException
    {
        Type type = getColumnType(conn, tableId, colStats.columnId());

        String currentMin = null;
        String currentMax = null;
        boolean containsNull = false;
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT contains_null, min_value, max_value FROM ducklake_table_column_stats WHERE table_id = ? AND column_id = ?")) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, colStats.columnId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    containsNull = rs.getBoolean("contains_null");
                    currentMin = rs.getString("min_value");
                    currentMax = rs.getString("max_value");
                }
            }
        }

        String mergedMin = mergeMinValue(type, currentMin, colStats.minValue());
        String mergedMax = mergeMaxValue(type, currentMax, colStats.maxValue());

        try (PreparedStatement stmt = conn.prepareStatement(
                "UPDATE ducklake_table_column_stats SET contains_null = ?, min_value = ?, max_value = ? WHERE table_id = ? AND column_id = ?")) {
            stmt.setBoolean(1, containsNull || colStats.nullCount() > 0);
            stmt.setString(2, mergedMin);
            stmt.setString(3, mergedMax);
            stmt.setLong(4, tableId);
            stmt.setLong(5, colStats.columnId());
            stmt.executeUpdate();
        }
    }

    private Type getColumnType(Connection conn, long tableId, long columnId)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT column_type FROM ducklake_column WHERE table_id = ? AND column_id = ? AND end_snapshot IS NULL")) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Column not found for stats update: table_id=" + tableId + ", column_id=" + columnId);
                }
                return DuckLakeTypeMapping.toTrinoType(rs.getString("column_type"));
            }
        }
    }

    private String mergeMinValue(Type type, String currentValue, String candidateValue)
    {
        if (currentValue == null) {
            return candidateValue;
        }
        if (candidateValue == null) {
            return currentValue;
        }
        return compareStatStrings(type, candidateValue, currentValue) < 0 ? candidateValue : currentValue;
    }

    private String mergeMaxValue(Type type, String currentValue, String candidateValue)
    {
        if (currentValue == null) {
            return candidateValue;
        }
        if (candidateValue == null) {
            return currentValue;
        }
        return compareStatStrings(type, candidateValue, currentValue) > 0 ? candidateValue : currentValue;
    }

    private int compareStatStrings(Type type, String left, String right)
    {
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType || type instanceof DateType) {
            return Long.compare(Long.parseLong(left), Long.parseLong(right));
        }
        if (type instanceof RealType || type instanceof DoubleType) {
            return Double.compare(Double.parseDouble(left), Double.parseDouble(right));
        }
        if (type instanceof DecimalType) {
            return new BigDecimal(left).compareTo(new BigDecimal(right));
        }
        if (type instanceof BooleanType) {
            return Boolean.compare(Boolean.parseBoolean(left), Boolean.parseBoolean(right));
        }
        return left.compareTo(right);
    }

    /**
     * Constructs the absolute path for a data or delete file.
     * When path_is_relative is true: full path = data_path + schema_path + table_path + file_path
     */
    private String resolvePath(
            String dataPath,
            String schemaPath, boolean schemaRelative,
            String tablePath, boolean tableRelative,
            String filePath, boolean fileRelative)
    {
        if (!fileRelative) {
            return filePath;
        }
        StringBuilder sb = new StringBuilder(dataPath);
        if (schemaRelative && schemaPath != null && !schemaPath.isEmpty()) {
            sb.append(schemaPath);
            if (!schemaPath.endsWith("/")) {
                sb.append("/");
            }
        }
        if (tableRelative && tablePath != null && !tablePath.isEmpty()) {
            sb.append(tablePath);
            if (!tablePath.endsWith("/")) {
                sb.append("/");
            }
        }
        sb.append(filePath);
        return sb.toString();
    }

    private void setUuid(PreparedStatement stmt, int parameterIndex, UUID value)
            throws SQLException
    {
        if (connectionManager.isPostgresql()) {
            stmt.setObject(parameterIndex, value);
            return;
        }
        stmt.setString(parameterIndex, value.toString());
    }
}
