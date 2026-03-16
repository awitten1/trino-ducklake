package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DuckLakeClient
{
    private final DuckLakeConnectionManager connectionManager;

    @Inject
    public DuckLakeClient(DuckLakeConnectionManager connectionManager)
    {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    // -------------------------------------------------------------------------
    // Snapshot
    // -------------------------------------------------------------------------

    private long getCurrentSnapshotId(Connection conn)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT max(snapshot_id) FROM ducklake_snapshot")) {
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

    // -------------------------------------------------------------------------
    // Data path
    // -------------------------------------------------------------------------

    public String getDataPath()
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            return queryDataPath(conn);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read data path from ducklake_metadata", e);
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

    public boolean tableExists(String schemaName, String tableName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            return queryTableInfo(conn, schemaName, tableName, snapshotId).isPresent();
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to check table existence", e);
        }
    }

    public Optional<DuckLakeTableInfo> getTableInfo(String schemaName, String tableName)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
            return queryTableInfo(conn, schemaName, tableName, snapshotId);
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get table info", e);
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
        Optional<DuckLakeTableInfo> tableInfo = getTableInfo(schemaName, tableName);
        if (tableInfo.isEmpty()) {
            return null;
        }
        try (Connection conn = connectionManager.openMetadataConnection()) {
            long snapshotId = getCurrentSnapshotId(conn);
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

    public List<DuckLakeDataFileInfo> getDataFiles(long tableId, long snapshotId, DuckLakeTableInfo tableInfo)
    {
        try (Connection conn = connectionManager.openMetadataConnection()) {
            String dataPath = queryDataPath(conn);
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
                                rs.getLong("data_file_id"),
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
}
