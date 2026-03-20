package io.trino.plugin.ducklake;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDuckLakeClient
{
    @TempDir
    Path tempDir;

    private DuckLakeClient client;

    @BeforeEach
    void setUp()
            throws Exception
    {
        Path dataDir = tempDir.resolve("data_files");
        Files.createDirectories(dataDir);
        Path dbPath = tempDir.resolve("metadata.sqlite");

        DuckLakeQueryRunner.initializeMetadataDatabase(dbPath.toString(), dataDir.toString());

        DuckLakeConfig config = new DuckLakeConfig();
        config.setMetadataConnectionString("jdbc:sqlite:" + dbPath);
        DuckLakeConnectionManager connectionManager = new DuckLakeConnectionManager(config);
        client = new DuckLakeClient(connectionManager);
    }

    // -----------------------------------------------------------------------
    // Schema operations
    // -----------------------------------------------------------------------

    @Test
    void testGetSchemaNames_empty()
    {
        List<String> schemas = client.getSchemaNames();
        assertThat(schemas).isEmpty();
    }

    @Test
    void testCreateSchema()
    {
        client.createSchema("test_schema");
        List<String> schemas = client.getSchemaNames();
        assertThat(schemas).containsExactly("test_schema");
    }

    @Test
    void testCreateSchema_duplicate()
    {
        client.createSchema("dup_schema");
        assertThatThrownBy(() -> client.createSchema("dup_schema"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void testDropSchema()
    {
        client.createSchema("drop_me");
        assertThat(client.getSchemaNames()).contains("drop_me");
        client.dropSchema("drop_me", false);
        assertThat(client.getSchemaNames()).doesNotContain("drop_me");
    }

    @Test
    void testDropSchema_notFound()
    {
        assertThatThrownBy(() -> client.dropSchema("nonexistent", false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void testDropSchema_notEmpty()
    {
        client.createSchema("notempty");
        client.createTable("notempty", "tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));
        assertThatThrownBy(() -> client.dropSchema("notempty", false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("not empty");
    }

    // -----------------------------------------------------------------------
    // Table operations
    // -----------------------------------------------------------------------

    @Test
    void testCreateTable()
    {
        client.createSchema("tbl_schema");
        client.createTable("tbl_schema", "my_table", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER),
                new ColumnMetadata("name", VarcharType.VARCHAR),
                new ColumnMetadata("count", BigintType.BIGINT)));

        List<SchemaTableName> tables = client.listTables(Optional.of("tbl_schema"));
        assertThat(tables).hasSize(1);
        assertEquals("my_table", tables.get(0).getTableName());
    }

    @Test
    void testCreateTable_duplicate()
    {
        client.createSchema("dup_tbl_schema");
        client.createTable("dup_tbl_schema", "tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));
        assertThatThrownBy(() -> client.createTable("dup_tbl_schema", "tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void testCreateTable_schemaNotFound()
    {
        assertThatThrownBy(() -> client.createTable("nonexistent", "tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void testDropTable()
    {
        client.createSchema("drop_tbl_schema");
        client.createTable("drop_tbl_schema", "to_drop", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));

        Optional<DuckLakeClient.TableInfoWithSnapshot> info = client.getTableInfoWithSnapshot("drop_tbl_schema", "to_drop");
        assertTrue(info.isPresent());

        DuckLakeTableHandle handle = new DuckLakeTableHandle(
                "drop_tbl_schema", "to_drop",
                info.get().tableInfo().tableId(),
                info.get().snapshotId(),
                TupleDomain.all());
        client.dropTable(handle);

        assertThat(client.listTables(Optional.of("drop_tbl_schema"))).isEmpty();
    }

    @Test
    void testGetTableInfoWithSnapshot()
    {
        client.createSchema("info_schema");
        client.createTable("info_schema", "info_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));

        Optional<DuckLakeClient.TableInfoWithSnapshot> result = client.getTableInfoWithSnapshot("info_schema", "info_tbl");
        assertTrue(result.isPresent());
        assertEquals("info_schema", result.get().tableInfo().schemaName());
        assertEquals("info_tbl", result.get().tableInfo().tableName());
        assertTrue(result.get().snapshotId() > 0);
    }

    @Test
    void testGetTableInfoWithSnapshot_missing()
    {
        Optional<DuckLakeClient.TableInfoWithSnapshot> result = client.getTableInfoWithSnapshot("nonexistent", "nonexistent");
        assertTrue(result.isEmpty());
    }

    @Test
    void testListTables()
    {
        client.createSchema("list_schema_a");
        client.createSchema("list_schema_b");
        client.createTable("list_schema_a", "tbl1", List.of(new ColumnMetadata("id", IntegerType.INTEGER)));
        client.createTable("list_schema_a", "tbl2", List.of(new ColumnMetadata("id", IntegerType.INTEGER)));
        client.createTable("list_schema_b", "tbl3", List.of(new ColumnMetadata("id", IntegerType.INTEGER)));

        List<SchemaTableName> allTables = client.listTables(Optional.empty());
        assertThat(allTables).hasSize(3);

        List<SchemaTableName> schemaATables = client.listTables(Optional.of("list_schema_a"));
        assertThat(schemaATables).hasSize(2);

        List<SchemaTableName> schemaBTables = client.listTables(Optional.of("list_schema_b"));
        assertThat(schemaBTables).hasSize(1);
    }

    @Test
    void testGetColumns()
    {
        client.createSchema("col_schema");
        client.createTable("col_schema", "col_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER),
                new ColumnMetadata("name", VarcharType.VARCHAR)));

        List<ColumnMetadata> columns = client.getColumns("col_schema", "col_tbl");
        assertNotNull(columns);
        assertThat(columns).hasSize(2);
        assertEquals("id", columns.get(0).getName());
        assertEquals(IntegerType.INTEGER, columns.get(0).getType());
        assertEquals("name", columns.get(1).getName());
        assertEquals(VarcharType.VARCHAR, columns.get(1).getType());
    }

    @Test
    void testGetColumnInfos()
    {
        client.createSchema("colinfo_schema");
        client.createTable("colinfo_schema", "colinfo_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER),
                new ColumnMetadata("val", BigintType.BIGINT)));

        Optional<DuckLakeClient.TableInfoWithSnapshot> info = client.getTableInfoWithSnapshot("colinfo_schema", "colinfo_tbl");
        assertTrue(info.isPresent());

        List<DuckLakeColumnInfo> columnInfos = client.getColumnInfos(info.get().tableInfo().tableId(), info.get().snapshotId());
        assertThat(columnInfos).hasSize(2);
        assertEquals("id", columnInfos.get(0).columnName());
        assertEquals("int32", columnInfos.get(0).columnType());
        assertEquals("val", columnInfos.get(1).columnName());
        assertEquals("int64", columnInfos.get(1).columnType());
    }

    // -----------------------------------------------------------------------
    // Insert + file registration
    // -----------------------------------------------------------------------

    @Test
    void testFinishInsert_registersFiles()
    {
        client.createSchema("ins_schema");
        client.createTable("ins_schema", "ins_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));

        Optional<DuckLakeClient.TableInfoWithSnapshot> info = client.getTableInfoWithSnapshot("ins_schema", "ins_tbl");
        assertTrue(info.isPresent());
        long tableId = info.get().tableInfo().tableId();

        List<DuckLakeColumnInfo> columnInfos = client.getColumnInfos(tableId, info.get().snapshotId());
        long columnId = columnInfos.get(0).columnId();

        // Register a fake file
        DuckLakeWrittenFileInfo file = new DuckLakeWrittenFileInfo(
                "test_file.parquet",
                100,
                1024,
                128,
                List.of(new DuckLakeWrittenFileInfo.ColumnStats(columnId, 100, 0, "1", "100")));

        client.finishInsert(tableId, List.of(file));

        // Verify via getDataFiles
        DuckLakeTableHandle handle = new DuckLakeTableHandle(
                "ins_schema", "ins_tbl",
                tableId,
                client.getTableInfoWithSnapshot("ins_schema", "ins_tbl").get().snapshotId(),
                TupleDomain.all());
        List<DuckLakeDataFileInfo> dataFiles = client.getDataFiles(handle, TupleDomain.all());
        assertThat(dataFiles).hasSize(1);
        assertThat(dataFiles.get(0).dataFilePath()).endsWith("test_file.parquet");
        assertEquals(100, dataFiles.get(0).recordCount());
    }

    @Test
    void testFinishInsert_emptyFiles()
    {
        client.createSchema("empty_ins_schema");
        client.createTable("empty_ins_schema", "empty_ins_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));

        Optional<DuckLakeClient.TableInfoWithSnapshot> info = client.getTableInfoWithSnapshot("empty_ins_schema", "empty_ins_tbl");
        long tableId = info.get().tableInfo().tableId();

        // Empty files list should be a no-op
        client.finishInsert(tableId, List.of());

        DuckLakeTableHandle handle = new DuckLakeTableHandle(
                "empty_ins_schema", "empty_ins_tbl",
                tableId,
                client.getTableInfoWithSnapshot("empty_ins_schema", "empty_ins_tbl").get().snapshotId(),
                TupleDomain.all());
        List<DuckLakeDataFileInfo> dataFiles = client.getDataFiles(handle, TupleDomain.all());
        assertThat(dataFiles).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Snapshot isolation
    // -----------------------------------------------------------------------

    @Test
    void testSnapshotIsolation()
    {
        client.createSchema("snap_schema");

        // Capture snapshot before creating a table
        Optional<DuckLakeClient.TableInfoWithSnapshot> beforeInfo = client.getTableInfoWithSnapshot("snap_schema", "snap_tbl");
        assertTrue(beforeInfo.isEmpty());

        // Create table after the snapshot
        client.createTable("snap_schema", "snap_tbl", List.of(
                new ColumnMetadata("id", IntegerType.INTEGER)));

        // The new table should be visible now
        Optional<DuckLakeClient.TableInfoWithSnapshot> afterInfo = client.getTableInfoWithSnapshot("snap_schema", "snap_tbl");
        assertTrue(afterInfo.isPresent());
    }
}
