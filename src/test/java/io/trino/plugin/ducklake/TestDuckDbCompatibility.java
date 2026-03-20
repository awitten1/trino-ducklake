package io.trino.plugin.ducklake;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that data written by Trino can be read by DuckDB, and vice versa.
 * Both engines share the same SQLite metadata database and Parquet data files.
 */
class TestDuckDbCompatibility
{
    @TempDir
    static Path tempDir;

    private static DistributedQueryRunner queryRunner;
    private static String metadataPath;
    private static String dataPath;

    @BeforeAll
    static void setUp()
            throws Exception
    {
        queryRunner = DuckLakeQueryRunner.createQueryRunner(tempDir);
        metadataPath = tempDir.resolve("metadata.sqlite").toString();
        dataPath = tempDir.resolve("data_files").toString();
    }

    @AfterAll
    static void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test
    void testDuckDbReadsTrinoData()
            throws Exception
    {
        // Trino writes data
        queryRunner.execute("CREATE SCHEMA ducklake.compat_schema");
        queryRunner.execute("CREATE TABLE ducklake.compat_schema.numbers (id INTEGER, name VARCHAR)");
        queryRunner.execute("INSERT INTO ducklake.compat_schema.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        // DuckDB reads it
        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, name FROM dl.compat_schema.numbers ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("one", rs.getString("name"));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("two", rs.getString("name"));

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertEquals("three", rs.getString("name"));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testDuckDbReadsTrinoDataAfterMultipleInserts()
            throws Exception
    {
        queryRunner.execute("CREATE SCHEMA ducklake.multi_schema");
        queryRunner.execute("CREATE TABLE ducklake.multi_schema.items (id INTEGER, val BIGINT)");
        queryRunner.execute("INSERT INTO ducklake.multi_schema.items VALUES (1, 100), (2, 200)");
        queryRunner.execute("INSERT INTO ducklake.multi_schema.items VALUES (3, 300)");

        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*) AS cnt FROM dl.multi_schema.items")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getLong("cnt"));
            }

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT sum(val) AS total FROM dl.multi_schema.items")) {
                assertTrue(rs.next());
                assertEquals(600, rs.getLong("total"));
            }
        }
    }

    @Test
    void testDuckDbReadsTrinoTypes()
            throws Exception
    {
        queryRunner.execute("CREATE SCHEMA ducklake.types_schema");
        queryRunner.execute("CREATE TABLE ducklake.types_schema.typed_tbl (" +
                "bool_col BOOLEAN, " +
                "tiny_col TINYINT, " +
                "small_col SMALLINT, " +
                "int_col INTEGER, " +
                "big_col BIGINT, " +
                "real_col REAL, " +
                "double_col DOUBLE, " +
                "varchar_col VARCHAR)");
        queryRunner.execute("INSERT INTO ducklake.types_schema.typed_tbl VALUES (" +
                "true, 1, 100, 10000, 1000000000, 1.5, 2.5, 'hello')");

        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM dl.types_schema.typed_tbl")) {
                assertTrue(rs.next());
                assertEquals(true, rs.getBoolean("bool_col"));
                assertEquals(1, rs.getByte("tiny_col"));
                assertEquals(100, rs.getShort("small_col"));
                assertEquals(10000, rs.getInt("int_col"));
                assertEquals(1000000000L, rs.getLong("big_col"));
                assertEquals(1.5f, rs.getFloat("real_col"), 0.01f);
                assertEquals(2.5, rs.getDouble("double_col"), 0.01);
                assertEquals("hello", rs.getString("varchar_col"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testDuckDbReadsTrinoDataAfterDelete()
            throws Exception
    {
        queryRunner.execute("CREATE SCHEMA ducklake.del_compat_schema");
        queryRunner.execute("CREATE TABLE ducklake.del_compat_schema.tbl (id INTEGER, name VARCHAR)");
        queryRunner.execute("INSERT INTO ducklake.del_compat_schema.tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        queryRunner.execute("DELETE FROM ducklake.del_compat_schema.tbl WHERE id = 2");

        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM dl.del_compat_schema.tbl ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testDuckDbReadsTrinoDataAfterUpdate()
            throws Exception
    {
        queryRunner.execute("CREATE SCHEMA ducklake.upd_compat_schema");
        queryRunner.execute("CREATE TABLE ducklake.upd_compat_schema.tbl (id INTEGER, name VARCHAR)");
        queryRunner.execute("INSERT INTO ducklake.upd_compat_schema.tbl VALUES (1, 'old'), (2, 'old')");
        queryRunner.execute("UPDATE ducklake.upd_compat_schema.tbl SET name = 'new' WHERE id = 1");

        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT name FROM dl.upd_compat_schema.tbl WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("new", rs.getString("name"));
            }
        }
    }

    @Test
    void testDuckDbReadsTrinoCreateTableAsSelect()
            throws Exception
    {
        queryRunner.execute("CREATE SCHEMA ducklake.ctas_compat_schema");
        queryRunner.execute("CREATE TABLE ducklake.ctas_compat_schema.ctas_tbl AS " +
                "SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, label)");

        try (Connection conn = duckDbConnection();
             Statement stmt = conn.createStatement()) {
            attachDuckLake(stmt);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, label FROM dl.ctas_compat_schema.ctas_tbl ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alpha", rs.getString("label"));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("beta", rs.getString("label"));

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertEquals("gamma", rs.getString("label"));

                assertFalse(rs.next());
            }
        }
    }

    private static Connection duckDbConnection()
            throws Exception
    {
        return new org.duckdb.DuckDBDriver().connect("jdbc:duckdb:", new Properties());
    }

    private void attachDuckLake(Statement stmt)
            throws Exception
    {
        stmt.execute("INSTALL ducklake; LOAD ducklake;");
        stmt.execute("ATTACH 'ducklake:sqlite:" + metadataPath + "' AS dl (DATA_PATH '" + dataPath + "/');");
    }
}
