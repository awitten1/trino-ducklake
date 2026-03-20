package io.trino.plugin.ducklake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDuckLakeConnectionManager
{
    private static DuckLakeConnectionManager create(String jdbcUrl)
    {
        DuckLakeConfig config = new DuckLakeConfig();
        config.setMetadataConnectionString(jdbcUrl);
        return new DuckLakeConnectionManager(config);
    }

    @Test
    void testIsPostgresql()
    {
        assertTrue(create("jdbc:postgresql://localhost:5432/db").isPostgresql());
        assertFalse(create("jdbc:sqlite:/tmp/test.db").isPostgresql());
        assertFalse(create("jdbc:duckdb:/tmp/test.duckdb").isPostgresql());
    }

    @Test
    void testGetMetadataBaseDirectory_sqlite()
    {
        DuckLakeConnectionManager manager = create("jdbc:sqlite:/tmp/foo/bar.db");
        String baseDir = manager.getMetadataBaseDirectory();
        assertNotNull(baseDir);
        assertTrue(baseDir.endsWith(File.separator));
        assertTrue(baseDir.contains("foo"));
    }

    @Test
    void testGetMetadataBaseDirectory_duckdb()
    {
        DuckLakeConnectionManager manager = create("jdbc:duckdb:/tmp/foo/bar.duckdb");
        String baseDir = manager.getMetadataBaseDirectory();
        assertNotNull(baseDir);
        assertTrue(baseDir.endsWith(File.separator));
        assertTrue(baseDir.contains("foo"));
    }

    @Test
    void testGetMetadataBaseDirectory_postgres()
    {
        DuckLakeConnectionManager manager = create("jdbc:postgresql://localhost:5432/db");
        assertNull(manager.getMetadataBaseDirectory());
    }

    @Test
    void testGetMetadataBaseDirectory_inMemoryDuckdb()
    {
        DuckLakeConnectionManager manager = create("jdbc:duckdb:");
        assertNull(manager.getMetadataBaseDirectory());
    }

    @Test
    void testOpenConnection_sqlite(@TempDir Path tempDir)
            throws SQLException
    {
        String dbPath = tempDir.resolve("test.db").toString();
        DuckLakeConnectionManager manager = create("jdbc:sqlite:" + dbPath);
        try (Connection conn = manager.openMetadataConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testOpenConnection_unsupported()
    {
        DuckLakeConnectionManager manager = create("jdbc:mysql://localhost/db");
        assertThrows(SQLException.class, manager::openMetadataConnection);
    }
}
