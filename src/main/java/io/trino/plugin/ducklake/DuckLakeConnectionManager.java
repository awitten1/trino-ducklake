package io.trino.plugin.ducklake;

import com.google.inject.Inject;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class DuckLakeConnectionManager
{
    private final String metadataJdbcUrl;

    @Inject
    public DuckLakeConnectionManager(DuckLakeConfig config)
    {
        this.metadataJdbcUrl = requireNonNull(config.getMetadataConnectionString(), "metadataConnectionString is null");
    }

    /**
     * Opens a connection to the metadata database (SQLite, DuckDB file, Postgres, etc.).
     * Drivers are instantiated directly to avoid DriverManager classloader issues in
     * Trino's isolated plugin classloader.
     * The caller is responsible for closing the connection.
     */
    public Connection openMetadataConnection()
            throws SQLException
    {
        if (metadataJdbcUrl.startsWith("jdbc:duckdb:")) {
            Properties props = new Properties();
            props.setProperty("duckdb.read_only", "true");
            return new org.duckdb.DuckDBDriver().connect(metadataJdbcUrl, props);
        }
        if (metadataJdbcUrl.startsWith("jdbc:sqlite:")) {
            return new org.sqlite.JDBC().connect(metadataJdbcUrl, new Properties());
        }
        if (metadataJdbcUrl.startsWith("jdbc:postgresql:")) {
            return new org.postgresql.Driver().connect(metadataJdbcUrl, new Properties());
        }
        throw new SQLException("Unsupported JDBC URL: " + metadataJdbcUrl);
    }

    /**
     * For file-based metadata databases (jdbc:sqlite: or jdbc:duckdb:), returns
     * the absolute path of the directory containing the metadata file, with a
     * trailing slash. Returns null for non-file-based databases (e.g. PostgreSQL).
     *
     * Used to resolve a relative data_path from ducklake_metadata into an
     * absolute path.
     */
    public String getMetadataBaseDirectory()
    {
        String filePath = null;
        if (metadataJdbcUrl.startsWith("jdbc:sqlite:")) {
            filePath = metadataJdbcUrl.substring("jdbc:sqlite:".length());
        }
        else if (metadataJdbcUrl.startsWith("jdbc:duckdb:") &&
                metadataJdbcUrl.length() > "jdbc:duckdb:".length()) {
            filePath = metadataJdbcUrl.substring("jdbc:duckdb:".length());
        }

        if (filePath == null || filePath.isEmpty()) {
            return null;
        }

        String parent = new File(filePath).getAbsoluteFile().getParent();
        if (parent == null) {
            return null;
        }
        return parent.endsWith(File.separator) ? parent : parent + File.separator;
    }
}
