package io.trino.plugin.ducklake;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDuckLakeQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DuckLakeQueryRunner.createQueryRunner();
    }

    // -----------------------------------------------------------------------
    // DDL
    // -----------------------------------------------------------------------

    @Test
    void testCreateAndDropSchema()
    {
        assertUpdate("CREATE SCHEMA test_ddl_schema");
        MaterializedResult schemas = computeActual("SHOW SCHEMAS");
        assertThat(schemas.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_ddl_schema");
        assertUpdate("DROP SCHEMA test_ddl_schema");
    }

    @Test
    void testCreateAndDropTable()
    {
        assertUpdate("CREATE SCHEMA test_table_schema");
        assertUpdate("CREATE TABLE test_table_schema.test_tbl (id INTEGER, name VARCHAR)");
        MaterializedResult tables = computeActual("SHOW TABLES FROM test_table_schema");
        assertThat(tables.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_tbl");
        assertUpdate("DROP TABLE test_table_schema.test_tbl");
        assertUpdate("DROP SCHEMA test_table_schema");
    }

    @Test
    void testCreateTableAsSelect()
    {
        assertUpdate("CREATE SCHEMA ctas_schema");
        assertUpdate("CREATE TABLE ctas_schema.ctas_tbl AS SELECT 1 AS id, 'hello' AS name", 1);
        MaterializedResult result = computeActual("SELECT * FROM ctas_schema.ctas_tbl");
        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getMaterializedRows().get(0).getField(0));
        assertEquals("hello", result.getMaterializedRows().get(0).getField(1));
        assertUpdate("DROP TABLE ctas_schema.ctas_tbl");
        assertUpdate("DROP SCHEMA ctas_schema");
    }

    @Test
    void testDescribeTable()
    {
        assertUpdate("CREATE SCHEMA desc_schema");
        assertUpdate("CREATE TABLE desc_schema.desc_tbl (id INTEGER, name VARCHAR, active BOOLEAN)");
        MaterializedResult result = computeActual("DESCRIBE desc_schema.desc_tbl");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("id", "name", "active");
        assertUpdate("DROP TABLE desc_schema.desc_tbl");
        assertUpdate("DROP SCHEMA desc_schema");
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    @Test
    void testSelectFromTable()
    {
        assertUpdate("CREATE SCHEMA read_schema");
        assertUpdate("CREATE TABLE read_schema.numbers (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO read_schema.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);
        MaterializedResult result = computeActual("SELECT * FROM read_schema.numbers ORDER BY id");
        assertEquals(3, result.getRowCount());
        assertEquals(1, result.getMaterializedRows().get(0).getField(0));
        assertEquals("one", result.getMaterializedRows().get(0).getField(1));
        assertEquals(3, result.getMaterializedRows().get(2).getField(0));
        assertUpdate("DROP TABLE read_schema.numbers");
        assertUpdate("DROP SCHEMA read_schema");
    }

    @Test
    void testSelectWithPredicate()
    {
        assertUpdate("CREATE SCHEMA pred_schema");
        assertUpdate("CREATE TABLE pred_schema.nums (id INTEGER, val VARCHAR)");
        assertUpdate("INSERT INTO pred_schema.nums VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        MaterializedResult result = computeActual("SELECT val FROM pred_schema.nums WHERE id = 2");
        assertEquals(1, result.getRowCount());
        assertEquals("b", result.getMaterializedRows().get(0).getField(0));
        assertUpdate("DROP TABLE pred_schema.nums");
        assertUpdate("DROP SCHEMA pred_schema");
    }

    @Test
    void testSelectCount()
    {
        assertUpdate("CREATE SCHEMA count_schema");
        assertUpdate("CREATE TABLE count_schema.items (id INTEGER)");
        assertUpdate("INSERT INTO count_schema.items VALUES (1), (2), (3), (4), (5)", 5);
        Object count = computeScalar("SELECT count(*) FROM count_schema.items");
        assertEquals(5L, count);
        assertUpdate("DROP TABLE count_schema.items");
        assertUpdate("DROP SCHEMA count_schema");
    }

    @Test
    void testSelectFromEmptyTable()
    {
        assertUpdate("CREATE SCHEMA empty_schema");
        assertUpdate("CREATE TABLE empty_schema.empty_tbl (id INTEGER)");
        MaterializedResult result = computeActual("SELECT * FROM empty_schema.empty_tbl");
        assertEquals(0, result.getRowCount());
        assertUpdate("DROP TABLE empty_schema.empty_tbl");
        assertUpdate("DROP SCHEMA empty_schema");
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    @Test
    void testInsertValues()
    {
        assertUpdate("CREATE SCHEMA insert_schema");
        assertUpdate("CREATE TABLE insert_schema.tbl (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO insert_schema.tbl VALUES (1, 'alice')", 1);
        assertUpdate("INSERT INTO insert_schema.tbl VALUES (2, 'bob')", 1);
        MaterializedResult result = computeActual("SELECT * FROM insert_schema.tbl ORDER BY id");
        assertEquals(2, result.getRowCount());
        assertUpdate("DROP TABLE insert_schema.tbl");
        assertUpdate("DROP SCHEMA insert_schema");
    }

    @Test
    void testInsertSelect()
    {
        assertUpdate("CREATE SCHEMA insselect_schema");
        assertUpdate("CREATE TABLE insselect_schema.src AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)", 2);
        assertUpdate("CREATE TABLE insselect_schema.dst (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO insselect_schema.dst SELECT * FROM insselect_schema.src", 2);
        MaterializedResult result = computeActual("SELECT count(*) FROM insselect_schema.dst");
        assertEquals(2L, result.getMaterializedRows().get(0).getField(0));
        assertUpdate("DROP TABLE insselect_schema.dst");
        assertUpdate("DROP TABLE insselect_schema.src");
        assertUpdate("DROP SCHEMA insselect_schema");
    }

    @Test
    void testInsertMultipleBatches()
    {
        assertUpdate("CREATE SCHEMA multi_schema");
        assertUpdate("CREATE TABLE multi_schema.tbl (id INTEGER)");
        assertUpdate("INSERT INTO multi_schema.tbl VALUES (1), (2)", 2);
        assertUpdate("INSERT INTO multi_schema.tbl VALUES (3), (4)", 2);
        assertUpdate("INSERT INTO multi_schema.tbl VALUES (5)", 1);
        Object count = computeScalar("SELECT count(*) FROM multi_schema.tbl");
        assertEquals(5L, count);
        assertUpdate("DROP TABLE multi_schema.tbl");
        assertUpdate("DROP SCHEMA multi_schema");
    }

    // -----------------------------------------------------------------------
    // Type coverage
    // -----------------------------------------------------------------------

    @Test
    void testIntegerTypes()
    {
        assertUpdate("CREATE SCHEMA type_schema");
        assertUpdate("CREATE TABLE type_schema.int_types (a TINYINT, b SMALLINT, c INTEGER, d BIGINT)");
        assertUpdate("INSERT INTO type_schema.int_types VALUES (1, 100, 10000, 1000000000)", 1);
        MaterializedResult result = computeActual("SELECT * FROM type_schema.int_types");
        assertEquals(1, result.getRowCount());
        assertEquals((byte) 1, result.getMaterializedRows().get(0).getField(0));
        assertEquals((short) 100, result.getMaterializedRows().get(0).getField(1));
        assertEquals(10000, result.getMaterializedRows().get(0).getField(2));
        assertEquals(1000000000L, result.getMaterializedRows().get(0).getField(3));
        assertUpdate("DROP TABLE type_schema.int_types");
        assertUpdate("DROP SCHEMA type_schema");
    }

    @Test
    void testFloatTypes()
    {
        assertUpdate("CREATE SCHEMA float_schema");
        assertUpdate("CREATE TABLE float_schema.float_types (a REAL, b DOUBLE)");
        assertUpdate("INSERT INTO float_schema.float_types VALUES (1.5, 2.5)", 1);
        MaterializedResult result = computeActual("SELECT * FROM float_schema.float_types");
        assertEquals(1, result.getRowCount());
        assertUpdate("DROP TABLE float_schema.float_types");
        assertUpdate("DROP SCHEMA float_schema");
    }

    @Test
    void testBooleanType()
    {
        assertUpdate("CREATE SCHEMA bool_schema");
        assertUpdate("CREATE TABLE bool_schema.bool_tbl (flag BOOLEAN)");
        assertUpdate("INSERT INTO bool_schema.bool_tbl VALUES (true), (false)", 2);
        MaterializedResult result = computeActual("SELECT * FROM bool_schema.bool_tbl ORDER BY flag");
        assertEquals(2, result.getRowCount());
        assertEquals(false, result.getMaterializedRows().get(0).getField(0));
        assertEquals(true, result.getMaterializedRows().get(1).getField(0));
        assertUpdate("DROP TABLE bool_schema.bool_tbl");
        assertUpdate("DROP SCHEMA bool_schema");
    }

    @Test
    void testVarcharType()
    {
        assertUpdate("CREATE SCHEMA varchar_schema");
        assertUpdate("CREATE TABLE varchar_schema.str_tbl (name VARCHAR)");
        assertUpdate("INSERT INTO varchar_schema.str_tbl VALUES ('hello'), ('world')", 2);
        MaterializedResult result = computeActual("SELECT * FROM varchar_schema.str_tbl ORDER BY name");
        assertEquals(2, result.getRowCount());
        assertEquals("hello", result.getMaterializedRows().get(0).getField(0));
        assertEquals("world", result.getMaterializedRows().get(1).getField(0));
        assertUpdate("DROP TABLE varchar_schema.str_tbl");
        assertUpdate("DROP SCHEMA varchar_schema");
    }

    @Test
    void testDateType()
    {
        assertUpdate("CREATE SCHEMA date_schema");
        assertUpdate("CREATE TABLE date_schema.date_tbl (d DATE)");
        assertUpdate("INSERT INTO date_schema.date_tbl VALUES (DATE '2024-01-15')", 1);
        MaterializedResult result = computeActual("SELECT * FROM date_schema.date_tbl");
        assertEquals(1, result.getRowCount());
        assertUpdate("DROP TABLE date_schema.date_tbl");
        assertUpdate("DROP SCHEMA date_schema");
    }

    // -----------------------------------------------------------------------
    // Update / Delete (merge path)
    // -----------------------------------------------------------------------

    @Test
    void testDeleteRows()
    {
        assertUpdate("CREATE SCHEMA del_schema");
        assertUpdate("CREATE TABLE del_schema.tbl (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO del_schema.tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertUpdate("DELETE FROM del_schema.tbl WHERE id = 2", 1);
        MaterializedResult result = computeActual("SELECT id FROM del_schema.tbl ORDER BY id");
        assertEquals(2, result.getRowCount());
        assertEquals(1, result.getMaterializedRows().get(0).getField(0));
        assertEquals(3, result.getMaterializedRows().get(1).getField(0));
        assertUpdate("DROP TABLE del_schema.tbl");
        assertUpdate("DROP SCHEMA del_schema");
    }

    @Test
    void testUpdateRows()
    {
        assertUpdate("CREATE SCHEMA upd_schema");
        assertUpdate("CREATE TABLE upd_schema.tbl (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO upd_schema.tbl VALUES (1, 'old'), (2, 'old')", 2);
        assertUpdate("UPDATE upd_schema.tbl SET name = 'new' WHERE id = 1", 1);
        MaterializedResult result = computeActual("SELECT name FROM upd_schema.tbl WHERE id = 1");
        assertEquals(1, result.getRowCount());
        assertEquals("new", result.getMaterializedRows().get(0).getField(0));
        assertUpdate("DROP TABLE upd_schema.tbl");
        assertUpdate("DROP SCHEMA upd_schema");
    }
}
