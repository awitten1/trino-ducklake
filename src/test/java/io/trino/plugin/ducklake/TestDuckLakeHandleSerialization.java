package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.InternalTypeManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDuckLakeHandleSerialization
{
    private final ObjectMapper mapper;

    TestDuckLakeHandleSerialization()
    {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NONE);
        mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        SimpleModule typeModule = new SimpleModule();
        typeModule.addDeserializer(Type.class, new TypeDeserializer(InternalTypeManager.TESTING_TYPE_MANAGER));
        mapper.registerModule(typeModule);
    }

    @Test
    void testSplit_jsonRoundTrip()
            throws Exception
    {
        DuckLakeSplit split = new DuckLakeSplit(42L, "/data/file.parquet", Optional.empty(), 0L, 1000L);
        String json = mapper.writeValueAsString(split);
        DuckLakeSplit deserialized = mapper.readValue(json, DuckLakeSplit.class);
        assertEquals(split.getDataFileId(), deserialized.getDataFileId());
        assertEquals(split.getDataFilePath(), deserialized.getDataFilePath());
        assertEquals(split.getDeleteFilePath(), deserialized.getDeleteFilePath());
        assertEquals(split.getRowIdStart(), deserialized.getRowIdStart());
        assertEquals(split.getRecordCount(), deserialized.getRecordCount());
    }

    @Test
    void testSplit_withDeleteFile_jsonRoundTrip()
            throws Exception
    {
        DuckLakeSplit split = new DuckLakeSplit(42L, "/data/file.parquet", Optional.of("/data/delete.parquet"), 100L, 500L);
        String json = mapper.writeValueAsString(split);
        DuckLakeSplit deserialized = mapper.readValue(json, DuckLakeSplit.class);
        assertEquals(split.getDataFileId(), deserialized.getDataFileId());
        assertEquals(split.getDeleteFilePath(), deserialized.getDeleteFilePath());
    }

    @Test
    void testColumnHandle_jsonRoundTrip()
            throws Exception
    {
        DuckLakeColumnHandle handle = new DuckLakeColumnHandle("id", IntegerType.INTEGER, 0, 1L);
        String json = mapper.writeValueAsString(handle);
        DuckLakeColumnHandle deserialized = mapper.readValue(json, DuckLakeColumnHandle.class);
        assertEquals(handle.getColumnName(), deserialized.getColumnName());
        assertEquals(handle.getColumnType(), deserialized.getColumnType());
        assertEquals(handle.getOrdinalPosition(), deserialized.getOrdinalPosition());
        assertEquals(handle.getColumnId(), deserialized.getColumnId());
    }

    @Test
    void testInsertTableHandle_jsonRoundTrip()
            throws Exception
    {
        DuckLakeColumnHandle col = new DuckLakeColumnHandle("id", IntegerType.INTEGER, 0, 1L);
        DuckLakeInsertTableHandle handle = new DuckLakeInsertTableHandle(
                10L,
                List.of(col),
                "/data/",
                "main/",
                "my_table/");
        String json = mapper.writeValueAsString(handle);
        DuckLakeInsertTableHandle deserialized = mapper.readValue(json, DuckLakeInsertTableHandle.class);
        assertEquals(handle.getTableId(), deserialized.getTableId());
        assertEquals(handle.getDataPath(), deserialized.getDataPath());
        assertEquals(handle.getSchemaPath(), deserialized.getSchemaPath());
        assertEquals(handle.getTablePath(), deserialized.getTablePath());
        assertEquals(1, deserialized.getColumns().size());
        assertEquals("id", deserialized.getColumns().get(0).getColumnName());
    }

    @Test
    void testOutputTableHandle_jsonRoundTrip()
            throws Exception
    {
        DuckLakeColumnHandle col = new DuckLakeColumnHandle("name", VarcharType.VARCHAR, 0, 2L);
        DuckLakeOutputTableHandle handle = new DuckLakeOutputTableHandle(
                "test_schema",
                "test_table",
                List.of(col),
                5L,
                3L,
                "550e8400-e29b-41d4-a716-446655440000",
                "/data/",
                "test_schema/",
                "test_table/");
        String json = mapper.writeValueAsString(handle);
        DuckLakeOutputTableHandle deserialized = mapper.readValue(json, DuckLakeOutputTableHandle.class);
        assertEquals(handle.getSchemaName(), deserialized.getSchemaName());
        assertEquals(handle.getTableName(), deserialized.getTableName());
        assertEquals(handle.getTableId(), deserialized.getTableId());
        assertEquals(handle.getSnapshotId(), deserialized.getSnapshotId());
        assertEquals(handle.getTableUuid(), deserialized.getTableUuid());
    }
}
