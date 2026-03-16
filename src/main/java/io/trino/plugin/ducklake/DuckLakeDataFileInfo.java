package io.trino.plugin.ducklake;

import java.util.Optional;

record DuckLakeDataFileInfo(
        long dataFileId,
        String dataFilePath,
        Optional<String> deleteFilePath,
        long rowIdStart,
        long recordCount)
{}
