/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cache a seed build produces must hold a row count for every table the model declares, and
 * the build's completeness check must report any that is not.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class IcebergSchemaCacheCompletenessTest {

  private static final String CACHE_DIR_PROPERTY = "iceberg.metadata.cache.directory";

  private String previousCacheDir;
  private Path warehouse;

  @BeforeEach void setUp(@TempDir Path tmp) {
    previousCacheDir = System.getProperty(CACHE_DIR_PROPERTY);
    System.setProperty(CACHE_DIR_PROPERTY, tmp.resolve("cache").toFile().getAbsolutePath());
    IcebergSchemaCache.invalidate();
    warehouse = tmp.resolve("wh");
  }

  @AfterEach void tearDown() {
    IcebergSchemaCache.invalidate();
    if (previousCacheDir == null) {
      System.clearProperty(CACHE_DIR_PROPERTY);
    } else {
      System.setProperty(CACHE_DIR_PROPERTY, previousCacheDir);
    }
  }

  private Table create(String schema, String name, long rows) {
    Table table = new HadoopTables(new Configuration()).create(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())),
        PartitionSpec.unpartitioned(), warehouse.resolve(schema).resolve(name).toString());
    table.newAppend().appendFile(DataFiles.builder(table.spec())
        .withPath(table.location() + "/data/f.parquet").withFileSizeInBytes(10)
        .withRecordCount(rows).withFormat(FileFormat.PARQUET).build()).commit();
    return table;
  }

  @Test void everyDeclaredTableHasItsSnapshotRowCountAndTheCacheRoundTrips() throws Exception {
    Table a = create("law", "lobbying_activities", 2742367L);
    Table b = create("fiscal", "county_migration_flows", 41L);
    IcebergSchemaCache.recordSnapshotStatistics(a.location(), a);
    IcebergSchemaCache.recordSnapshotStatistics(b.location(), b);
    List<String> declared = Arrays.asList("law/lobbying_activities", "fiscal/county_migration_flows");

    assertTrue(IcebergSchemaCache.incompleteTables(declared).isEmpty());
    assertEquals(Double.valueOf(2742367.0), IcebergSchemaCache.lookupRowCount(a.location()));
    assertEquals(Double.valueOf(41.0), IcebergSchemaCache.lookupRowCount(b.location()));

    // Persisted and reloaded: the file a seed bundles still carries the counts.
    assertTrue(IcebergSchemaCache.save());
    IcebergSchemaCache.Document doc = new com.fasterxml.jackson.databind.ObjectMapper()
        .readValue(IcebergSchemaCache.cacheFile(), IcebergSchemaCache.Document.class);
    assertEquals(2, doc.tables.size());
    assertEquals(Double.valueOf(2742367.0), doc.tables.get(a.location()).rowCount);
    assertEquals(Double.valueOf(41.0), doc.tables.get(b.location()).rowCount);
    assertTrue(doc.tables.get(a.location()).statsSnapshotId != null);
  }

  @Test void buildCheckFailsWhenADeclaredTableIsMissingOrHasNoRowCount() {
    Table a = create("law", "lobbying_activities", 5L);
    IcebergSchemaCache.recordSnapshotStatistics(a.location(), a);
    // Schema-only entry, as a live schema read that never captured statistics leaves behind.
    IcebergSchemaCache.record(warehouse.resolve("law/lobbying_filings").toString(),
        a.schema(), null, null);

    List<String> problems = IcebergSchemaCache.incompleteTables(
        Arrays.asList("law/lobbying_activities", "law/lobbying_filings", "fiscal/absent"));

    assertEquals(2, problems.size(), problems.toString());
    assertTrue(problems.get(0).startsWith("law/lobbying_filings") && problems.get(0).contains("row count"));
    assertTrue(problems.get(1).startsWith("fiscal/absent") && problems.get(1).contains("no entry"));
  }
}
