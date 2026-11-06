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

import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code getStatistic} must never walk manifests or data files. A table the cache does not
 * describe reads the snapshot summary's total-records, records it and says the seed is stale; a
 * table the cache describes touches the table not at all.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class IcebergTableStatisticsTest {

  private static final String CACHE_DIR_PROPERTY = "iceberg.metadata.cache.directory";

  private String previousCacheDir;
  private Path warehouse;
  private final List<String> warnings = Collections.synchronizedList(new ArrayList<String>());
  private AbstractAppender appender;

  @BeforeEach void setUp(@TempDir Path tmp) {
    previousCacheDir = System.getProperty(CACHE_DIR_PROPERTY);
    File cacheDir = tmp.resolve("cache").toFile();
    System.setProperty(CACHE_DIR_PROPERTY, cacheDir.getAbsolutePath());
    IcebergSchemaCache.invalidate();
    warehouse = tmp.resolve("wh");

    appender = new AbstractAppender("stats-warn", null, null, true, Property.EMPTY_ARRAY) {
      @Override public void append(LogEvent event) {
        if (event.getLevel() == Level.WARN) {
          warnings.add(event.getMessage().getFormattedMessage());
        }
      }
    };
    appender.start();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().addAppender(appender);
    ctx.getConfiguration().getLoggerConfig(IcebergTable.class.getName()).addAppender(appender,
        Level.WARN, null);
    ctx.updateLoggers();
  }

  @AfterEach void tearDown() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().getLoggerConfig(IcebergTable.class.getName())
        .removeAppender(appender.getName());
    ctx.updateLoggers();
    appender.stop();
    IcebergSchemaCache.invalidate();
    if (previousCacheDir == null) {
      System.clearProperty(CACHE_DIR_PROPERTY);
    } else {
      System.setProperty(CACHE_DIR_PROPERTY, previousCacheDir);
    }
  }

  /** A real local Iceberg table with two data files of 7 and 5 rows (summary total 12). */
  private Table twelveRowTable(String name) {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    Table table = new HadoopTables(new Configuration())
        .create(schema, PartitionSpec.unpartitioned(), warehouse.resolve(name).toString());
    table.newAppend().appendFile(dataFile(table, name + "-a", 7)).commit();
    table.newAppend().appendFile(dataFile(table, name + "-b", 5)).commit();
    return table;
  }

  private static DataFile dataFile(Table table, String file, long records) {
    return DataFiles.builder(table.spec())
        .withPath(table.location() + "/data/" + file + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(records)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /** The table, with every manifest/data-file walk counted and refused. */
  private static Table walkRefusing(Table real, final AtomicInteger walks) {
    return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
        (proxy, method, args) -> {
          if (method.getName().equals("newScan") || method.getName().equals("newBatchScan")) {
            walks.incrementAndGet();
            throw new AssertionError("getStatistic walked the table: " + method.getName());
          }
          try {
            return method.invoke(real, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test void uncachedTableReadsTotalRecordsRecordsAndWarnsWithoutWalking() {
    Table real = twelveRowTable("uncached");
    String path = real.location();
    AtomicInteger walks = new AtomicInteger();
    IcebergTable table = new IcebergTable(walkRefusing(real, walks), Sources.of(new File(path)));

    Statistic stat = table.getStatistic();

    assertEquals(12.0, stat.getRowCount(), "row count is the snapshot summary's total-records");
    assertEquals(0, walks.get(), "no manifest/data-file walk");
    assertEquals(Double.valueOf(12.0), IcebergSchemaCache.lookupRowCount(path), "recorded");
    boolean warned = false;
    for (String w : warnings) {
      warned |= w.contains(path) && w.contains("stale");
    }
    assertTrue(warned, "a WARNING naming the table and saying the seed is stale; got " + warnings);
  }

  @Test void tableTheCacheDescribesIsNotTouched() {
    Table real = twelveRowTable("cached");
    String path = real.location();
    IcebergSchemaCache.recordStatistics(path, 1L, 99.0);
    AtomicInteger walks = new AtomicInteger();
    IcebergTable table = new IcebergTable(walkRefusing(real, walks), Sources.of(new File(path)));

    assertEquals(99.0, table.getStatistic().getRowCount());
    assertEquals(0, walks.get());
    assertTrue(warnings.isEmpty(), "a described table is not a stale-seed case: " + warnings);
  }

  @Test void recordSnapshotStatisticsStoresRowCountSnapshotAndNdvDescription() {
    Table real = twelveRowTable("recorded");
    String path = real.location();

    assertEquals(12.0, IcebergSchemaCache.recordSnapshotStatistics(path, real));

    assertEquals(Double.valueOf(12.0), IcebergSchemaCache.lookupRowCount(path));
    assertNotNull(IcebergSchemaCache.lookupNdv(path),
        "NDV is recorded (empty when the table publishes none) so no runtime lookup is needed");
    assertTrue(IcebergSchemaCache.incompleteTables(Collections.singletonList("wh/recorded"))
        .isEmpty());
  }
}
