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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.ColumnTypeInfo;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.TypeInferenceConfig;
import org.apache.calcite.adapter.file.schema.SchemaStrategy;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recode of weak file-adapter CSV/schema tests to exact assertions, exercising the
 * engine-independent {@link CsvTypeInferrer} and {@link SchemaStrategy} directly. FILE-157 pins the
 * empty-source and header-only edge cases; FILE-073 pins the per-format schema-resolution defaults.
 */
@Tag("unit")
public class CsvSchemaRequirementsTest {

  private static final TypeInferenceConfig CONF =
      new TypeInferenceConfig(true, 1.0, 1000, 0.95, true, true, true, true, 0.0);

  private static List<ColumnTypeInfo> infer(Path dir, String csv) throws Exception {
    Path f = Files.createTempFile(dir, "infer", ".csv");
    Files.write(f, csv.getBytes(StandardCharsets.UTF_8));
    return CsvTypeInferrer.inferTypes(Sources.of(f.toFile()), CONF, "UNCHANGED");
  }

  @Test @Tag("FILE-157") void emptySourceReturnsEmptyTypeList(@TempDir Path dir) throws Exception {
    assertTrue(infer(dir, "").isEmpty(), "empty source -> empty type list");
  }

  @Test @Tag("FILE-157") void headerOnlyCsvIsAllNullableVarchar(@TempDir Path dir) throws Exception {
    List<ColumnTypeInfo> types = infer(dir, "id,name,score\n");
    assertEquals(3, types.size(), "one ColumnTypeInfo per header column");
    for (int i = 0; i < types.size(); i++) {
      assertEquals(SqlTypeName.VARCHAR, types.get(i).inferredType, "header-only column " + i + " -> VARCHAR");
      assertTrue(types.get(i).nullable, "header-only column " + i + " is nullable");
    }
  }

  @Test @Tag("FILE-073") void defaultSchemaStrategyPerFormatResolution() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, strategy.getParquetStrategy(),
        "default parquet strategy");
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, strategy.getCsvStrategy(),
        "default csv strategy");
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, strategy.getJsonStrategy(),
        "default json strategy");
  }
}
