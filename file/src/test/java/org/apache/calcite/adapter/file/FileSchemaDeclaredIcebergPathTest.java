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

import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link FileSchema#declaredIcebergTablePath} decides which tables are registered lazily (no I/O
 * at schema creation) and which are resolved eagerly, an object-store round trip each. It must
 * read {@code materialize.enabled} the way {@link MaterializeConfig} does: absent means enabled.
 */
@Tag("unit")
class FileSchemaDeclaredIcebergPathTest {

  @TempDir Path tempDir;

  private static Map<String, Object> table(String name, Object enabled, String format) {
    Map<String, Object> iceberg = new LinkedHashMap<>();
    iceberg.put("warehousePath", "s3://bucket/crime");
    iceberg.put("tableName", name);
    Map<String, Object> materialize = new LinkedHashMap<>();
    if (enabled != null) {
      materialize.put("enabled", enabled);
    }
    materialize.put("format", format);
    materialize.put("iceberg", iceberg);
    Map<String, Object> def = new LinkedHashMap<>();
    def.put("name", name);
    def.put("materialize", materialize);
    return def;
  }

  private FileSchema schema(List<Map<String, Object>> tables) {
    SchemaPlus parent = mock(SchemaPlus.class);
    when(parent.getName()).thenReturn("root");
    return new FileSchema(parent, "declared_iceberg_path", tempDir.toFile(), tables,
        new ExecutionEngineConfig());
  }

  @Test void tableThatOmitsEnabledIsDeferredLikeOneThatSaysTrue() {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(table("omitted", null, "iceberg"));
    tables.add(table("explicit", Boolean.TRUE, "iceberg"));
    FileSchema schema = schema(tables);

    assertEquals("s3://bucket/crime/omitted", schema.declaredIcebergTablePath("omitted"));
    assertEquals("s3://bucket/crime/explicit", schema.declaredIcebergTablePath("explicit"));
  }

  @Test void omittedEnabledMeansEnabledToTheMaterializeConfigToo() {
    Map<String, Object> materialize = new LinkedHashMap<>();
    materialize.put("format", "iceberg");
    Map<String, Object> output = new LinkedHashMap<>();
    output.put("pattern", "type=agency/");
    materialize.put("output", output);

    assertTrue(MaterializeConfig.fromMap(materialize).isEnabled());
  }

  @Test void disabledNonIcebergAndUndeclaredTablesAreNotDeferred() {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(table("disabled", Boolean.FALSE, "iceberg"));
    tables.add(table("parquet", null, "parquet"));
    Map<String, Object> plain = new LinkedHashMap<>();
    plain.put("name", "plain");
    tables.add(plain);
    FileSchema schema = schema(tables);

    assertNull(schema.declaredIcebergTablePath("disabled"));
    assertNull(schema.declaredIcebergTablePath("parquet"));
    assertNull(schema.declaredIcebergTablePath("plain"));
    assertNull(schema.declaredIcebergTablePath("no_such_table"));
  }
}
