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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.MaterializeConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The entity sweep resolves each of its four output tables against ref-schema.yaml before writing
 * it. All four are source-less, and the two bridge tables have {@code hooks.enabled: false}
 * because this sweep populates them rather than the per-table lifecycle — which means
 * {@code SchemaConfig.getTables()} legitimately omits them, and resolving through it fails on the
 * last table written after the whole multi-minute resolution build has already run. Cheap to
 * assert here, expensive to discover there.
 */
@Tag("unit")
public class EntityBridgeWriteTargetTest {

  private static final String[] OUTPUT_TABLES = {
      "entity_org_bridge",
      "entity_person_bridge",
      "canonical_org_entity",
      "canonical_person_entity",
  };

  private static MaterializeConfig resolve(String tableName) throws Exception {
    Method m = EntityBridgeListener.class.getDeclaredMethod(
        "standaloneMaterializeConfig", String.class);
    m.setAccessible(true);
    try {
      return (MaterializeConfig) m.invoke(null, tableName);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  @Test void everyOutputTableResolvesFromTheSchemaYaml() throws Exception {
    for (String table : OUTPUT_TABLES) {
      MaterializeConfig mat = resolve(table);
      assertNotNull(mat, table + " must resolve a materialize config");
      assertTrue(mat.isEnabled(), table + " must be materialized");
    }
  }

  /**
   * Columns must arrive from the table's own {@code columns:} block. Without them the writer
   * infers the schema from the first batch and guesses the narrowest type it happens to see.
   */
  @Test void columnsAreFoldedInFromTheTableBlock() throws Exception {
    for (String table : OUTPUT_TABLES) {
      MaterializeConfig mat = resolve(table);
      assertNotNull(mat.getColumns(), table + " must carry declared columns");
      assertFalse(mat.getColumns().isEmpty(), table + " must carry declared columns");
    }
  }

  /** The table name reaches the writer, whether declared in the materialize block or defaulted. */
  @Test void tableNameIsResolved() throws Exception {
    for (String table : OUTPUT_TABLES) {
      MaterializeConfig mat = resolve(table);
      String name = mat.getName() != null && !mat.getName().isEmpty()
          ? mat.getName() : mat.getTargetTableId();
      assertNotNull(name, table + " must resolve a target name");
      assertTrue(name.contains(table), "expected " + table + " in resolved name " + name);
    }
  }

  @Test void anUnknownTableIsReportedNotSilentlySkipped() {
    java.io.IOException e =
        assertThrows(java.io.IOException.class, () -> resolve("no_such_ref_table"));
    assertTrue(e.getMessage().contains("no_such_ref_table"),
        "the message must name the missing table, got: " + e.getMessage());
  }

  /** Resolution is cached; repeated lookups must stay consistent rather than racing the cache. */
  @Test void repeatedResolutionIsStable() throws Exception {
    MaterializeConfig first = resolve("canonical_org_entity");
    MaterializeConfig second = resolve("canonical_org_entity");
    assertEquals(first.getColumns().size(), second.getColumns().size());
  }
}
