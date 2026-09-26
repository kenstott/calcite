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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link EtlPipelineConfig#withDimensions} carries every field.
 *
 * <p>The copy is made when a listener resolves a table's dimensions at run time. A field left
 * out of it is silently reset to its default for every such table, so the check walks the
 * class's own fields by reflection: a field added to {@link EtlPipelineConfig} without being
 * carried by the copy fails here, and so does a field the fixture does not populate.
 */
@Tag("unit")
public class EtlPipelineConfigCopyTest {

  private static Map<String, Object> map(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(String.valueOf(kv[i]), kv[i + 1]);
    }
    return m;
  }

  /** A config whose every field holds something other than its default. */
  private static EtlPipelineConfig fullyPopulated() {
    return EtlPipelineConfig.fromMap(map(
        "name", "econ.demo",
        "enabled", false,
        "source", map("type", "http", "url", "https://example.invalid/data"),
        "download", map("bulkDownload", "demo_bulk"),
        "materialize", map("enabled", false),
        "dimensions", map("region", Arrays.asList("east", "west")),
        "columns", Collections.singletonList(map("name", "c1", "type", "string")),
        "errorHandling", map("transientRetries", 5),
        "hooks", map("responseTransformer", "example.Transformer"),
        "freshness", map("type", "hash"),
        "releaseWindow", map("dow", Collections.singletonList(0)),
        "dataset_type", "snapshot",
        "backfill_period", "2y",
        "dqRowLimit", 100,
        "lookbackPeriods", 6));
  }

  private static Map<String, DimensionConfig> resolved() {
    return EtlPipelineConfig.fromMap(map(
        "name", "other",
        "source", map("type", "http", "url", "https://example.invalid/data"),
        "materialize", map("enabled", false),
        "dimensions", map("state", Arrays.asList("CA", "NY")))).getDimensions();
  }

  private static Field[] instanceFields() {
    java.util.List<Field> fields = new java.util.ArrayList<Field>();
    for (Field f : EtlPipelineConfig.class.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic()) {
        f.setAccessible(true);
        fields.add(f);
      }
    }
    return fields.toArray(new Field[0]);
  }

  /** Guards the fixture itself: a field left at its default could not show a dropped copy. */
  @Test void fixtureSetsEveryFieldToANonDefault() throws Exception {
    EtlPipelineConfig source = fullyPopulated();
    for (Field f : instanceFields()) {
      Object value = f.get(source);
      assertNotNull(value, "fixture leaves '" + f.getName() + "' unset");
      if (value instanceof Integer) {
        assertFalse(((Integer) value).intValue() == 0,
            "fixture leaves '" + f.getName() + "' at 0");
      }
    }
    assertFalse(source.isEnabled(), "enabled defaults to true, so the fixture must set false");
  }

  @Test void copyCarriesEveryFieldExceptDimensions() throws Exception {
    EtlPipelineConfig source = fullyPopulated();
    Map<String, DimensionConfig> replacement = resolved();
    EtlPipelineConfig copy = source.withDimensions(replacement);

    for (Field f : instanceFields()) {
      if (f.getName().equals("dimensions")) {
        continue;
      }
      Object expected = f.get(source);
      Object actual = f.get(copy);
      if (expected instanceof Collection || expected instanceof Map
          || expected instanceof Number || expected instanceof Boolean
          || expected instanceof String) {
        assertEquals(expected, actual, "copy dropped or changed '" + f.getName() + "'");
      } else {
        assertSame(expected, actual, "copy dropped or changed '" + f.getName() + "'");
      }
    }
  }

  @Test void copyReplacesTheDimensions() {
    EtlPipelineConfig source = fullyPopulated();
    EtlPipelineConfig copy = source.withDimensions(resolved());

    assertTrue(copy.getDimensions().containsKey("state"));
    assertFalse(copy.getDimensions().containsKey("region"));
    assertTrue(source.getDimensions().containsKey("region"), "the original must be unchanged");
  }

  /** The specific field a hand-written copy had been dropping. */
  @Test void copyKeepsLookbackPeriods() {
    assertEquals(Integer.valueOf(6), fullyPopulated().withDimensions(resolved()).getLookbackPeriods());
  }
}
