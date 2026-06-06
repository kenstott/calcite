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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonCatalogResolver}.
 */
@Tag("unit")
class JsonCatalogResolverTest {

  @Test void testResolveSimpleArray() {
    // Create a test JSON resource in classpath
    // Use the test resource that should be at the classpath
    // We'll test with a dynamically created test resource via class
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-simple-array.json",
        "countries");

    assertNotNull(values);
    assertEquals(3, values.size());
    assertTrue(values.contains("USA"));
    assertTrue(values.contains("CAN"));
    assertTrue(values.contains("MEX"));
  }

  @Test void testResolveNestedPath() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-nested.json",
        "groups.G7.countries");

    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.contains("USA"));
    assertTrue(values.contains("GBR"));
  }

  @Test void testResolveArrayWildcard() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-array-objects.json",
        "items[*].code");

    assertNotNull(values);
    assertEquals(3, values.size());
    assertTrue(values.contains("A"));
    assertTrue(values.contains("B"));
    assertTrue(values.contains("C"));
  }

  @Test void testResolveEmptyPath() {
    // Empty path on an array root
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-root-array.json",
        "");

    assertNotNull(values);
    assertEquals(3, values.size());
  }

  @Test void testResolveNullPath() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-root-array.json",
        null);

    assertNotNull(values);
    assertEquals(3, values.size());
  }

  @Test void testResolveObjectWithCodeField() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-objects-with-code.json",
        "items[*]");

    assertNotNull(values);
    // Objects with "code" field should extract the code value
    assertTrue(values.contains("US"));
    assertTrue(values.contains("CA"));
  }

  @Test void testResolveNumericValues() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-numeric.json",
        "years");

    assertNotNull(values);
    assertEquals(3, values.size());
    assertTrue(values.contains("2020"));
    assertTrue(values.contains("2021"));
    assertTrue(values.contains("2022"));
  }

  @Test void testResolveNotFoundResource() {
    assertThrows(RuntimeException.class,
        () -> JsonCatalogResolver.resolve(
            JsonCatalogResolverTest.class,
            "/nonexistent-resource.json",
            "path"));
  }

  @Test void testResolveMissingPath() {
    List<String> values =
        JsonCatalogResolver.resolve(JsonCatalogResolverTest.class,
        "/etl-test/catalog-simple-array.json",
        "nonexistent.path");

    assertNotNull(values);
    assertTrue(values.isEmpty());
  }
}
