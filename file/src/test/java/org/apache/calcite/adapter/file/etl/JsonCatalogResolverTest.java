/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
