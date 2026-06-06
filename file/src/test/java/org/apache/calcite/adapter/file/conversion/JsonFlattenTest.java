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

import org.apache.calcite.adapter.file.format.json.JsonFlattener;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.adapter.file.FileAdapterTests.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for JSON flattening functionality.
 */
@Tag("unit")
public class JsonFlattenTest {

  @Test void testJsonFlattening() {
    // Test flattened JSON table access
    // Note: Using flattenSeparator="_" in config
    sql("sales-json-flatten", "select * from \"SALES\".\"NESTED_FLAT\"")
        .returns("id=1; name=John Doe; address_street=123 Main St; "
            + "address_city=Anytown; address_zip=12345; tags=customer,vip,active",
            "id=2; name=Jane Smith; address_street=456 Oak Ave; "
            + "address_city=Other City; address_zip=67890; tags=customer,new")
        .ok();
  }

  @Test void testJsonFlatteningSpecificColumns() {
    // Test accessing specific flattened columns
    // Note: Using flattenSeparator="_" in config
    sql("sales-json-flatten",
        "select id, name, address_city from \"SALES\".\"NESTED_FLAT\" where id = 1")
        .returns("id=1; name=John Doe; address_city=Anytown")
        .ok();
  }

  @Test void testJsonFlatteningUnit() {
    // Test default flattener now uses "__" separator
    JsonFlattener flattener = new JsonFlattener();

    java.util.Map<String, Object> input = new java.util.LinkedHashMap<>();
    input.put("name", "John");

    java.util.Map<String, Object> address = new java.util.LinkedHashMap<>();
    address.put("street", "123 Main");
    address.put("city", "Anytown");
    input.put("address", address);

    java.util.List<String> tags = java.util.Arrays.asList("a", "b", "c");
    input.put("tags", tags);

    java.util.Map<String, Object> result = flattener.flatten(input);

    assertEquals("John", result.get("name"));
    assertEquals("123 Main", result.get("address__street"));
    assertEquals("Anytown", result.get("address__city"));
    assertEquals("a,b,c", result.get("tags"));
  }

  @Test void testJsonFlatteningCustomSeparator() {
    // Test custom separator (underscore) that works well with Parquet
    JsonFlattener flattener = new JsonFlattener(",", 3, "", "_");

    java.util.Map<String, Object> input = new java.util.LinkedHashMap<>();
    input.put("name", "John");

    java.util.Map<String, Object> address = new java.util.LinkedHashMap<>();
    address.put("street", "123 Main");
    address.put("city", "Anytown");
    input.put("address", address);

    java.util.List<String> tags = java.util.Arrays.asList("a", "b", "c");
    input.put("tags", tags);

    java.util.Map<String, Object> result = flattener.flatten(input);

    assertEquals("John", result.get("name"));
    assertEquals("123 Main", result.get("address_street"));
    assertEquals("Anytown", result.get("address_city"));
    assertEquals("a,b,c", result.get("tags"));
  }
}
