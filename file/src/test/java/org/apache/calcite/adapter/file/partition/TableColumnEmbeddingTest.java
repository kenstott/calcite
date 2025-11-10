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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for TableColumn embedding configuration methods.
 */
public class TableColumnEmbeddingTest {

  @Test
  void testBasicColumnWithoutEmbedding() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn("name", "string", true, "Test column");

    assertFalse(column.isComputed());
    assertFalse(column.hasEmbeddingConfig());
    assertNull(column.getEmbeddingSourceColumns());
    assertFalse(column.isVectorType());
  }

  @Test
  void testComputedColumnWithSingleSourceColumn() {
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("sourceColumn", "series_name");
    embeddingConfig.put("provider", "onnx");

    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "series_name_embedding", "array<double>", true, "Embedding column",
            true, embeddingConfig);

    assertTrue(column.isComputed());
    assertTrue(column.hasEmbeddingConfig());
    assertTrue(column.isVectorType());

    String[] sourceColumns = column.getEmbeddingSourceColumns();
    assertNotNull(sourceColumns);
    assertEquals(1, sourceColumns.length);
    assertEquals("series_name", sourceColumns[0]);

    assertEquals("onnx", column.getEmbeddingProvider());
    assertEquals("natural", column.getEmbeddingTemplate());
    assertEquals(", ", column.getEmbeddingSeparator());
    assertTrue(column.getEmbeddingExcludeNull());
  }

  @Test
  void testComputedColumnWithMultipleSourceColumns() {
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("sourceColumns", Arrays.asList("series_name", "category", "subcategory"));
    embeddingConfig.put("template", "natural");
    embeddingConfig.put("separator", ", ");
    embeddingConfig.put("excludeNull", true);

    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "row_embedding", "array<double>", true, "Row-level embedding",
            true, embeddingConfig);

    assertTrue(column.isComputed());
    assertTrue(column.hasEmbeddingConfig());
    assertTrue(column.isVectorType());

    String[] sourceColumns = column.getEmbeddingSourceColumns();
    assertNotNull(sourceColumns);
    assertEquals(3, sourceColumns.length);
    assertArrayEquals(new String[]{"series_name", "category", "subcategory"}, sourceColumns);

    assertEquals("natural", column.getEmbeddingTemplate());
    assertEquals(", ", column.getEmbeddingSeparator());
    assertTrue(column.getEmbeddingExcludeNull());
  }

  @Test
  void testEmbeddingConfigDefaults() {
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("sourceColumn", "text");

    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "text_embedding", "array<double>", true, "Embedding",
            true, embeddingConfig);

    // Test defaults
    assertEquals("natural", column.getEmbeddingTemplate());
    assertEquals(", ", column.getEmbeddingSeparator());
    assertTrue(column.getEmbeddingExcludeNull());
    assertEquals("onnx", column.getEmbeddingProvider());
  }

  @Test
  void testEmbeddingConfigCustomValues() {
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("sourceColumns", Arrays.asList("field1", "field2"));
    embeddingConfig.put("template", "simple");
    embeddingConfig.put("separator", " | ");
    embeddingConfig.put("excludeNull", false);
    embeddingConfig.put("provider", "custom");

    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "custom_embedding", "array<double>", false, "Custom embedding",
            true, embeddingConfig);

    assertEquals("simple", column.getEmbeddingTemplate());
    assertEquals(" | ", column.getEmbeddingSeparator());
    assertFalse(column.getEmbeddingExcludeNull());
    assertEquals("custom", column.getEmbeddingProvider());
  }

  @Test
  void testDeprecatedGetEmbeddingSourceColumn() {
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("sourceColumn", "text");

    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "text_embedding", "array<double>", true, "Embedding",
            true, embeddingConfig);

    // Test deprecated method returns first element
    assertEquals("text", column.getEmbeddingSourceColumn());
  }

  @Test
  void testIsVectorType() {
    PartitionedTableConfig.TableColumn arrayDoubleCol =
        new PartitionedTableConfig.TableColumn("vec", "array<double>", true, "Vector");
    assertTrue(arrayDoubleCol.isVectorType());

    PartitionedTableConfig.TableColumn arrayIntCol =
        new PartitionedTableConfig.TableColumn("arr", "array<int>", true, "Array");
    assertTrue(arrayIntCol.isVectorType());

    PartitionedTableConfig.TableColumn stringCol =
        new PartitionedTableConfig.TableColumn("str", "string", true, "String");
    assertFalse(stringCol.isVectorType());

    PartitionedTableConfig.TableColumn intCol =
        new PartitionedTableConfig.TableColumn("num", "int", false, "Number");
    assertFalse(intCol.isVectorType());
  }
}
