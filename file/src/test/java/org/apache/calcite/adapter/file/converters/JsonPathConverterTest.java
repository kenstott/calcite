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
package org.apache.calcite.adapter.file.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonPathConverter}.
 */
@Tag("unit")
class JsonPathConverterTest {

  @TempDir
  File tempDir;

  private final ObjectMapper mapper = new ObjectMapper();

  private File createJsonFile(String name, String content) throws IOException {
    File file = new File(tempDir, name);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }

  @Test void testExtractSimplePath() throws IOException {
    String json = "{\"data\": [{\"id\": 1}, {\"id\": 2}]}";
    File source = createJsonFile("source.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$.data", tempDir);

    assertTrue(output.exists());
    JsonNode result = mapper.readTree(output);
    assertNotNull(result);
    assertTrue(result.isArray());
  }

  @Test void testExtractNestedPath() throws IOException {
    String json = "{\"root\": {\"nested\": {\"value\": 42}}}";
    File source = createJsonFile("nested.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$.root.nested", tempDir);

    assertTrue(output.exists());
    JsonNode result = mapper.readTree(output);
    assertNotNull(result);
  }

  @Test void testExtractNonexistentPath() throws IOException {
    String json = "{\"data\": [1, 2, 3]}";
    File source = createJsonFile("data.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$.nonexistent", tempDir);

    assertTrue(output.exists());
    // Should produce empty object for non-existent path
    JsonNode result = mapper.readTree(output);
    assertNotNull(result);
  }

  @Test void testExtractRootPath() throws IOException {
    String json = "[{\"name\": \"test\"}]";
    File source = createJsonFile("root.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$", tempDir);

    assertTrue(output.exists());
    JsonNode result = mapper.readTree(output);
    assertNotNull(result);
    assertTrue(result.isArray());
  }

  @Test void testExtractFromEmptyObject() throws IOException {
    String json = "{}";
    File source = createJsonFile("empty.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$.data", tempDir);

    assertTrue(output.exists());
  }

  @Test void testExtractFromArray() throws IOException {
    String json = "[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]";
    File source = createJsonFile("array.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$", tempDir);

    assertTrue(output.exists());
    JsonNode result = mapper.readTree(output);
    assertTrue(result.isArray());
  }

  @Test void testExtractAtomicValue() throws IOException {
    String json = "{\"count\": 42}";
    File source = createJsonFile("atomic.json", json);
    File output = new File(tempDir, "output.json");

    JsonPathConverter.extract(source, output, "$.count", tempDir);

    assertTrue(output.exists());
  }
}
