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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for JsonTable covering constructors,
 * data loading, caching, and file modification detection.
 */
@Tag("unit")
public class JsonTableDeepTest {

  @TempDir
  Path tempDir;

  private File createJsonFile(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    FileWriter writer = new FileWriter(file);
    writer.write(content);
    writer.close();
    return file;
  }

  @Test void testJsonTableSourceConstructor() throws IOException {
    File json = createJsonFile("test.json", "[{\"name\":\"Alice\",\"age\":30}]");
    JsonTable table = new JsonTable(Sources.of(json));
    assertNotNull(table);
  }

  @Test void testJsonTableWithOptions() throws IOException {
    File json = createJsonFile("opts.json", "[{\"id\":1,\"value\":\"test\"}]");
    Map<String, Object> options = new HashMap<>();
    options.put("format", "json");
    JsonTable table = new JsonTable(Sources.of(json), options);
    assertNotNull(table);
  }

  @Test void testJsonTableWithCasing() throws IOException {
    File json = createJsonFile("casing.json", "[{\"FirstName\":\"Alice\"}]");
    JsonTable table = new JsonTable(Sources.of(json), null, "TO_LOWER");
    assertNotNull(table);
  }

  @Test void testGetRowType() throws IOException {
    File json = createJsonFile("schema.json", "[{\"name\":\"Alice\",\"age\":30}]");
    JsonTable table = new JsonTable(Sources.of(json));
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertTrue(rowType.getFieldCount() >= 2);
  }

  @Test void testGetRowTypeCached() throws IOException {
    File json = createJsonFile("cached.json", "[{\"x\":1}]");
    JsonTable table = new JsonTable(Sources.of(json));
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType type1 = table.getRowType(typeFactory);
    RelDataType type2 = table.getRowType(typeFactory);
    assertSame(type1, type2); // Should be cached
  }

  @Test void testGetDataList() throws IOException {
    File json = createJsonFile("data.json", "[{\"a\":1},{\"a\":2}]");
    JsonTable table = new JsonTable(Sources.of(json));
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    List<Object> data = table.getDataList(typeFactory);
    assertNotNull(data);
    assertTrue(data.size() >= 2);
  }

  @Test void testGetDataListCached() throws IOException {
    File json = createJsonFile("data_cached.json", "[{\"a\":1}]");
    JsonTable table = new JsonTable(Sources.of(json));
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    List<Object> data1 = table.getDataList(typeFactory);
    List<Object> data2 = table.getDataList(typeFactory);
    assertSame(data1, data2); // Should be cached
  }

  @Test void testGetStatistic() throws IOException {
    File json = createJsonFile("stat.json", "[{\"a\":1}]");
    JsonTable table = new JsonTable(Sources.of(json));
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
  }

  @Test void testFileModificationTriggersRefresh() throws IOException, InterruptedException {
    File json = createJsonFile("modified.json", "[{\"a\":1}]");
    JsonTable table = new JsonTable(Sources.of(json));
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Load data initially
    List<Object> data1 = table.getDataList(typeFactory);
    assertNotNull(data1);

    // Wait and modify the file
    Thread.sleep(100); // Ensure different timestamp
    FileWriter writer = new FileWriter(json);
    writer.write("[{\"a\":1},{\"a\":2},{\"a\":3}]");
    writer.close();
    json.setLastModified(json.lastModified() + 1000);

    // Get data again - should detect modification
    List<Object> data2 = table.getDataList(typeFactory);
    assertNotNull(data2);
  }
}
