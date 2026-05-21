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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link DataProvider} and {@link DataWriter} functional interfaces.
 */
@Tag("unit")
class DataProviderAndWriterTest {

  @Test void testDataProviderDefaultReturnsNull() throws IOException {
    Iterator<Map<String, Object>> result =
        DataProvider.DEFAULT.fetch(null, Collections.<String, String>emptyMap());
    assertNull(result);
  }

  @Test void testDataWriterDefaultReturnsNegativeOne() throws IOException {
    long result =
        DataWriter.DEFAULT.write(null, Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(-1, result);
  }

  @Test void testCustomDataProvider() throws IOException {
    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return Collections.<Map<String, Object>>singletonList(
            Collections.<String, Object>singletonMap("key", "value"))
        .iterator();
      }
    };

    Iterator<Map<String, Object>> result =
        provider.fetch(null, Collections.<String, String>emptyMap());
    Map<String, Object> row = result.next();
    assertEquals("value", row.get("key"));
  }

  @Test void testCustomDataWriter() throws IOException {
    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    java.util.List<Map<String, Object>> data = new java.util.ArrayList<Map<String, Object>>();
    data.add(Collections.<String, Object>singletonMap("a", "1"));
    data.add(Collections.<String, Object>singletonMap("a", "2"));

    long result =
        writer.write(null, data.iterator(), Collections.<String, String>emptyMap());
    assertEquals(2, result);
  }
}
