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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link SubSchemaFactory} default methods:
 * shouldAutoDownload and getDependencies.
 */
@Tag("unit")
class SubSchemaFactoryDeepTest {

  /** Test implementation of SubSchemaFactory. */
  static class TestSubSchemaFactory implements SubSchemaFactory {
    @Override public String getSchemaResourceName() {
      return "/test/test-schema.yaml";
    }

    @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
      // No hooks for test
    }
  }

  @Test void testShouldAutoDownloadDefaultTrue() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadBooleanTrue() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    operand.put("autoDownload", Boolean.TRUE);
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadBooleanFalse() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    operand.put("autoDownload", Boolean.FALSE);
    assertFalse(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadStringTrue() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    operand.put("autoDownload", "true");
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadStringFalse() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    operand.put("autoDownload", "false");
    assertFalse(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadOtherType() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    operand.put("autoDownload", 42); // Not Boolean or String
    assertTrue(factory.shouldAutoDownload(operand)); // Default is true
  }

  @Test void testGetDependenciesDefault() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    List<String> deps = factory.getDependencies();
    assertNotNull(deps);
    assertTrue(deps.isEmpty());
  }

  @Test void testGetSchemaResourceName() {
    TestSubSchemaFactory factory = new TestSubSchemaFactory();
    assertEquals("/test/test-schema.yaml", factory.getSchemaResourceName());
  }

  private void assertNotNull(Object obj) {
    org.junit.jupiter.api.Assertions.assertNotNull(obj);
  }
}
