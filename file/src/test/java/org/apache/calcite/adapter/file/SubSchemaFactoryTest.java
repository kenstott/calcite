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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SubSchemaFactory} default methods.
 */
@Tag("unit")
class SubSchemaFactoryTest {

  /** Test implementation of SubSchemaFactory. */
  private static class TestSubSchemaFactory implements SubSchemaFactory {
    @Override public String getSchemaResourceName() {
      return "/test/schema.yaml";
    }

    @Override public void configureHooks(FileSchemaBuilder builder,
        Map<String, Object> operand) {
      // No-op for testing
    }
  }

  @Test void testShouldAutoDownloadDefaultTrue() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadWithBooleanTrue() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("autoDownload", Boolean.TRUE);
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadWithBooleanFalse() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("autoDownload", Boolean.FALSE);
    assertFalse(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadWithStringTrue() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("autoDownload", "true");
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadWithStringFalse() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("autoDownload", "false");
    assertFalse(factory.shouldAutoDownload(operand));
  }

  @Test void testShouldAutoDownloadWithOtherType() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("autoDownload", Integer.valueOf(1));
    // Non-boolean, non-string defaults to true
    assertTrue(factory.shouldAutoDownload(operand));
  }

  @Test void testGetDependenciesDefault() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    List<String> deps = factory.getDependencies();
    assertNotNull(deps);
    assertTrue(deps.isEmpty());
  }

  @Test void testGetSchemaResourceName() {
    SubSchemaFactory factory = new TestSubSchemaFactory();
    assertEquals("/test/schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testCustomDependencies() {
    SubSchemaFactory factory = new SubSchemaFactory() {
      @Override public String getSchemaResourceName() {
        return "/custom/schema.yaml";
      }

      @Override public void configureHooks(FileSchemaBuilder builder,
          Map<String, Object> operand) {
        // No-op
      }

      @Override public List<String> getDependencies() {
        return Collections.singletonList("reference_data");
      }
    };

    List<String> deps = factory.getDependencies();
    assertEquals(1, deps.size());
    assertEquals("reference_data", deps.get(0));
  }
}
