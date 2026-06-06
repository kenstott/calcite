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
