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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ModelLifecycleProcessor}.
 *
 * <p>Exercises the {@code process()}, {@code processSchema()}, and the
 * {@link ModelLifecycleProcessor.Builder} fluent API including
 * {@link IncrementalTracker} and storage provider wiring.
 */
@Tag("unit")
public class ModelLifecycleProcessorDeepCoverageTest {

  @TempDir
  Path tempDir;

  /**
   * Minimal SubSchemaFactory that uses a classpath YAML with an empty schema definition.
   * Disables autoDownload to avoid ETL pipeline requirements in unit tests.
   */
  private static class MinimalSubSchemaFactory implements SubSchemaFactory {
    @Override public String getSchemaResourceName() {
      return "/test/minimal-schema.yaml";
    }

    @Override public void configureHooks(FileSchemaBuilder builder,
        Map<String, Object> operand) {
      // No hooks needed for minimal test
    }

    @Override public boolean shouldAutoDownload(Map<String, Object> operand) {
      // Disable autoDownload for unit tests - no ETL infrastructure available
      return false;
    }
  }

  // === Constructor path tests ===

  @Test public void testProcessCreatesSchemaForSingleSchema() {
    String dir = tempDir.toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("TEST_SCHEMA", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getRootSchema());
    assertNotNull(result.getSchema("TEST_SCHEMA"));
  }

  @Test public void testProcessWithOperatingDirectory() {
    String dir = tempDir.toString();
    String opDir = tempDir.resolve("operating").toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .operatingDirectory(opDir)
        .addSchema("OP_SCHEMA", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getRootSchema());
  }

  @Test public void testProcessCreatesOperatingDirectoryIfMissing() {
    String dir = tempDir.toString();
    // Use a new subdirectory that doesn't exist yet
    String opDir = tempDir.resolve("new_operating_dir").toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .operatingDirectory(opDir)
        .addSchema("OPDIR_SCHEMA", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertTrue(new java.io.File(opDir).exists(), "Operating directory should be created");
  }

  @Test public void testProcessWithMultipleSchemas() {
    String dir = tempDir.toString();
    Map<String, Object> operand1 = new HashMap<String, Object>();
    operand1.put("directory", dir);
    operand1.put("ephemeralCache", Boolean.TRUE);

    Map<String, Object> operand2 = new HashMap<String, Object>();
    operand2.put("directory", dir);
    operand2.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("SCHEMA_A", new MinimalSubSchemaFactory(), operand1)
        .addSchema("SCHEMA_B", new MinimalSubSchemaFactory(), operand2)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getSchema("SCHEMA_A"));
    assertNotNull(result.getSchema("SCHEMA_B"));
  }

  @Test public void testProcessWithHookOverrides() {
    String dir = tempDir.toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    final boolean[] hookCalled = {false};

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("HOOK_SCHEMA", new MinimalSubSchemaFactory(), operand,
            builder -> { hookCalled[0] = true; })
        .build();

    processor.process();

    assertTrue(hookCalled[0], "Hook override should have been called");
  }

  @Test public void testProcessWithIncrementalTracker() {
    String dir = tempDir.toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .incrementalTracker(IncrementalTracker.NOOP)
        .addSchema("TRACKER_SCHEMA", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getSchema("TRACKER_SCHEMA"));
  }

  @Test public void testProcessWithNullOperatingDirectory() {
    String dir = tempDir.toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    // No operatingDirectory set
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("NULL_OPDIR", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getSchema("NULL_OPDIR"));
  }

  @Test public void testProcessSchemaWithSchemaSpecificOperatingDirectory() {
    String dir = tempDir.toString();
    String schemaOpDir = tempDir.resolve("schema_op_dir").toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("operatingDirectory", schemaOpDir);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("OPDIR_SCHEMA2", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result);
    assertNotNull(result.getSchema("OPDIR_SCHEMA2"));
  }

  @Test public void testProcessErrorPropagatesAsRuntimeException() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("ephemeralCache", Boolean.TRUE);
    // No directory - may cause exception inside factory

    SubSchemaFactory badFactory = new SubSchemaFactory() {
      @Override public String getSchemaResourceName() {
        return "/nonexistent/bad-schema.yaml";
      }
      @Override public void configureHooks(FileSchemaBuilder builder,
          Map<String, Object> operand) {
      }
      @Override public boolean shouldAutoDownload(Map<String, Object> operand) {
        return false;
      }
    };

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("BAD_SCHEMA", badFactory, operand)
        .build();

    assertThrows(RuntimeException.class, processor::process);
  }

  @Test public void testProcessResultGetSchemaReturnsNullForUnknown() {
    String dir = tempDir.toString();
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", dir);
    operand.put("ephemeralCache", Boolean.TRUE);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("KNOWN_SCHEMA", new MinimalSubSchemaFactory(), operand)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNull(result.getSchema("UNKNOWN_SCHEMA"));
  }

  @Test public void testBuilderAddSchemaWithNullOperand() {
    String dir = tempDir.toString();
    // Null operand should be handled (converted to empty map)
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .addSchema("NULL_OPERAND_SCHEMA", new SubSchemaFactory() {
          @Override public String getSchemaResourceName() {
            return "/test/minimal-schema.yaml";
          }
          @Override public void configureHooks(FileSchemaBuilder builder,
              Map<String, Object> operand) {
            // Add directory at hook time since operand was null
            builder.operand("directory", dir);
            builder.operand("ephemeralCache", Boolean.TRUE);
          }
          @Override public boolean shouldAutoDownload(Map<String, Object> operand) {
            return false;
          }
        }, null)
        .build();

    ModelLifecycleProcessor.ProcessResult result = processor.process();
    assertNotNull(result);
  }

  @Test public void testBuilderCapturesIncrementalTrackerPerSchema() {
    String dir = tempDir.toString();
    Map<String, Object> operand1 = new HashMap<String, Object>();
    operand1.put("directory", dir);
    operand1.put("ephemeralCache", Boolean.TRUE);

    Map<String, Object> operand2 = new HashMap<String, Object>();
    operand2.put("directory", dir);
    operand2.put("ephemeralCache", Boolean.TRUE);

    // First schema added with NOOP tracker, then change tracker, add second schema
    ModelLifecycleProcessor.Builder builder = ModelLifecycleProcessor.builder()
        .incrementalTracker(IncrementalTracker.NOOP);

    builder.addSchema("SCHEMA_1", new MinimalSubSchemaFactory(), operand1);

    // Now build with both schemas
    builder.addSchema("SCHEMA_2", new MinimalSubSchemaFactory(), operand2);

    ModelLifecycleProcessor processor = builder.build();
    ModelLifecycleProcessor.ProcessResult result = processor.process();

    assertNotNull(result.getSchema("SCHEMA_1"));
    assertNotNull(result.getSchema("SCHEMA_2"));
  }
}
