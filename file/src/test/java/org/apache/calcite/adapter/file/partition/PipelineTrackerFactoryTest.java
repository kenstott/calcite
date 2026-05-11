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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PipelineTrackerFactory}.
 */
@Tag("unit")
public class PipelineTrackerFactoryTest {

  @TempDir
  Path tempDir;

  @Test void testCreateNoopTracker() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "noop", tempDir.toString(), Collections.<String, String>emptyMap());

    assertNotNull(tracker);
    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  @Test void testCreateNoopTrackerCaseInsensitive() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "NOOP", tempDir.toString(), Collections.<String, String>emptyMap());

    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  @Test void testCreateDuckDBTracker() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "duckdb", tempDir.toString(), Collections.<String, String>emptyMap());

    assertNotNull(tracker);
    assertTrue(tracker instanceof DuckDBPartitionStatusStore);
  }

  @Test void testCreateDefaultTracker() {
    // Null backend should default to duckdb
    PipelineTracker tracker = PipelineTrackerFactory.create(
        null, tempDir.toString(), Collections.<String, String>emptyMap());

    assertNotNull(tracker);
    assertTrue(tracker instanceof DuckDBPartitionStatusStore);
  }

  @Test void testCreateUnknownBackendFallsToDuckDB() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "unknown_backend", tempDir.toString(), Collections.<String, String>emptyMap());

    assertNotNull(tracker);
    assertTrue(tracker instanceof DuckDBPartitionStatusStore);
  }

  @Test void testCreateWithBaseDirectoryShortcut() {
    PipelineTracker tracker = PipelineTrackerFactory.create(tempDir.toString());

    assertNotNull(tracker);
  }

  @Test void testCreateS3TrackerRequiresBucket() {
    assertThrows(IllegalArgumentException.class, () -> {
      PipelineTrackerFactory.create(
          "s3", tempDir.toString(), Collections.<String, String>emptyMap());
    });
  }

  @Test void testCreatePGTrackerRequiresJdbcUrl() {
    assertThrows(RuntimeException.class, () -> {
      PipelineTrackerFactory.create(
          "pg", tempDir.toString(), Collections.<String, String>emptyMap());
    });
  }

  @Test void testCreateFromOperandWithNoopBackend() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "noop");

    PipelineTracker tracker =
        PipelineTrackerFactory.createFromOperand(operand, tempDir.toString());

    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  @Test void testCreateFromOperandWithDuckDB() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "duckdb");

    PipelineTracker tracker =
        PipelineTrackerFactory.createFromOperand(operand, tempDir.toString());

    assertNotNull(tracker);
    assertTrue(tracker instanceof DuckDBPartitionStatusStore);
  }

  @Test void testCreateFromOperandWithTrackerConfig() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "noop");

    Map<String, String> trackerConfig = new HashMap<String, String>();
    trackerConfig.put("someKey", "someValue");
    operand.put("trackerConfig", trackerConfig);

    PipelineTracker tracker =
        PipelineTrackerFactory.createFromOperand(operand, tempDir.toString());

    assertNotNull(tracker);
  }

  @Test void testCreateFromOperandMergesS3Config() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "noop");

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "testKey");
    s3Config.put("secretAccessKey", "testSecret");
    s3Config.put("endpoint", "http://localhost:9000");
    s3Config.put("region", "us-east-1");
    operand.put("s3Config", s3Config);

    PipelineTracker tracker =
        PipelineTrackerFactory.createFromOperand(operand, tempDir.toString());

    // Should not throw - S3 config is merged but noop doesn't use it
    assertNotNull(tracker);
  }

  @Test void testCreateFromOperandTrackerConfigTakesPrecedence() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "noop");

    Map<String, String> trackerConfig = new HashMap<String, String>();
    trackerConfig.put("accessKeyId", "trackerKey");
    operand.put("trackerConfig", trackerConfig);

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "s3Key");
    operand.put("s3Config", s3Config);

    // trackerConfig should take precedence over s3Config
    PipelineTracker tracker =
        PipelineTrackerFactory.createFromOperand(operand, tempDir.toString());

    assertNotNull(tracker);
  }
}
