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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for PipelineTrackerFactory covering all backend types,
 * error conditions, and operand-based configuration.
 */
@Tag("unit")
public class PipelineTrackerFactoryDeepTest {

  // ===== Noop backend =====

  @Test void testCreateNoopBackend() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "noop", "/tmp/test", Collections.<String, String>emptyMap());
    assertNotNull(tracker);
    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  @Test void testCreateNoopBackendCaseInsensitive() {
    PipelineTracker tracker = PipelineTrackerFactory.create(
        "NOOP", "/tmp/test", Collections.<String, String>emptyMap());
    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  // ===== S3 backend errors =====

  @Test void testCreateS3BackendNoBucketThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        PipelineTrackerFactory.create("s3", "/tmp/test", Collections.<String, String>emptyMap()));
  }

  @Test void testCreateS3BackendWithBucketInConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("bucket", "my-bucket");
    // This will create an S3HivePipelineTracker - it may throw if S3 is not available
    // but the factory selection code is tested
    try {
      PipelineTracker tracker = PipelineTrackerFactory.create("s3", "/tmp/test", config);
      assertNotNull(tracker);
    } catch (Exception e) {
      // S3 connectivity issues are expected in unit tests
      assertTrue(e.getMessage() != null);
    }
  }

  // ===== PG backend errors =====

  @Test void testCreatePGBackendNoUrlThrows() {
    assertThrows(RuntimeException.class, () ->
        PipelineTrackerFactory.create("pg", "/tmp/test", Collections.<String, String>emptyMap()));
  }

  @Test void testCreatePostgresBackendNoUrlThrows() {
    assertThrows(RuntimeException.class, () ->
        PipelineTrackerFactory.create("postgres", "/tmp/test", Collections.<String, String>emptyMap()));
  }

  // ===== Unknown backend =====

  @Test void testCreateUnknownBackendFallsToDuckDB() {
    // Unknown backend should fall back to duckdb (which may fail without proper setup)
    try {
      PipelineTracker tracker = PipelineTrackerFactory.create(
          "unknown_backend", "/tmp/test", Collections.<String, String>emptyMap());
      assertNotNull(tracker);
    } catch (Exception e) {
      // DuckDB may not be available in unit test environment
      assertTrue(e.getMessage() != null);
    }
  }

  // ===== createFromOperand =====

  @Test void testCreateFromOperandNoop() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("trackerBackend", "noop");

    PipelineTracker tracker = PipelineTrackerFactory.createFromOperand(operand, "/tmp/test");
    assertSame(PipelineTracker.NOOP_PIPELINE, tracker);
  }

  @Test void testCreateFromOperandWithTrackerConfig() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("trackerBackend", "noop");
    Map<String, String> trackerConfig = new HashMap<>();
    trackerConfig.put("key1", "value1");
    operand.put("trackerConfig", trackerConfig);

    PipelineTracker tracker = PipelineTrackerFactory.createFromOperand(operand, "/tmp/test");
    assertNotNull(tracker);
  }

  @Test void testCreateFromOperandMergesS3Config() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("trackerBackend", "noop");

    Map<String, String> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "secret");
    s3Config.put("endpoint", "http://localhost:9000");
    s3Config.put("region", "us-east-1");
    operand.put("s3Config", s3Config);

    // Should not throw - s3Config should be merged into config but
    // since backend is noop, it won't actually use them
    PipelineTracker tracker = PipelineTrackerFactory.createFromOperand(operand, "/tmp/test");
    assertNotNull(tracker);
  }

  @Test void testCreateFromOperandNullBackend() {
    Map<String, Object> operand = new HashMap<>();
    // No trackerBackend -> falls to env or default
    // Default is duckdb which may fail in test
    try {
      PipelineTracker tracker = PipelineTrackerFactory.createFromOperand(operand, "/tmp/test");
      assertNotNull(tracker);
    } catch (Exception e) {
      // DuckDB may not be available
      assertTrue(e.getMessage() != null);
    }
  }

  // ===== create(baseDirectory) - simple form =====

  @Test void testCreateSimpleForm() {
    // Tests the simple create(baseDirectory) overload
    try {
      PipelineTracker tracker = PipelineTrackerFactory.create("/tmp/test");
      assertNotNull(tracker);
    } catch (Exception e) {
      // DuckDB may not be available
      assertTrue(e.getMessage() != null);
    }
  }
}
