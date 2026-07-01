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

  @Test void testCreateDefaultTrackerIsReadOnly() {
    // Null backend must fail closed to a read-only tracker, NOT a writable DuckDB.
    // (Assumes CALCITE_TRACKER_BACKEND is unset in the test environment.)
    PipelineTracker tracker = PipelineTrackerFactory.create(
        null, tempDir.toString(), Collections.<String, String>emptyMap());

    assertNotNull(tracker);
    assertTrue(tracker instanceof ReadOnlyPipelineTracker);
    // Reads report untracked; writes throw rather than silently corrupting state.
    assertTrue(!tracker.isComplete("k", "t", "p"));
    assertThrows(IllegalStateException.class,
        () -> tracker.markComplete("k", "t", "p", 1));
  }

  @Test void testCreateUnknownBackendThrows() {
    // An unrecognized backend is a hard error — never a silent fallback to a writable store.
    assertThrows(IllegalArgumentException.class, () -> PipelineTrackerFactory.create(
        "unknown_backend", tempDir.toString(), Collections.<String, String>emptyMap()));
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
