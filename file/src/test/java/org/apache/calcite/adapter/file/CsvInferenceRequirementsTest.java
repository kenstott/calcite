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

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.ColumnTypeInfo;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.TypeInferenceConfig;
import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the CSV type-inference requirements (FILE-096..099), exercising the engine-independent
 * {@link CsvTypeInferrer} directly. Active tests assert behavior that survives the contradiction
 * resolutions; tests for resolved-but-not-yet-fixed behavior are {@code @Disabled} with the C-id.
 */
@Tag("unit")
public class CsvInferenceRequirementsTest {

  /** Deterministic config: full sampling (FILE-099), inference + temporal on, all-nullable. */
  private static final TypeInferenceConfig CONF =
      new TypeInferenceConfig(true, 1.0, 1000, 0.95, true, true, true, true, 0.0);

  private static List<ColumnTypeInfo> infer(Path dir, String csv) throws Exception {
    Path f = Files.createTempFile(dir, "infer", ".csv");
    Files.write(f, csv.getBytes(StandardCharsets.UTF_8));
    return CsvTypeInferrer.inferTypes(Sources.of(f.toFile()), CONF, "UNCHANGED");
  }

  private static List<ColumnTypeInfo> inferResource(String resource) throws Exception {
    File f = new File(CsvInferenceRequirementsTest.class.getResource(resource).toURI());
    return CsvTypeInferrer.inferTypes(Sources.of(f), CONF, "UNCHANGED");
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-097 — numeric promotion: INTEGER in 32-bit range, BIGINT outside, 0/1 are INTEGER not
  // BOOLEAN, pure decimals are DOUBLE.
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-097") void numericPromotion(@TempDir Path dir) throws Exception {
    assertEquals(SqlTypeName.INTEGER, infer(dir, "v\n1\n2\n3\n").get(0).inferredType,
        "small whole numbers -> INTEGER");
    assertEquals(SqlTypeName.BIGINT, infer(dir, "v\n3000000000\n4000000000\n").get(0).inferredType,
        "out of 32-bit range -> BIGINT");
    assertEquals(SqlTypeName.DOUBLE, infer(dir, "v\n1.5\n2.25\n").get(0).inferredType,
        "decimals -> DOUBLE");
  }

  @Test @Tag("FILE-097") void zeroOneAreIntegerNotBoolean(@TempDir Path dir) throws Exception {
    assertEquals(SqlTypeName.INTEGER, infer(dir, "v\n0\n1\n1\n0\n").get(0).inferredType,
        "0/1 are INTEGER (integer pattern tried before boolean)");
    assertEquals(SqlTypeName.BOOLEAN, infer(dir, "v\ntrue\nfalse\ntrue\n").get(0).inferredType,
        "true/false -> BOOLEAN");
  }

  @Test @Tag("FILE-097")
  @Disabled("blocked on C-09: dot-less scientific notation (1e5) should infer DOUBLE")
  void scientificNotationIsNumeric_target(@TempDir Path dir) throws Exception {
    assertEquals(SqlTypeName.DOUBLE, infer(dir, "v\n1e5\n2e5\n").get(0).inferredType);
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-096 — type resolution: an all-null column is nullable VARCHAR (confidence 1.0); an
  // all-string column is VARCHAR. (Minority-string promotion is the C-08 target, disabled below.)
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-096") void allNullColumnIsNullableVarchar(@TempDir Path dir) throws Exception {
    ColumnTypeInfo c = infer(dir, "v\nNULL\nNA\nNONE\n").get(0);
    assertEquals(SqlTypeName.VARCHAR, c.inferredType, "all-null -> VARCHAR");
    assertTrue(c.nullable, "all-null column is nullable");
    assertEquals(1.0, c.confidence, 0.0, "all-null confidence is 1.0");
  }

  @Test @Tag("FILE-096") void allStringColumnIsVarchar(@TempDir Path dir) throws Exception {
    assertEquals(SqlTypeName.VARCHAR, infer(dir, "v\nalpha\nbeta\ngamma\n").get(0).inferredType);
  }

  @Test @Tag("FILE-096")
  @Disabled("blocked on C-08: a minority of strings below 1-confidenceThreshold should promote, not force VARCHAR")
  void confidenceThresholdPromotesMajority_target(@TempDir Path dir) throws Exception {
    // 19 integers + 1 stray string = 5% non-conforming, below 1 - 0.95; target is INTEGER.
    StringBuilder sb = new StringBuilder("v\n");
    for (int i = 0; i < 19; i++) {
      sb.append(i).append('\n');
    }
    sb.append("stray\n");
    assertEquals(SqlTypeName.INTEGER, infer(dir, sb.toString()).get(0).inferredType);
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-098 — null token set: {NULL, NA, N/A, NONE, NIL} (case-insensitive) plus any
  // empty/whitespace-only string.
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-098") void defaultNullTokenSet() {
    for (String tok : new String[] {"NULL", "NA", "N/A", "NONE", "NIL"}) {
      assertTrue(NullEquivalents.DEFAULT_NULL_EQUIVALENTS.contains(tok),
          "default null set contains " + tok);
    }
  }

  @Test @Tag("FILE-098") void nullRepresentationIsCaseInsensitiveAndCoversBlanks() {
    assertTrue(NullEquivalents.isNullRepresentation("null"), "case-insensitive null");
    assertTrue(NullEquivalents.isNullRepresentation("N/A"));
    assertTrue(NullEquivalents.isNullRepresentation(""), "empty string is null");
    assertTrue(NullEquivalents.isNullRepresentation("   "), "whitespace-only is null");
    assertFalse(NullEquivalents.isNullRepresentation("data"), "real value is not null");
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-099 — with samplingRate=1.0 inference is deterministic across runs over the same bytes.
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-099") void inferenceIsDeterministicAtFullSampling(@TempDir Path dir) throws Exception {
    String csv = "id,name,score,when\n1,alice,9.5,2020-01-01\n2,bob,8.0,2021-06-15\n";
    List<ColumnTypeInfo> a = infer(dir, csv);
    List<ColumnTypeInfo> b = infer(dir, csv);
    assertEquals(a.size(), b.size());
    for (int i = 0; i < a.size(); i++) {
      assertEquals(a.get(i).inferredType, b.get(i).inferredType,
          "column " + i + " type stable across runs");
    }
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-006 — temporal inference is EXACT and engine-independent (recode of the legacy
  // type-OR-tolerant JDBC test): local datetimes -> TIMESTAMP, tz-carrying -> TIMESTAMP WITH LOCAL
  // TIME ZONE.
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-006") void timestampInferenceIsExact() throws Exception {
    List<ColumnTypeInfo> t = inferResource("/csv-type-inference/timestamps.csv");
    assertEquals(SqlTypeName.INTEGER, t.get(0).inferredType, "event_id");
    assertEquals(SqlTypeName.VARCHAR, t.get(1).inferredType, "event_name");
    assertEquals(SqlTypeName.TIMESTAMP, t.get(2).inferredType, "timestamp_local (no tz)");
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, t.get(3).inferredType, "timestamp_utc (Z)");
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, t.get(4).inferredType, "timestamp_rfc (RFC)");
  }

  // FILE-054 — TIME (HH:mm:ss) and slash-date (MM/dd/yyyy) token forms infer exactly.
  @Test @Tag("FILE-054") void timeAndSlashDateFormatsInfer(@TempDir Path dir) throws Exception {
    assertEquals(SqlTypeName.TIME, infer(dir, "t\n09:30:00\n10:45:30\n11:00:00\n").get(0).inferredType,
        "HH:mm:ss -> TIME");
    assertEquals(SqlTypeName.DATE, infer(dir, "d\n01/15/2024\n02/20/2024\n03/05/2024\n").get(0).inferredType,
        "MM/dd/yyyy -> DATE");
  }
}
