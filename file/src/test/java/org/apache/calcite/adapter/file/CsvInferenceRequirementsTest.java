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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
  // FILE-055 — refines FILE-003/098: assert the EXACT null-token set (not a contains() probe), the
  // full set of documented case variants, and the ACTUAL nullable rule. The requirement's
  // "nullable when null ratio exceeds nullableThreshold" clause is dead code (see C-32) and is staged
  // @Disabled rather than asserted, since the code's real rule is binary (any null token -> nullable).
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-055") void recognizedNullTokenSetIsExactlySixCanonicalTokens() {
    assertEquals(new HashSet<>(Arrays.asList("NULL", "NA", "N/A", "NONE", "NIL", "")),
        NullEquivalents.DEFAULT_NULL_EQUIVALENTS,
        "the default null set is exactly these six canonical (upper-cased) tokens");
    assertEquals(6, NullEquivalents.DEFAULT_NULL_EQUIVALENTS.size());
  }

  @Test @Tag("FILE-055") void everyDocumentedCaseVariantIsRecognizedAsNull() {
    for (String t : new String[] {"", "  ", "NULL", "null", "Null", "NA", "N/A",
        "NONE", "None", "NIL", "nil"}) {
      assertTrue(NullEquivalents.isNullRepresentation(t), t + " must be a null token");
    }
    assertFalse(NullEquivalents.isNullRepresentation("data"));
    assertFalse(NullEquivalents.isNullRepresentation("0"), "0 is a value, not a null token");
    assertFalse(NullEquivalents.isNullRepresentation((String) null), "Java null is not a token match");
  }

  @Test @Tag("FILE-055") void anyNullTokenMakesColumnNullableWhenMakeAllNullableFalse(@TempDir Path dir)
      throws Exception {
    // makeAllNullable=false isolates the per-column nullable decision from the blanket flag.
    TypeInferenceConfig noBlanketNull =
        new TypeInferenceConfig(true, 1.0, 1000, 0.95, true, true, true, false, 0.0);

    Path clean = Files.createTempFile(dir, "clean", ".csv");
    Files.write(clean, "v\n1\n2\n3\n".getBytes(StandardCharsets.UTF_8));
    ColumnTypeInfo noNulls =
        CsvTypeInferrer.inferTypes(Sources.of(clean.toFile()), noBlanketNull, "UNCHANGED").get(0);
    assertEquals(SqlTypeName.INTEGER, noNulls.inferredType);
    assertFalse(noNulls.nullable, "no null tokens + makeAllNullable=false -> NOT nullable");

    Path withNull = Files.createTempFile(dir, "withnull", ".csv");
    Files.write(withNull, "v\n1\nNA\n3\n".getBytes(StandardCharsets.UTF_8));
    ColumnTypeInfo oneNull =
        CsvTypeInferrer.inferTypes(Sources.of(withNull.toFile()), noBlanketNull, "UNCHANGED").get(0);
    assertEquals(SqlTypeName.INTEGER, oneNull.inferredType, "NA is a null token, column stays INTEGER");
    assertTrue(oneNull.nullable, "a single null token (ratio 1/3) makes the column nullable");
  }

  @Test @Tag("FILE-055")
  @Disabled("C-32: nullableThreshold is parsed/clamped/exposed by getter but has NO consumer in "
      + "CsvTypeInferrer.determineType; the nullable decision is binary (nullValues>0 || "
      + "makeAllNullable). The requirement's 'nullable when null ratio exceeds nullableThreshold' "
      + "clause is unimplemented — pending code fix or requirement reword.")
  void columnNullableOnlyWhenNullRatioExceedsThreshold_target() {
    // INTENDED behavior (documented, NOT asserted as currently passing): with makeAllNullable=false
    // and nullableThreshold=0.5, a column whose null ratio is below the threshold should remain NOT
    // nullable, and only above it become nullable. The current code ignores the threshold entirely.
    // No assertion of current (wrong) behavior is made here.
  }

  // ----------------------------------------------------------------------------------------------
  // FILE-052 — inference-config defaults. TWO distinct "defaults" that legitimately differ:
  //   * TypeInferenceConfig.defaultConfig() is a convenience constructor with enabled=TRUE;
  //   * the SCHEMA/operand default routes through fromMap(...) which is DISABLED unless enabled:true,
  //     so a schema with no csvTypeInference operand does NO inference -> columns stay VARCHAR.
  // (The flat JDBC-URL aliases are NOT part of this requirement — tracked separately as the proposed
  // feature FILE-174; the current JDBC path uses csvInferTypes/csvSamplingRate/... with no aliases.)
  // ----------------------------------------------------------------------------------------------

  @Test @Tag("FILE-052") void defaultConfigObjectIsEnabledWithExpectedDefaults() {
    TypeInferenceConfig c = TypeInferenceConfig.defaultConfig();
    assertTrue(c.isEnabled(), "the defaultConfig() convenience constructor is enabled");
    assertEquals(0.1, c.getSamplingRate(), 0.0);
    assertEquals(1000, c.getMaxSampleRows());
    assertEquals(0.95, c.getConfidenceThreshold(), 0.0);
    assertTrue(c.isInferDates());
    assertTrue(c.isInferTimes());
    assertTrue(c.isInferTimestamps());
    assertTrue(c.isMakeAllNullable());
    assertEquals(0.0, c.getNullableThreshold(), 0.0);
  }

  @Test @Tag("FILE-052") void schemaDefaultIsDisabledSoColumnsStayVarchar(@TempDir Path dir)
      throws Exception {
    // No operand -> fromMap(null) -> disabled, and blankStringsAsNull stays on.
    TypeInferenceConfig none = TypeInferenceConfig.fromMap(null);
    assertFalse(none.isEnabled(), "no csvTypeInference operand -> inference disabled");
    assertTrue(none.isBlankStringsAsNull());
    // An operand present but without enabled:true is still disabled (enabled is opt-in).
    assertFalse(TypeInferenceConfig.fromMap(new HashMap<String, Object>()).isEnabled());
    // Disabled inference returns NO column types -> the schema leaves every column VARCHAR.
    Path f = Files.createTempFile(dir, "novary", ".csv");
    Files.write(f, "v\n1\n2\n3\n".getBytes(StandardCharsets.UTF_8));
    List<ColumnTypeInfo> r =
        CsvTypeInferrer.inferTypes(Sources.of(f.toFile()), TypeInferenceConfig.disabled(), "UNCHANGED");
    assertTrue(r.isEmpty(), "disabled inference emits no inferred types (all VARCHAR at the schema)");
  }

  @Test @Tag("FILE-052") void enabledFromMapAppliesPerFieldDefaults() {
    Map<String, Object> m = new HashMap<String, Object>();
    m.put("enabled", Boolean.TRUE);
    TypeInferenceConfig c = TypeInferenceConfig.fromMap(m);
    assertTrue(c.isEnabled());
    assertEquals(0.1, c.getSamplingRate(), 0.0);
    assertEquals(1000, c.getMaxSampleRows());
    assertEquals(0.95, c.getConfidenceThreshold(), 0.0);
    assertTrue(c.isInferDates());
    assertTrue(c.isInferTimes());
    assertTrue(c.isInferTimestamps());
    assertTrue(c.isMakeAllNullable());
    assertEquals(0.0, c.getNullableThreshold(), 0.0);
    assertFalse(c.isBlankStringsAsNull(), "enabled path defaults blankStringsAsNull to false");
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
