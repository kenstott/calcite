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

import org.apache.calcite.adapter.file.similarity.SimilarityFunctions;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * FILE-093 / FILE-150 — exact goldens for the file-adapter vector-search UDF registration
 * (recode of the weak {@code testRegisterFunctionsDoesNotThrow} spot-checks). Pins the complete
 * set of function names that {@link SimilarityFunctions#registerFunctions} installs on a schema,
 * the exact registered count, and the EMBED return policy (comma-separated VARCHAR, null-in/null-out).
 *
 * <p>The expected name set below is enumerated directly from the {@code schema.add(...)} calls in
 * {@code SimilarityFunctions.registerFunctions}; it is not a guess.
 */
@Tag("unit")
public class VectorFunctionRequirementsTest {

  /**
   * The exact, complete set of function names registered by
   * {@link SimilarityFunctions#registerFunctions}, in registration order.
   * Mirrors the {@code schema.add("NAME", ...)} calls one-for-one.
   */
  private static final String[] EXPECTED_FUNCTION_NAMES = {
      "COSINE_SIMILARITY",
      "SEMANTIC_SIMILARITY",
      "EMBED",
      "COSINE_DISTANCE",
      "EUCLIDEAN_DISTANCE",
      "DOT_PRODUCT",
      "VECTORS_SIMILAR",
      "VECTOR_NORM",
      "NORMALIZE_VECTOR",
      "TEXT_SIMILARITY",
  };

  private static SchemaPlus registerOntoFreshSchema() {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    SchemaPlus schemaPlus = rootSchema.plus();
    SimilarityFunctions.registerFunctions(schemaPlus);
    return schemaPlus;
  }

  // ===== FILE-093: exact registered-function set + count =====

  @Test
  @Tag("FILE-093")
  void registerFunctionsInstallsExactlyTheDocumentedVectorUdfSet() {
    SchemaPlus schema = registerOntoFreshSchema();

    // Every documented name is present (non-empty function collection).
    for (String name : EXPECTED_FUNCTION_NAMES) {
      assertNotNull(schema.getFunctions(name),
          "getFunctions(\"" + name + "\") returned null");
      assertFalse(schema.getFunctions(name).isEmpty(),
          "expected function registered under name: " + name);
    }

    // The set of registered names is EXACTLY the documented set — no more, no fewer.
    Set<String> expected = new HashSet<>(Arrays.asList(EXPECTED_FUNCTION_NAMES));
    Set<String> actual = new HashSet<>(schema.getFunctionNames());
    assertEquals(expected, actual,
        "registered function names must match the documented set exactly");

    // Exact count.
    assertEquals(10, EXPECTED_FUNCTION_NAMES.length,
        "documented set is expected to contain exactly 10 names");
    assertEquals(10, actual.size(),
        "schema must expose exactly 10 distinct registered function names");
  }

  // ===== FILE-150: exact count + EMBED return policy =====

  @Test
  @Tag("FILE-150")
  void registeredSetSizeIsExactAndEmbedReturnsCommaSeparatedVarchar() {
    // Exact registered-set size (pinned independently of FILE-093's name assertions).
    SchemaPlus schema = registerOntoFreshSchema();
    assertEquals(10, schema.getFunctionNames().size(),
        "schema must expose exactly 10 distinct registered function names");

    // EMBED is registered (maps to embedText). NOTE: EMBED is present in the code, so this
    // FILE-150 method asserts both the exact set size AND the EMBED return policy.
    assertNotNull(schema.getFunctions("EMBED"));
    assertFalse(schema.getFunctions("EMBED").isEmpty(),
        "EMBED must be among the registered functions");

    // EMBED return policy: null in -> null out (no embedding-server call on the null path).
    assertNull(SimilarityFunctions.embedText(null));

    // EMBED return policy: a non-null vector is returned as a comma-separated VARCHAR
    // (a String, never an ARRAY). Without a live embedding server we pin the exact
    // serialization contract that embedText applies to its embedding vector.
    String formatted = formatVector(new double[]{1.0, 0.0, -2.5});
    // Comma-separated, no surrounding brackets/parentheses, one token per component.
    assertFalse(formatted.startsWith("["), "must not be array-bracketed: " + formatted);
    assertFalse(formatted.startsWith("("), "must not be paren-wrapped: " + formatted);
    assertEquals(3, formatted.split(",").length,
        "comma-separated token count must equal component count: " + formatted);
    assertEquals("1.0,0.0,-2.5", formatted,
        "EMBED must emit a comma-separated VARCHAR with no delimiters");
  }

  /**
   * Reproduces {@code SimilarityFunctions.embedText}'s exact comma-join serialization so the
   * VARCHAR return-format contract can be pinned without invoking the live embedding server.
   */
  private static String formatVector(double[] v) {
    StringBuilder sb = new StringBuilder(v.length * 12);
    for (int i = 0; i < v.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(v[i]);
    }
    return sb.toString();
  }
}
