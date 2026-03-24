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
package org.apache.calcite.adapter.file.similarity;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for SimilarityFunctions covering edge cases,
 * null handling, error paths, and all input type variants.
 */
@Tag("unit")
public class SimilarityFunctionsDeepTest {

  // ===== cosineSimilarity - null/edge cases =====

  @Test void testCosineSimilarityBothNull() {
    assertEquals(0.0, SimilarityFunctions.cosineSimilarity((Object) null, (Object) null));
  }

  @Test void testCosineSimilarityFirstNull() {
    assertEquals(0.0, SimilarityFunctions.cosineSimilarity((Object) null, "1.0,2.0"));
  }

  @Test void testCosineSimilaritySecondNull() {
    assertEquals(0.0, SimilarityFunctions.cosineSimilarity("1.0,2.0", (Object) null));
  }

  @Test void testCosineSimilarityIdenticalVectors() {
    double sim = SimilarityFunctions.cosineSimilarity("1.0,0.0,0.0", "1.0,0.0,0.0");
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityOrthogonalVectors() {
    double sim = SimilarityFunctions.cosineSimilarity("1.0,0.0", "0.0,1.0");
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityOppositeVectors() {
    double sim = SimilarityFunctions.cosineSimilarity("1.0,0.0", "-1.0,0.0");
    assertEquals(-1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityZeroVector() {
    // Zero vector should return 0 (norm is 0)
    double sim = SimilarityFunctions.cosineSimilarity("0.0,0.0", "1.0,1.0");
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityDimensionMismatch() {
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.cosineSimilarity("1.0,2.0", "1.0,2.0,3.0"));
  }

  @Test void testCosineSimilarityStringOverload() {
    double sim = SimilarityFunctions.cosineSimilarity("1.0,0.0", "1.0,0.0");
    assertEquals(1.0, sim, 0.001);
  }

  // ===== cosineSimilarity - different input types =====

  @Test void testCosineSimilarityWithFloatArray() {
    float[] v1 = new float[]{1.0f, 0.0f, 0.0f};
    float[] v2 = new float[]{1.0f, 0.0f, 0.0f};
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithDoubleArray() {
    double[] v1 = new double[]{1.0, 0.0, 0.0};
    double[] v2 = new double[]{0.0, 1.0, 0.0};
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithList() {
    List<Double> v1 = Arrays.asList(3.0, 4.0);
    List<Double> v2 = Arrays.asList(3.0, 4.0);
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithListOfFloats() {
    List<Float> v1 = Arrays.asList(1.0f, 0.0f);
    List<Float> v2 = Arrays.asList(0.0f, 1.0f);
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityListNonNumericThrows() {
    List<String> v1 = Arrays.asList("not", "numbers");
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.cosineSimilarity(v1, v1));
  }

  @Test void testCosineSimilarityWithIntegerArray() {
    // Integer[] (boxed) should work via reflection array path
    Integer[] v1 = new Integer[]{1, 0, 0};
    Integer[] v2 = new Integer[]{0, 1, 0};
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithDuckDBArrayFormat() {
    // DuckDB format: [1.0, 2.0, 3.0]
    double sim = SimilarityFunctions.cosineSimilarity("[1.0, 0.0]", "[1.0, 0.0]");
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithParenthesesFormat() {
    // Parentheses format: (1.0, 2.0, 3.0)
    double sim = SimilarityFunctions.cosineSimilarity("(1.0, 0.0)", "(1.0, 0.0)");
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testCosineSimilarityWithObjectToString() {
    // Object that uses toString fallback
    Object v1 = new Object() {
      @Override public String toString() {
        return "1.0,0.0";
      }
    };
    Object v2 = new Object() {
      @Override public String toString() {
        return "0.0,1.0";
      }
    };
    double sim = SimilarityFunctions.cosineSimilarity(v1, v2);
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testCosineSimilarityArrayWithNonNumericThrows() {
    String[] v1 = new String[]{"not", "numbers"};
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.cosineSimilarity(v1, v1));
  }

  // ===== cosineDistance =====

  @Test void testCosineDistanceIdentical() {
    double dist = SimilarityFunctions.cosineDistance("1.0,0.0", "1.0,0.0");
    assertEquals(0.0, dist, 0.001);
  }

  @Test void testCosineDistanceOrthogonal() {
    double dist = SimilarityFunctions.cosineDistance("1.0,0.0", "0.0,1.0");
    assertEquals(1.0, dist, 0.001);
  }

  @Test void testCosineDistanceOpposite() {
    double dist = SimilarityFunctions.cosineDistance("1.0,0.0", "-1.0,0.0");
    assertEquals(2.0, dist, 0.001);
  }

  @Test void testCosineDistanceNullReturns1() {
    double dist = SimilarityFunctions.cosineDistance(null, "1.0,0.0");
    assertEquals(1.0, dist, 0.001); // 1.0 - 0.0 = 1.0
  }

  // ===== euclideanDistance =====

  @Test void testEuclideanDistanceIdentical() {
    double dist = SimilarityFunctions.euclideanDistance("1.0,2.0,3.0", "1.0,2.0,3.0");
    assertEquals(0.0, dist, 0.001);
  }

  @Test void testEuclideanDistanceSimple() {
    double dist = SimilarityFunctions.euclideanDistance("0.0,0.0", "3.0,4.0");
    assertEquals(5.0, dist, 0.001);
  }

  @Test void testEuclideanDistanceNullFirst() {
    double dist = SimilarityFunctions.euclideanDistance(null, "1.0,2.0");
    assertEquals(Double.MAX_VALUE, dist);
  }

  @Test void testEuclideanDistanceNullSecond() {
    double dist = SimilarityFunctions.euclideanDistance("1.0,2.0", null);
    assertEquals(Double.MAX_VALUE, dist);
  }

  @Test void testEuclideanDistanceDimensionMismatch() {
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.euclideanDistance("1.0,2.0", "1.0,2.0,3.0"));
  }

  // ===== dotProduct =====

  @Test void testDotProductSimple() {
    double dot = SimilarityFunctions.dotProduct("1.0,2.0,3.0", "4.0,5.0,6.0");
    assertEquals(32.0, dot, 0.001); // 4+10+18
  }

  @Test void testDotProductNullFirst() {
    assertEquals(0.0, SimilarityFunctions.dotProduct(null, "1.0,2.0"));
  }

  @Test void testDotProductNullSecond() {
    assertEquals(0.0, SimilarityFunctions.dotProduct("1.0,2.0", null));
  }

  @Test void testDotProductOrthogonal() {
    assertEquals(0.0, SimilarityFunctions.dotProduct("1.0,0.0", "0.0,1.0"), 0.001);
  }

  @Test void testDotProductDimensionMismatch() {
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.dotProduct("1.0", "1.0,2.0"));
  }

  // ===== vectorsSimilar =====

  @Test void testVectorsSimilarAboveThreshold() {
    assertTrue(SimilarityFunctions.vectorsSimilar("1.0,0.0", "1.0,0.0", 0.5));
  }

  @Test void testVectorsSimilarBelowThreshold() {
    assertFalse(SimilarityFunctions.vectorsSimilar("1.0,0.0", "0.0,1.0", 0.5));
  }

  @Test void testVectorsSimilarDefaultThreshold() {
    // null threshold should use 0.7
    assertTrue(SimilarityFunctions.vectorsSimilar("1.0,0.0", "1.0,0.0", null));
    assertFalse(SimilarityFunctions.vectorsSimilar("1.0,0.0", "0.0,1.0", null));
  }

  @Test void testVectorsSimilarExactThreshold() {
    // cosineSimilarity of (1,1) and (1,0) = 1/sqrt(2) ~= 0.707
    assertTrue(SimilarityFunctions.vectorsSimilar("1.0,1.0", "1.0,0.0", 0.7));
  }

  // ===== vectorNorm =====

  @Test void testVectorNormNull() {
    assertEquals(0.0, SimilarityFunctions.vectorNorm(null));
  }

  @Test void testVectorNormUnit() {
    assertEquals(1.0, SimilarityFunctions.vectorNorm("1.0,0.0,0.0"), 0.001);
  }

  @Test void testVectorNormZero() {
    assertEquals(0.0, SimilarityFunctions.vectorNorm("0.0,0.0"), 0.001);
  }

  // ===== normalizeVector =====

  @Test void testNormalizeVectorNull() {
    assertNull(SimilarityFunctions.normalizeVector(null));
  }

  @Test void testNormalizeVectorZero() {
    // Zero vector normalization should return the original vector
    String result = SimilarityFunctions.normalizeVector("0.0,0.0");
    assertEquals("0.0,0.0", result);
  }

  @Test void testNormalizeVectorAlreadyUnit() {
    String result = SimilarityFunctions.normalizeVector("1.0,0.0,0.0");
    assertNotNull(result);
    String[] parts = result.split(",");
    assertEquals(3, parts.length);
    assertEquals(1.0, Double.parseDouble(parts[0]), 0.001);
    assertEquals(0.0, Double.parseDouble(parts[1]), 0.001);
    assertEquals(0.0, Double.parseDouble(parts[2]), 0.001);
  }

  // ===== textSimilarity =====

  @Test void testTextSimilarityNullFirst() {
    assertEquals(0.0, SimilarityFunctions.textSimilarity(null, "hello world"));
  }

  @Test void testTextSimilarityNullSecond() {
    assertEquals(0.0, SimilarityFunctions.textSimilarity("hello world", null));
  }

  @Test void testTextSimilarityIdentical() {
    double sim = SimilarityFunctions.textSimilarity("hello world", "hello world");
    assertEquals(1.0, sim, 0.001);
  }

  @Test void testTextSimilarityNoOverlap() {
    double sim = SimilarityFunctions.textSimilarity("apple banana", "cherry grape");
    assertEquals(0.0, sim, 0.001);
  }

  @Test void testTextSimilarityPartialOverlap() {
    // Jaccard: intersection=1 (word), union=3
    double sim = SimilarityFunctions.textSimilarity("red blue", "blue green");
    assertTrue(sim > 0.0 && sim < 1.0);
  }

  @Test void testTextSimilarityCaseInsensitive() {
    double sim = SimilarityFunctions.textSimilarity("Hello World", "hello world");
    assertEquals(1.0, sim, 0.001);
  }

  // ===== parseVector error cases =====

  @Test void testInvalidVectorFormatThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> SimilarityFunctions.cosineSimilarity("abc,def", "1.0,2.0"));
  }

  // ===== registerFunctions =====

  @Test void testRegisterFunctionsDoesNotThrow() {
    // Use a real Calcite root schema
    org.apache.calcite.jdbc.CalciteSchema rootSchema =
        org.apache.calcite.jdbc.CalciteSchema.createRootSchema(true);
    org.apache.calcite.schema.SchemaPlus schemaPlus = rootSchema.plus();
    // Should not throw
    SimilarityFunctions.registerFunctions(schemaPlus);
    // Verify functions were registered
    assertNotNull(schemaPlus.getFunctions("COSINE_SIMILARITY"));
    assertFalse(schemaPlus.getFunctions("COSINE_SIMILARITY").isEmpty());
    assertNotNull(schemaPlus.getFunctions("EUCLIDEAN_DISTANCE"));
    assertFalse(schemaPlus.getFunctions("EUCLIDEAN_DISTANCE").isEmpty());
    assertNotNull(schemaPlus.getFunctions("TEXT_SIMILARITY"));
    assertFalse(schemaPlus.getFunctions("TEXT_SIMILARITY").isEmpty());
    assertNotNull(schemaPlus.getFunctions("VECTOR_NORM"));
    assertNotNull(schemaPlus.getFunctions("NORMALIZE_VECTOR"));
    assertNotNull(schemaPlus.getFunctions("DOT_PRODUCT"));
    assertNotNull(schemaPlus.getFunctions("VECTORS_SIMILAR"));
    assertNotNull(schemaPlus.getFunctions("COSINE_DISTANCE"));
  }
}
