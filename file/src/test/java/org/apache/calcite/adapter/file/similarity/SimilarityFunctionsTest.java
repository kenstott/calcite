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
package org.apache.calcite.adapter.file.similarity;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for SimilarityFunctions with real embedding providers.
 */
@Tag("unit")
public class SimilarityFunctionsTest {

  @Test void testCosineSimilarity() {
    String vector1 = "0.5,0.5,0.707";
    String vector2 = "0.707,0.707,0.0";

    double similarity = SimilarityFunctions.cosineSimilarity(vector1, vector2);

    assertTrue(similarity > 0.0 && similarity < 1.0);
    assertEquals(0.707, similarity, 0.01);
  }

  @Test void testTextSimilarityWithLocalProvider() {
    // Test that text similarity uses real embedding provider
    String text1 = "Revenue increased by 10% in the fourth quarter";
    String text2 = "Sales grew 10% in Q4 due to strong demand";

    double similarity = SimilarityFunctions.textSimilarity(text1, text2);

    // Should be greater than 0 since both texts are about revenue/sales growth
    assertTrue(similarity > 0.0, "Similarity should be positive for related financial texts");
    assertTrue(similarity <= 1.0, "Similarity should not exceed 1.0");
  }

  @Test void testTextSimilarityDifferentTexts() {
    String text1 = "The company reported strong revenue growth";
    String text2 = "It was raining cats and dogs";

    double similarity = SimilarityFunctions.textSimilarity(text1, text2);

    // Should be low similarity for unrelated texts
    assertTrue(similarity >= 0.0, "Similarity should be non-negative");
    assertTrue(similarity < 0.5, "Similarity should be low for unrelated texts");
  }

  // testEmbedFunction removed - embed() function deleted in Phase 4
  // Use DuckDB quackformers extension instead: SELECT embed(text)::FLOAT[384]

  @Test void testVectorNorm() {
    String vector = "3.0,4.0,0.0";

    double norm = SimilarityFunctions.vectorNorm(vector);

    assertEquals(5.0, norm, 0.001);
  }

  @Test void testNormalizeVector() {
    String vector = "3.0,4.0,0.0";

    String normalized = SimilarityFunctions.normalizeVector(vector);

    assertNotNull(normalized);
    String[] parts = normalized.split(",");
    assertEquals(3, parts.length);

    // Normalized vector should have unit length
    double norm = SimilarityFunctions.vectorNorm(normalized);
    assertEquals(1.0, norm, 0.001);
  }
}
