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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify ONNX model files are properly loaded and accessible.
 * This test validates Phase 2 of the vectors_upgrade.md plan.
 */
@Tag("unit")
class ONNXModelLoadTest {

  @Test
  void testModelLoadingFromClasspath() throws Exception {
    // Create provider with default configuration (should load from classpath)
    Map<String, Object> config = new HashMap<>();

    ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);

    // Verify provider initialized successfully
    assertNotNull(provider, "Provider should be created");
    assertEquals("onnx-sentence-transformers/all-MiniLM-L6-v2",
        provider.getProviderName(), "Provider name should match");
    assertEquals(384, provider.getDimensions(), "Dimensions should be 384");
  }

  @Test
  void testGenerateEmbedding() throws Exception {
    Map<String, Object> config = new HashMap<>();
    ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);

    // Generate embedding for a simple test string
    double[] embedding = provider.generateEmbedding("unemployment rate");

    // Verify embedding properties
    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(384, embedding.length, "Embedding should have 384 dimensions");
    assertTrue(isNormalized(embedding), "Embedding should be L2 normalized");
  }

  @Test
  void testEmbeddingConsistency() throws Exception {
    Map<String, Object> config = new HashMap<>();
    ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);

    // Generate embedding twice for same text
    double[] embedding1 = provider.generateEmbedding("test text");
    double[] embedding2 = provider.generateEmbedding("test text");

    // Verify embeddings are identical (deterministic)
    assertEquals(embedding1.length, embedding2.length,
        "Embeddings should have same length");

    for (int i = 0; i < embedding1.length; i++) {
      assertEquals(embedding1[i], embedding2[i], 1e-6,
          "Embeddings should be identical at position " + i);
    }
  }

  @Test
  void testDifferentTextsDifferentEmbeddings() throws Exception {
    Map<String, Object> config = new HashMap<>();
    ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);

    double[] embedding1 = provider.generateEmbedding("unemployment");
    double[] embedding2 = provider.generateEmbedding("inflation");

    // Calculate cosine similarity
    double similarity = cosineSimilarity(embedding1, embedding2);

    // Different texts should have different embeddings (similarity < 1.0)
    assertTrue(similarity < 1.0,
        "Different texts should have different embeddings");

    // But should still be somewhat related (both economic terms)
    assertTrue(similarity > 0.0,
        "Economic terms should have some semantic similarity");
  }

  /**
   * Checks if a vector is L2 normalized (length = 1.0).
   */
  private boolean isNormalized(double[] vector) {
    double norm = 0.0;
    for (double v : vector) {
      norm += v * v;
    }
    double length = Math.sqrt(norm);
    return Math.abs(length - 1.0) < 0.0001;
  }

  /**
   * Calculates cosine similarity between two vectors.
   */
  private double cosineSimilarity(double[] v1, double[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vectors must have same length");
    }

    double dotProduct = 0.0;
    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
    }

    return dotProduct; // Vectors are already normalized
  }
}