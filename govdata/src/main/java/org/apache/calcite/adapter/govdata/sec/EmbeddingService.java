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
package org.apache.calcite.adapter.govdata.sec;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for text embedding services.
 *
 * This abstraction allows plugging in different embedding providers:
 * - OpenAI Embeddings API
 * - Anthropic Claude embeddings
 * - Cohere Embed API
 * - Local models (Sentence-BERT, FinBERT)
 * - Azure OpenAI Service
 * - Google Vertex AI embeddings
 */
public interface EmbeddingService {

  /**
   * Generate embeddings for a single text.
   *
   * @param text The input text to embed
   * @return Array of embedding values (typically 128-1536 dimensions)
   * @throws RuntimeException if embedding generation fails
   */
  double[] generateEmbedding(String text);

  /**
   * Generate embeddings for multiple texts (batch processing).
   * More efficient than individual calls for large volumes.
   *
   * @param texts Array of input texts to embed
   * @return Array of embedding arrays
   * @throws RuntimeException if batch embedding fails
   */
  double[][] generateEmbeddings(String[] texts);

  /**
   * Generate embeddings asynchronously.
   *
   * @param text The input text to embed
   * @return Future containing the embedding array
   */
  CompletableFuture<double[]> generateEmbeddingAsync(String text);

  /**
   * Get the embedding dimension for this service.
   *
   * @return Number of dimensions in embeddings (e.g., 1536 for OpenAI text-embedding-ada-002)
   */
  int getDimensions();

  /**
   * Get the maximum input length for this service.
   *
   * @return Maximum characters/tokens supported
   */
  int getMaxInputLength();

  /**
   * Check if the service is available and configured.
   *
   * @return true if service can generate embeddings
   */
  boolean isAvailable();

  /**
   * Get a descriptive name for this embedding service.
   *
   * @return Service name (e.g., "OpenAI text-embedding-ada-002")
   */
  String getServiceName();
}
