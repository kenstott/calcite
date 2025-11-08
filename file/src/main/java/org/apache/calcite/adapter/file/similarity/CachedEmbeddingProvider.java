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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Wrapper that adds persistent caching to any TextEmbeddingProvider.
 *
 * <p>This provider checks a persistent cache before delegating to the underlying
 * provider. Cache hits avoid expensive embedding generation, and cache misses
 * are automatically added to the cache for future use.
 *
 * <p>The cache is shared across all uses of this provider, so the same text
 * embedded in different tables or schemas will reuse the cached embedding.
 */
public class CachedEmbeddingProvider implements TextEmbeddingProvider {
  private static final Logger logger = LoggerFactory.getLogger(CachedEmbeddingProvider.class);

  private final TextEmbeddingProvider delegate;
  private final PersistentEmbeddingCache cache;

  /**
   * Create a cached embedding provider.
   *
   * @param delegate The underlying embedding provider
   * @param cache The persistent cache to use
   */
  public CachedEmbeddingProvider(TextEmbeddingProvider delegate,
                                 PersistentEmbeddingCache cache) {
    if (delegate == null) {
      throw new IllegalArgumentException("Delegate provider cannot be null");
    }
    if (cache == null) {
      throw new IllegalArgumentException("Cache cannot be null");
    }

    this.delegate = delegate;
    this.cache = cache;

    logger.info("Created cached embedding provider wrapping: {}", delegate.getProviderName());
  }

  @Override public double[] generateEmbedding(String text) throws EmbeddingException {
    if (text == null || text.trim().isEmpty()) {
      return new double[getDimensions()];
    }

    // Check cache first
    double[] cached = cache.get(text);
    if (cached != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Cache hit for text: {}",
            text.length() > 50 ? text.substring(0, 50) + "..." : text);
      }
      return cached;
    }

    // Cache miss - generate embedding
    if (logger.isDebugEnabled()) {
      logger.debug("Cache miss for text: {}",
          text.length() > 50 ? text.substring(0, 50) + "..." : text);
    }

    double[] embedding = delegate.generateEmbedding(text);

    // Store in cache
    cache.put(text, embedding);

    return embedding;
  }

  @Override public List<double[]> generateEmbeddings(List<String> texts) throws EmbeddingException {
    if (texts == null || texts.isEmpty()) {
      return new ArrayList<>();
    }

    List<double[]> results = new ArrayList<>(texts.size());
    List<String> cacheMisses = new ArrayList<>();
    List<Integer> missIndices = new ArrayList<>();

    // Separate cache hits from misses
    for (int i = 0; i < texts.size(); i++) {
      String text = texts.get(i);
      double[] cached = cache.get(text);

      if (cached != null) {
        results.add(cached);
      } else {
        results.add(null); // Placeholder for cache miss
        cacheMisses.add(text);
        missIndices.add(i);
      }
    }

    // Log cache statistics
    int hits = texts.size() - cacheMisses.size();
    if (logger.isDebugEnabled()) {
      logger.debug("Batch embedding: {} cache hits, {} cache misses ({} % hit rate)",
          hits, cacheMisses.size(), String.format("%.1f", 100.0 * hits / texts.size()));
    }

    // Batch generate embeddings for cache misses
    if (!cacheMisses.isEmpty()) {
      List<double[]> newEmbeddings = delegate.generateEmbeddings(cacheMisses);

      // Fill in results and update cache
      for (int i = 0; i < cacheMisses.size(); i++) {
        String text = cacheMisses.get(i);
        double[] embedding = newEmbeddings.get(i);

        cache.put(text, embedding);
        results.set(missIndices.get(i), embedding);
      }
    }

    return results;
  }

  @Override public CompletableFuture<double[]> generateEmbeddingAsync(String text) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbedding(text);
      } catch (EmbeddingException e) {
        throw new RuntimeException("Failed to generate embedding", e);
      }
    });
  }

  @Override public CompletableFuture<List<double[]>> generateEmbeddingsAsync(List<String> texts) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbeddings(texts);
      } catch (EmbeddingException e) {
        throw new RuntimeException("Failed to generate embeddings", e);
      }
    });
  }

  @Override public int getDimensions() {
    return delegate.getDimensions();
  }

  @Override public int getMaxInputLength() {
    return delegate.getMaxInputLength();
  }

  @Override public boolean isAvailable() {
    return delegate.isAvailable();
  }

  @Override public String getProviderName() {
    return delegate.getProviderName() + " (cached)";
  }

  @Override public String getModelId() {
    return delegate.getModelId();
  }

  @Override public void close() {
    // Flush cache before closing
    logger.info("Closing cached embedding provider, flushing cache...");
    PersistentEmbeddingCache.CacheStats stats = cache.getStats();
    logger.info("Final cache statistics: {}", stats);

    cache.flushToStorage();
    cache.close();

    // Close underlying provider
    delegate.close();
  }

  @Override public double getCostPer1000Tokens() {
    // Cache makes cost effectively zero for repeated text
    return delegate.getCostPer1000Tokens();
  }

  @Override public boolean supportsBatchProcessing() {
    return delegate.supportsBatchProcessing();
  }

  /**
   * Get the current cache statistics.
   *
   * @return Cache statistics
   */
  public PersistentEmbeddingCache.CacheStats getCacheStats() {
    return cache.getStats();
  }

  /**
   * Get the underlying delegate provider (for testing/debugging).
   *
   * @return The delegate provider
   */
  public TextEmbeddingProvider getDelegate() {
    return delegate;
  }

  /**
   * Get the cache instance (for testing/debugging).
   *
   * @return The cache
   */
  public PersistentEmbeddingCache getCache() {
    return cache;
  }
}
