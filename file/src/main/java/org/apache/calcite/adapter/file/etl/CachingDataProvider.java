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
package org.apache.calcite.adapter.file.etl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link DataProvider} that downloads over HTTP and wants the pipeline's raw cache.
 *
 * <p>A provider exists to reach what the built-in path cannot: a binary that the
 * {@link ResponseTransformer} {@code String} signature cannot carry, or a file whose real URL has
 * to be discovered by crawling a listing. Neither of those has anything to do with whether the
 * downloaded bytes are worth keeping — but a provider replaces {@link HttpSource} outright, and
 * every line of raw caching lives inside it, so a provider that opened its own connection lost the
 * cache as a side effect of where the code sits. Providers that cared then grew private caches with
 * their own key layouts and their own invalidation, which the raw-cache tooling cannot see.
 *
 * <p>Implementing this interface gets the shared cache back without giving up any control: the
 * provider still decides which URL to fetch and how to parse what comes back, and simply reads
 * through {@link RawCache#openStream} instead of opening its own connection.
 *
 * <p>Declared as its own interface rather than a default method on {@link DataProvider} because the
 * pipeline must be able to tell, for certain, whether a provider handles the cache-aware form. A
 * default method cannot answer that: a proxy-based provider — a Mockito mock in a test, or any
 * dynamic proxy — does not run interface defaults, so the pipeline would call the cache-aware form,
 * silently receive null, and fall through to {@link HttpSource} as though the provider had declined
 * the batch. An {@code instanceof} check cannot be wrong in that way.
 *
 * <p>A provider that generates its rows, or reads something that is not HTTP at all, has nothing to
 * cache and should keep implementing {@link DataProvider} alone.
 */
public interface CachingDataProvider extends DataProvider {

  /**
   * Fetches data for a batch, reading through the pipeline's raw cache.
   *
   * @param config Pipeline configuration with source settings
   * @param variables Dimension values for this batch
   * @param rawCache cache for this batch; still returns content when caching is off or bypassed,
   *                 it just downloads every time, so an implementation never needs to branch on it
   * @return Iterator of records, or null to fall back to the built-in HttpSource
   * @throws IOException If data fetching fails
   */
  Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables,
      RawCache rawCache) throws IOException;

  /**
   * Routes the plain form through the cache-aware one with a non-caching handle, so a caller that
   * only knows about {@link DataProvider} still gets correct results.
   */
  @Override default Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    return fetch(config, variables, RawCache.passthrough());
  }
}
