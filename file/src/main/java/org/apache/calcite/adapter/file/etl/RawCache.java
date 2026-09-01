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
import java.io.InputStream;

/**
 * A cached download, for a {@link DataProvider} that fetches over HTTP.
 *
 * <p>A provider exists to reach a source the built-in HTTP path cannot: a shapefile or other
 * binary the response transformer's {@code String} signature cannot carry, or a file whose real
 * URL has to be discovered by crawling a listing page. Neither of those has anything to do with
 * caching — but because a provider replaces {@link HttpSource} outright, and every line of raw
 * caching lives inside it, a provider that opened its own connection silently gave up the cache
 * as well. That is an accident of where the code sits, not a property of the source.
 *
 * <p>This hands the cache back. A provider calls {@link #openStream} with whatever URL it worked
 * out for itself, and gets the same behaviour {@link HttpSource} would have given it: an existing
 * entry is served from storage, a miss is downloaded, stored, and then served. The provider keeps
 * full control of <em>what</em> to fetch and how to parse it, and stops paying for that control
 * with a re-download on every run.
 *
 * <p>Byte-oriented throughout, so a zip or other binary caches exactly as a CSV does.
 *
 * <p>The cache key is the pipeline's dimension combination, identical to the key
 * {@link HttpSource} would compute for the same batch, so a provider-backed table's entries sit
 * beside every other table's and obey the same invalidation. The URL is deliberately not part of
 * the key: for a discovery-based provider the URL is itself a function of the dimensions, and a
 * newly discovered URL for the same batch means the upstream file moved, not that a different
 * batch is being fetched.
 */
public interface RawCache {

  /**
   * Opens {@code url}, serving it from the cache when the entry for this batch already exists and
   * downloading and storing it when it does not.
   *
   * @param url the URL the provider resolved for this batch
   * @return a stream over the content; the caller closes it
   * @throws IOException if the download fails, or the entry can be neither read nor written
   */
  InputStream openStream(String url) throws IOException;

  /**
   * Whether this handle actually caches. False when the table sets
   * {@code rawCache.enabled: false}, or when the run is bypassing the cache (a force-download or
   * a full reprocess). {@link #openStream} still works and still returns the content — it just
   * downloads every time — so a provider never needs to branch on this. It is here so a provider
   * that wants to say something different in a log line can.
   *
   * @return whether reads may be served from storage
   */
  boolean isEnabled();

  /** A handle that always downloads, for callers constructed without caching available. */
  static RawCache passthrough() {
    return new RawCache() {
      @Override public InputStream openStream(String url) throws IOException {
        return java.net.URI.create(url).toURL().openStream();
      }

      @Override public boolean isEnabled() {
        return false;
      }
    };
  }
}
