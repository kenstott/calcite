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
 * <p>Entries live under the directory {@link HttpSource} would compute for the same batch, so a
 * provider-backed table's cache sits beside every other table's and obeys the same invalidation.
 * Within that directory an entry is keyed by URL, because a provider may fetch several URLs for
 * one batch — the IRS business master file is four regional shards — and a single per-batch entry
 * would make them overwrite one another and then serve whichever landed last for all four.
 *
 * <p>A consequence worth stating: a source that moves its file re-fetches rather than replaying
 * the entry under the old URL. That is the safe direction, since an entry here is validated by
 * existence alone.
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
   * Serves the entry for {@code key}, calling {@code onMiss} to produce the content when there is
   * none — for a provider whose fetch is not a plain GET of {@code key}.
   *
   * <p>The plain {@link #openStream(String)} form covers a provider that downloads exactly the URL
   * it names, and most do. The rest do not, in ways that have nothing in common except that the
   * caching is identical: the SSA workbooks are unreachable directly and are pulled from a Wayback
   * capture derived from the canonical URL; the USAspending and RePORTER endpoints are POSTs whose
   * body is built from the batch's dimensions; a discovery-based provider crawls a listing to find
   * the file. Each knows how to fetch its own content and only wants somewhere to keep it.
   *
   * <p>{@code key} names the content, not the transport. Use the canonical URL — the SSA workbook
   * rather than the Wayback wrapper around it, the endpoint plus whatever distinguishes the request
   * rather than an opaque handle — so the entry stays stable when the way it is retrieved changes.
   *
   * @param key stable identity for the content within this batch
   * @param onMiss produces the content when nothing is cached; not called on a hit
   * @return a stream over the content; the caller closes it
   * @throws IOException if the supplier fails, or the entry can be neither read nor written
   */
  InputStream openStream(String key, ContentSupplier onMiss) throws IOException;

  /** Produces content for a cache miss. */
  @FunctionalInterface
  interface ContentSupplier {
    /**
     * Opens the content.
     *
     * @return a stream the cache will consume and close
     * @throws IOException if the fetch fails
     */
    InputStream open() throws IOException;
  }

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

      @Override public InputStream openStream(String key, ContentSupplier onMiss)
          throws IOException {
        return onMiss.open();
      }

      @Override public boolean isEnabled() {
        return false;
      }
    };
  }
}
