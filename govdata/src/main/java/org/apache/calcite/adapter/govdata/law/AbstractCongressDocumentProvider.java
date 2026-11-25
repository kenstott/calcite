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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SharedHttpClient;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.GovDataException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Base of the providers that list the documents of one Congress and then read one document per
 * listed item. Each document is cached under the item's key and the source's change stamp, so a
 * daily refresh re-reads only what the source changed, and tables built from the same source
 * share the cache.
 *
 * <p>The listing is read in full before any document, and a listing that yields fewer distinct
 * items than the source's own total fails: paging by offset over a source that is being updated
 * can skip an item, and a partition replaced from such a listing would drop it.
 *
 * <p>Failures are not swallowed: an HTTP error that outlasts the retries or a document without
 * the expected shape fails the crawl.
 */
abstract class AbstractCongressDocumentProvider implements StorageAwareDataProvider {

  /** One listed document: where to read it and the key and change stamp that cache it. */
  static final class Item {
    final String key;
    final String stamp;
    final String url;

    Item(String key, String stamp, String url) {
      this.key = key;
      this.stamp = stamp;
      this.url = url;
    }
  }

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  /**
   * Every item of one Congress, keyed by {@link Item#key} so a repeat across pages counts once.
   *
   * @throws IOException when a listing page cannot be read
   */
  abstract Map<String, Item> list(String baseUrl, String congress, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException;

  /** The directory under the raw cache that holds this source's documents. */
  abstract String cacheName();

  /** The file extension of a cached document. */
  abstract String extension();

  /** The headers of a document request, given those of the table's source block. */
  abstract Map<String, String> documentHeaders(Map<String, String> sourceHeaders);

  /** The rows one document contributes to the named table; throws if it is malformed. */
  abstract List<Map<String, Object>> rows(String table, Item item, byte[] document)
      throws IOException;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    HttpSourceConfig source = config.getSource();
    final String table = config.getName();
    final String congress = required(variables, "congress");
    required(variables, "refresh_date");
    String baseUrl = VariableResolver.substitute(source.getUrl(), variables);
    requireResolved("url", baseUrl);
    final Map<String, String> headers = new LinkedHashMap<String, String>();
    if (source.getHeaders() != null) {
      for (Map.Entry<String, String> e : source.getHeaders().entrySet()) {
        String value = VariableResolver.substitute(e.getValue(), variables);
        requireResolved("header " + e.getKey(), value);
        headers.put(e.getKey(), value);
      }
    }
    // dq_record_limit is this provider's own knob, not an API parameter: in a DQ run it caps the
    // documents read per Congress, so a DQ crawl does not read a whole Congress.
    int recordLimit = 0;
    String limit = source.getParameters() == null ? null
        : source.getParameters().get("dq_record_limit");
    if (limit != null && isDqSampleMode()) {
      recordLimit = Integer.parseInt(limit.trim());
    }
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    final HttpSourceConfig.RateLimitConfig rateLimit = source.getRateLimit();
    final Deque<Item> items =
        new ArrayDeque<Item>(list(baseUrl, congress, headers, rateLimit).values());
    final int toRead = recordLimit > 0 ? Math.min(recordLimit, items.size()) : items.size();
    return new Iterator<Map<String, Object>>() {
      private final Deque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
      private int read;

      @Override public boolean hasNext() {
        while (pending.isEmpty() && read < toRead) {
          Item item = items.poll();
          read++;
          try {
            pending.addAll(document(table, item, headers, rateLimit));
          } catch (IOException e) {
            throw new GovDataException(table + ": reading " + item.url + " failed", e);
          }
        }
        return !pending.isEmpty();
      }

      @Override public Map<String, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return pending.poll();
      }
    };
  }

  private List<Map<String, Object>> document(String table, Item item,
      Map<String, String> headers, HttpSourceConfig.RateLimitConfig rateLimit)
      throws IOException {
    String path = storageProvider.resolvePath(cacheBaseDir, cacheName() + "/" + item.key
        + "/update=" + item.stamp.replaceAll("[^0-9A-Za-z]", "") + "." + extension());
    if (storageProvider.exists(path)) {
      try (InputStream in = storageProvider.openInputStream(path)) {
        return rows(table, item, readAll(in));
      }
    }
    byte[] body = get(item.url, documentHeaders(headers), rateLimit);
    // Cache only a document that parses into the expected shape.
    List<Map<String, Object>> rows = rows(table, item, body);
    storageProvider.writeFile(path, body);
    return rows;
  }

  static byte[] get(String url, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException {
    SharedHttpClient.Response conn = RetryableHttp.openWithRetry(url, headers, rateLimit, true);
    try (InputStream in = conn.getInputStream()) {
      return readAll(in);
    } finally {
      conn.disconnect();
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[8192];
    int n;
    while ((n = in.read(chunk)) > 0) {
      buf.write(chunk, 0, n);
    }
    return buf.toByteArray();
  }

  /** Fails a listing that yielded fewer distinct items than the source says it holds. */
  static void requireComplete(String url, int distinct, int total) {
    if (distinct < total) {
      throw new GovDataException("Listing " + url + " yielded " + distinct
          + " distinct items of the " + total + " the source reports; the source changed while "
          + "it was being paged, so rerun");
    }
  }

  /** True when DQ sample mode is active; GOVDATA_DQ is an allowed global run flag. */
  private static boolean isDqSampleMode() {
    String v = System.getProperty("GOVDATA_DQ");
    if (v == null) {
      v = System.getenv("GOVDATA_DQ");
    }
    return "true".equalsIgnoreCase(v);
  }

  private String required(Map<String, String> variables, String name) {
    String v = variables.get(name);
    if (v == null || v.isEmpty()) {
      throw new GovDataException(getClass().getSimpleName() + ": dimension '" + name
          + "' is required but was not supplied");
    }
    return v;
  }

  /** An unset {@code ${VAR}} is left in place by the resolver; the API would answer 403. */
  private void requireResolved(String what, String value) {
    if (value.contains("${")) {
      throw new GovDataException(getClass().getSimpleName() + ": " + what
          + " still contains an unresolved variable (" + value
          + "); set the environment variable it names");
    }
  }
}
