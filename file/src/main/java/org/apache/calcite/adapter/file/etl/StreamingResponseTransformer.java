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
 * A {@link ResponseTransformer} that produces rows as a lazy iterator instead of a JSON string.
 *
 * <p>Implementing this interface eliminates the StringWriter accumulation that causes OOM
 * on large datasets. {@link HttpSource} detects {@code instanceof StreamingResponseTransformer}
 * and calls {@link #fetchAndTransform} directly, bypassing the normal
 * fetch → transform → parseResponse pipeline entirely.
 *
 * <p>The returned iterator is consumed by the ETL writer in fixed-size chunks (typically
 * 10k rows per Iceberg write), so memory usage is O(chunk_size) regardless of total row count.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>The iterator must be lazy: rows are produced one at a time, not buffered.</li>
 *   <li>Any per-table setup (file downloads, auxiliary lookups) happens before the iterator
 *       is returned, not inside {@code hasNext()}/{@code next()}.</li>
 *   <li>{@link #transform} always throws {@link UnsupportedOperationException}; callers
 *       must use {@link HttpSource} to invoke this transformer.</li>
 * </ul>
 */
public interface StreamingResponseTransformer extends ResponseTransformer {

  /**
   * Performs any necessary setup (file downloads, auxiliary lookups) and returns a lazy
   * iterator over the rows for this request.
   *
   * @param context Request context including the substituted URL, parameters, headers,
   *                and dimension values (e.g., {@code {"year": "2023"}})
   * @return Lazy row iterator; each map holds typed values (String, Integer, Double, Boolean, null)
   * @throws IOException if setup (download, file open) fails
   */
  Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) throws IOException;

  /**
   * Always throws — streaming transformers are not invoked via the legacy string pipeline.
   *
   * @throws UnsupportedOperationException always
   */
  @Override default String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " is a streaming transformer; "
            + "it must be invoked via HttpSource.fetch(), not transform()");
  }
}
