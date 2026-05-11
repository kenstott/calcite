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
  @Override
  default String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " is a streaming transformer; "
            + "it must be invoked via HttpSource.fetch(), not transform()");
  }
}
