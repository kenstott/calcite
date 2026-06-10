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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipInputStream;

/**
 * Base for EIA bulk-download transformers: streams a cumulative EIA bulk ZIP (one NDJSON
 * v1-series record per line) and flat-maps each series into materialized rows via
 * {@link #rowsForSeries}, replacing per-period EIA API v2 fan-out.
 *
 * <p>Streaming throughout — the ZIP is read line-by-line and only one series' exploded rows
 * are buffered at a time, so memory stays bounded for any bulk file (NG 24 MB … ELEC 238 MB).
 * Subclasses implement the dataset-specific series filter, {@code series_id} parsing, and
 * {@code data[[period,value]]} explode.
 */
public abstract class EiaBulkSeriesTransformer implements StreamingResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(openBulkTxt(context.getUrl()), StandardCharsets.UTF_8));
    return new Iterator<Map<String, Object>>() {
      private final ArrayDeque<Map<String, Object>> pending =
          new ArrayDeque<Map<String, Object>>();
      private boolean closed;

      private void fill() {
        try {
          String line;
          while (pending.isEmpty() && (line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
              pending.addAll(rowsForSeries(MAPPER.readTree(line)));
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed streaming EIA bulk NDJSON", e);
        }
        if (pending.isEmpty() && !closed) {
          closed = true;
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort
          }
        }
      }

      @Override public boolean hasNext() {
        fill();
        return !pending.isEmpty();
      }

      @Override public Map<String, Object> next() {
        fill();
        if (pending.isEmpty()) {
          throw new NoSuchElementException();
        }
        return pending.poll();
      }
    };
  }

  /**
   * Filters and explodes one v1 series record into output rows, or an empty list when the
   * series is not relevant to this dataset/table.
   */
  protected abstract List<Map<String, Object>> rowsForSeries(JsonNode series);

  /** Downloads the bulk ZIP and returns a stream positioned at its first entry's content. */
  protected static InputStream openBulkTxt(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("EIA bulk download HTTP " + code + ": " + url);
    }
    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    if (zis.getNextEntry() == null) {
      zis.close();
      throw new IOException("EIA bulk ZIP is empty: " + url);
    }
    return zis;
  }

  /** Convenience for subclasses: the {@code .} -separated body of a series_id between the
   * leading dataset prefix and the trailing frequency suffix, split into segments by '_'. */
  protected static String[] seriesIdBody(String seriesId) {
    String body = seriesId.substring(seriesId.indexOf('.') + 1, seriesId.lastIndexOf('.'));
    return body.split("_");
  }
}
