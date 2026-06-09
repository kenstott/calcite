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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

/**
 * Prototype: streams the EIA bulk {@code NG.zip} (one cumulative NDJSON file, one v1 series
 * per line) and materializes weekly natural-gas underground-storage rows — a drop-in
 * replacement for the per-period {@code eia_natural_gas_storage} EIA API v2 fan-out.
 *
 * <p>Each storage series ({@code NG.NW2_EPG0_S..._R##_BCF.W}) carries the full history in a
 * {@code data: [[YYYYMMDD, value], ...]} array; this transformer filters to the storage family,
 * parses the dimensions out of the {@code series_id}, and <b>explodes</b> the data array into one
 * row per (region × storage_type × report_date), matching the existing table's columns.
 *
 * <p>Streaming throughout: the 24 MB file is read line-by-line and only one series' exploded
 * rows are buffered at a time, so memory stays bounded (the same approach scales to the 238 MB
 * ELEC.zip).
 */
public class EiaNgStorageBulkTransformer implements StreamingResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Weekly storage-volume series by region, e.g. NG.NW2_EPG0_SWO_R48_BCF.W. */
  private static final Pattern STORAGE_SERIES =
      Pattern.compile("^NG\\.NW2_EPG0_S[A-Z]+_R\\d+_BCF\\.W$");

  private static final DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyyMMdd");

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final InputStream ndjson = openBulkTxt(context.getUrl());
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(ndjson, StandardCharsets.UTF_8));
    return new Iterator<Map<String, Object>>() {
      private final ArrayDeque<Map<String, Object>> pending =
          new ArrayDeque<Map<String, Object>>();
      private boolean done;

      private void fill() {
        try {
          String line;
          while (pending.isEmpty() && (line = reader.readLine()) != null) {
            if (line.isEmpty()) {
              continue;
            }
            pending.addAll(rowsForSeries(MAPPER.readTree(line)));
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed streaming NG bulk NDJSON", e);
        }
        if (pending.isEmpty()) {
          close();
        }
      }

      private void close() {
        if (!done) {
          done = true;
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort close
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
   * Downloads the bulk ZIP and returns a stream positioned at the first (only) entry's content.
   * The {@link ZipInputStream} stays open over the underlying connection stream — the caller
   * reads it lazily.
   */
  private static InputStream openBulkTxt(String url) throws IOException {
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

  /**
   * Explodes one v1 series record into storage rows, or an empty list when the series is not a
   * weekly storage-volume-by-region series. Package-visible for unit testing.
   */
  static List<Map<String, Object>> rowsForSeries(JsonNode series) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    JsonNode sidNode = series.get("series_id");
    if (sidNode == null || !STORAGE_SERIES.matcher(sidNode.asText()).matches()) {
      return rows;
    }
    String seriesId = sidNode.asText();
    // NG.NW2_EPG0_SWO_R48_BCF.W -> [NW2, EPG0, SWO, R48, BCF]
    String body = seriesId.substring(seriesId.indexOf('.') + 1, seriesId.lastIndexOf('.'));
    String[] seg = body.split("_");
    String storageTypeCode = seg.length > 2 ? seg[2] : null;
    String regionCode = seg.length > 3 ? seg[3] : null;
    String units = series.path("unitsshort").asText("Bcf");
    String name = series.path("name").asText(null);
    String region = regionFromName(name);

    JsonNode data = series.get("data");
    if (data == null || !data.isArray()) {
      return rows;
    }
    for (JsonNode point : data) {
      if (!point.isArray() || point.size() < 2 || point.get(1).isNull()) {
        continue;
      }
      LocalDate date = LocalDate.parse(point.get(0).asText(), YMD);
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      // 'year' is the partition column (single cumulative fetch → partition by the data year).
      row.put("year", date.getYear());
      row.put("series_id", seriesId);
      row.put("report_date", date.toString());
      row.put("storage_year", date.getYear());
      row.put("storage_week", date.get(WeekFields.ISO.weekOfWeekBasedYear()));
      row.put("eia_region_code", regionCode);
      row.put("region", region);
      row.put("storage_type_code", storageTypeCode);
      row.put("storage_type", "SWO".equals(storageTypeCode) ? "Working Gas" : storageTypeCode);
      row.put("volume_bcf", point.get(1).asDouble());
      row.put("units", units);
      rows.add(row);
    }
    return rows;
  }

  /** Extracts the region label from the series name, e.g. "Weekly East Region ..." -> "East". */
  private static String regionFromName(String name) {
    if (name == null) {
      return null;
    }
    int idx = name.indexOf(" Region");
    if (idx < 0) {
      return name.contains("Lower 48") ? "Lower 48 States" : null;
    }
    String head = name.substring(0, idx);
    int sp = head.lastIndexOf(' ');
    return sp >= 0 ? head.substring(sp + 1) : head;
  }
}
