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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for transformers that receive CSV rows (with header) and map them to output records.
 *
 * <p>Implements {@link PerRecordResponseTransformer} so {@code HttpSource} streams the CSV one row
 * at a time (its per-record cache path) instead of reading the whole response into a single String.
 * This keeps memory O(1) per row — essential for the multi-GB bulk CSVs (e.g. CMS Open Payments),
 * where the legacy whole-String {@link #transform(String, RequestContext)} path OOMed. That String
 * path is retained only as a fallback for callers that do not stream.
 *
 * <p>Subclasses implement {@link #mapRow(String[], String[], ObjectNode)} to produce output records;
 * the same hook serves both the streaming and fallback paths.
 */
public abstract class AbstractCsvResponseTransformer implements PerRecordResponseTransformer {
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Streaming per-record path: {@code row} holds one CSV record keyed by header name. Reuse
   * {@link #mapRow} to build the output columns, then replace the record's contents in place (the
   * streaming writer consumes the mutated map), so the full CSV is never buffered as one String.
   */
  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    int n = row.size();
    String[] headers = new String[n];
    String[] values = new String[n];
    int i = 0;
    for (Map.Entry<String, Object> e : row.entrySet()) {
      headers[i] = e.getKey();
      Object v = e.getValue();
      values[i] = v == null ? "" : String.valueOf(v);
      i++;
    }
    ObjectNode mapped = MAPPER.createObjectNode();
    mapRow(headers, values, mapped);
    row.clear();
    Iterator<Map.Entry<String, JsonNode>> fields = mapped.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> e = fields.next();
      JsonNode val = e.getValue();
      row.put(e.getKey(), val == null || val.isNull() ? null : val.asText());
    }
  }

  @Override
  public String transform(String response, RequestContext context) {
    try {
      BufferedReader reader = new BufferedReader(new StringReader(response));
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return "[]";
      }
      String[] headers = parseCsvLine(headerLine);
      ArrayNode out = MAPPER.createArrayNode();

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        String[] values = parseCsvLine(line);
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(headers, values, row);
        out.add(row);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform CSV response", e);
    }
  }

  protected abstract void mapRow(String[] headers, String[] values, ObjectNode row);

  protected static String col(String[] headers, String[] values, String name) {
    for (int i = 0; i < headers.length; i++) {
      if (headers[i].equalsIgnoreCase(name)) {
        if (i < values.length) {
          String v = values[i].trim();
          return v.isEmpty() ? null : v;
        }
        return null;
      }
    }
    return null;
  }

  protected static void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  protected static String truncate(String value, int maxLength) {
    if (value == null) {
      return null;
    }
    return value.length() > maxLength ? value.substring(0, maxLength) : value;
  }

  /**
   * Parses a complete CSV record (possibly spanning multiple physical lines)
   * into fields. Delegates to {@link CsvRecordReader#splitFields} so all CSV
   * parsing in govdata uses one canonical implementation.
   */
  protected static String[] parseCsvLine(String line) {
    return CsvRecordReader.splitFields(line, ',').toArray(new String[0]);
  }
}
