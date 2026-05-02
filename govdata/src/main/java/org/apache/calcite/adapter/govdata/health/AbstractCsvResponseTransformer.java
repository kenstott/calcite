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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for transformers that receive CSV batches (with header line) and return JSON.
 *
 * <p>The input is a CSV string: first line is the header, subsequent lines are data rows.
 * Subclasses implement {@link #mapRow(String[], String[], ObjectNode)} to produce output records.
 */
public abstract class AbstractCsvResponseTransformer implements ResponseTransformer {
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      BufferedReader reader = new BufferedReader(new StringReader(response));
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return "[]";
      }
      String[] headers = parseCsvLine(headerLine);
      ArrayNode out = MAPPER.createArrayNode();

      String line;
      while ((line = reader.readLine()) != null) {
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
   * Parses a single CSV line respecting quoted fields.
   */
  protected static String[] parseCsvLine(String line) {
    List<String> fields = new ArrayList<String>();
    boolean inQuotes = false;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          sb.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        fields.add(sb.toString());
        sb.setLength(0);
      } else {
        sb.append(c);
      }
    }
    fields.add(sb.toString());
    return fields.toArray(new String[0]);
  }
}
