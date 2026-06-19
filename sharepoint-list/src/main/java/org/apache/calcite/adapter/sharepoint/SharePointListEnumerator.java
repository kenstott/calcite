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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.linq4j.Enumerator;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Enumerator that fetches and returns SharePoint list items.
 */
public class SharePointListEnumerator implements Enumerator<Object[]> {
  private final SharePointListMetadata metadata;
  private final MicrosoftGraphListClient client;
  private Iterator<Map<String, Object>> iterator;
  private Object[] current;

  public SharePointListEnumerator(SharePointListMetadata metadata, MicrosoftGraphListClient client) {
    this.metadata = metadata;
    this.client = client;
    reset();
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      Map<String, Object> item = iterator.next();
      current = convertToRow(item);
      return true;
    }
    return false;
  }

  @Override public void reset() {
    try {
      List<Map<String, Object>> items = client.getListItems(metadata.getListId());
      iterator = items.iterator();
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch SharePoint list items", e);
    }
  }

  @Override public void close() {
    // Nothing to close
  }

  private Object[] convertToRow(Map<String, Object> item) {
    List<SharePointColumn> columns = metadata.getColumns();
    Object[] row = new Object[columns.size() + 1]; // +1 for ID column

    // First column is always the ID
    row[0] = item.get("id");

    // Then the other columns
    for (int i = 0; i < columns.size(); i++) {
      SharePointColumn column = columns.get(i);
      Object value = item.get(column.getInternalName());
      row[i + 1] = convertValue(value, column.getType());
    }

    return row;
  }

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private Object convertValue(Object value, String type) {
    if (value == null) {
      return null;
    }

    switch (type.toLowerCase(Locale.ROOT)) {
    case "number":
    case "currency":
      return value instanceof Number ? ((Number) value).doubleValue()
          : Double.parseDouble(value.toString());
    case "integer":
    case "counter":
      return value instanceof Number ? ((Number) value).intValue()
          : Integer.parseInt(value.toString());
    case "boolean":
      return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
    case "datetime":
      // Calcite represents TIMESTAMP as epoch milliseconds (a number); returning a
      // java.sql.Timestamp object breaks the Avatica/JDBC cursor accessor. SharePoint returns
      // ISO-8601 UTC strings.
      return toEpochMillis(value.toString());
    case "lookup":
    case "user":
      // These are complex objects in SharePoint, extract the display value when present.
      if (value instanceof Map) {
        Map<String, Object> lookupValue = (Map<String, Object>) value;
        Object title = lookupValue.get("Title") != null
            ? lookupValue.get("Title") : lookupValue.get("LookupValue");
        return title != null ? title.toString() : toJson(value);
      }
      return value.toString();
    case "multichoice":
      // Multiple choice fields return arrays
      if (value instanceof List) {
        return String.join(", ", (List<String>) value);
      }
      return value.toString();
    default:
      // Coerce any remaining complex (object/array) value to JSON so it fits the VARCHAR column;
      // scalars fall through to their string form.
      if (value instanceof Map || value instanceof List) {
        return toJson(value);
      }
      return value.toString();
    }
  }

  /** Parses a SharePoint ISO-8601 datetime into epoch milliseconds. */
  private static Long toEpochMillis(String value) {
    try {
      return java.time.Instant.parse(value).toEpochMilli();
    } catch (RuntimeException e) {
      // Fall back for values without an explicit zone/offset.
      return java.sql.Timestamp.valueOf(
          value.replace("T", " ").replace("Z", "")).getTime();
    }
  }

  /** Serializes a complex SharePoint value to a JSON string. */
  private static String toJson(Object value) {
    try {
      return JSON_MAPPER.writeValueAsString(value);
    } catch (Exception e) {
      return value.toString();
    }
  }
}
