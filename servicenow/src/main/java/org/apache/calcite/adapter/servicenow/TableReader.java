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
package org.apache.calcite.adapter.servicenow;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reads the rows of one table through the Table API, one page per request, lazily.
 *
 * <p>Paging is by key, not by offset. Each page asks for
 * {@code <extraQuery>^sys_id><last sys_id seen>^ORDERBYsys_id} with {@code sysparm_limit} set to
 * the page size. {@code sys_id} is unique and indexed, so no row is skipped or repeated when rows
 * are inserted while a scan is running; offset paging over changing data loses and duplicates
 * rows. The {@code ORDERBYsys_id} here orders the scan itself and is not a pushed-down user sort.
 *
 * <p>Two rules keep the scan from returning a plausible but incomplete result:
 * <ul>
 *   <li>The scan ends only on an empty page, never on a short one. ServiceNow applies
 *   {@code sysparm_limit} before ACL evaluation, so a page can hold fewer rows than the limit
 *   while readable rows still follow.
 *   <li>Every page must advance past the previous key. ServiceNow drops a {@code sysparm_query}
 *   term it cannot parse and runs the rest; if it ever dropped the key term the same page would
 *   come back forever. That is detected and reported instead.
 * </ul>
 *
 * <p>Because the scan is lazy, a consumer that stops early (a {@code LIMIT} above the scan) stops
 * the page fetches too.
 */
class TableReader implements Iterator<JsonNode> {

  private final ServiceNowConnection connection;
  private final String table;
  private final String fields;
  private final boolean displayValues;
  private final int pageSize;
  private final String extraQuery;

  private Iterator<JsonNode> page;
  private String lastKey;
  private boolean finished;
  private int requests;

  /**
   * Creates a reader.
   *
   * @param fields        ServiceNow field names to request; must contain {@code sys_id}
   * @param displayValues true to request {@code sysparm_display_value=all}, which returns a
   *                      {@code value} and a {@code display_value} for every field; false to
   *                      request stored values with reference links excluded
   * @param extraQuery    encoded-query terms to AND with the paging term, or empty. Always empty
   *                      in this release: see {@link PredicatePushdown}
   */
  TableReader(ServiceNowConnection connection, String table, List<String> fields,
      boolean displayValues, int pageSize, String extraQuery) {
    if (!fields.contains("sys_id")) {
      throw new IllegalArgumentException(
          "Keyset paging needs sys_id among the requested fields of " + table + ": " + fields);
    }
    if (pageSize < 1) {
      throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
    }
    this.connection = connection;
    this.table = table;
    this.fields = String.join(",", fields);
    this.displayValues = displayValues;
    this.pageSize = pageSize;
    this.extraQuery = extraQuery;
  }

  /** Number of requests made so far. */
  int requestCount() {
    return requests;
  }

  @Override public boolean hasNext() {
    while (!finished && (page == null || !page.hasNext())) {
      fetchPage();
    }
    return !finished;
  }

  @Override public JsonNode next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return page.next();
  }

  private void fetchPage() {
    final StringBuilder query = new StringBuilder(extraQuery);
    if (lastKey != null) {
      if (query.length() > 0) {
        query.append('^');
      }
      query.append("sys_id>").append(lastKey);
    }
    if (query.length() > 0) {
      query.append('^');
    }
    query.append("ORDERBYsys_id");

    final Map<String, String> params = new LinkedHashMap<>();
    params.put("sysparm_query", query.toString());
    params.put("sysparm_limit", Integer.toString(pageSize));
    params.put("sysparm_fields", fields);
    params.put("sysparm_no_count", "true");
    if (displayValues) {
      params.put("sysparm_display_value", "all");
    } else {
      params.put("sysparm_display_value", "false");
      params.put("sysparm_exclude_reference_link", "true");
    }

    requests++;
    final JsonNode result = connection.getTable(table, params).body.path("result");
    if (!result.isArray()) {
      throw new ServiceNowException("Table API response for " + table + " has no 'result' array; "
          + "the response was: " + abbreviate(result));
    }
    if (result.size() == 0) {
      // The only end of data the adapter trusts; see the class comment
      finished = true;
      return;
    }
    String previous = lastKey;
    for (JsonNode row : result) {
      final String key = Rows.text(row, "sys_id", table, true);
      if (previous != null && key.compareTo(previous) <= 0) {
        throw new ServiceNowException("Keyset paging of " + table + " did not advance: sys_id "
            + key + " arrived after " + previous + ". ServiceNow most likely ignored the "
            + "'sys_id>' term (it drops query terms it cannot parse) or orders sys_id differently "
            + "than it compares it; continuing would repeat rows or loop forever.");
      }
      previous = key;
    }
    lastKey = previous;
    page = result.iterator();
  }

  private static String abbreviate(JsonNode node) {
    final String text = node.toString();
    return text.length() <= 200 ? text : text.substring(0, 200) + "...";
  }
}
