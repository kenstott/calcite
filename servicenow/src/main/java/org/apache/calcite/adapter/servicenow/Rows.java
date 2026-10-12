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

/** Reads fields out of Table API result rows, failing on any shape the adapter does not expect. */
final class Rows {
  private Rows() {}

  /**
   * Returns the stored value of a field as text.
   *
   * <p>With {@code sysparm_display_value=all} every field is an object with {@code value} and
   * {@code display_value}. With display values off a field is a string; a reference field is
   * either a string (link excluded) or an object with {@code link} and {@code value}. A missing
   * field, a JSON null, a number or any other object is an error: the adapter requested the field,
   * so its absence means a hidden field or a changed response, and neither can be told apart from
   * an empty value.
   *
   * @param table    table name, for the error message
   * @param required whether an empty string is an error (true for sys_id and metadata keys)
   */
  static String text(JsonNode row, String field, String table, boolean required) {
    final JsonNode node = row.path(field);
    final String text;
    if (node.isTextual()) {
      text = node.asText();
    } else if (node.isObject() && node.path("value").isTextual()) {
      text = node.path("value").asText();
    } else {
      throw new ServiceNowException("Row of " + table + " has no usable '" + field + "' field "
          + "(expected a string or an object with a string 'value'; the field is hidden by an ACL "
          + "or the response shape changed). Found: " + abbreviate(node));
    }
    if (required && text.isEmpty()) {
      throw new ServiceNowException("Row of " + table + " has an empty '" + field + "'");
    }
    return text;
  }

  /**
   * Returns the stored value of a field of a data row, accepting only the shape the request asked
   * for: an object with {@code value} under {@code sysparm_display_value=all}; otherwise a string,
   * or for a reference field a string or an object with {@code value}.
   */
  static String stored(JsonNode row, String field, String table, boolean displayMode,
      boolean reference) {
    final JsonNode node = row.path(field);
    if (displayMode ? !node.isObject() : !(node.isTextual() || reference && node.isObject())) {
      throw new ServiceNowException("Row of " + table + " has the wrong shape for '" + field
          + "' (displayMode=" + displayMode + ", reference=" + reference + "). Found: "
          + abbreviate(node));
    }
    return text(row, field, table, false);
  }

  /**
   * Returns the display value of a field, which only a {@code sysparm_display_value=all} response
   * carries.
   */
  static String displayText(JsonNode row, String field, String table) {
    final JsonNode node = row.path(field).path("display_value");
    if (!node.isTextual()) {
      throw new ServiceNowException("Row of " + table + " has no 'display_value' for '" + field
          + "' although sysparm_display_value=all was requested. Found: "
          + abbreviate(row.path(field)));
    }
    return node.asText();
  }

  private static String abbreviate(JsonNode node) {
    final String text = node.isMissingNode() ? "(missing)" : node.toString();
    return text.length() <= 200 ? text : text.substring(0, 200) + "...";
  }
}
