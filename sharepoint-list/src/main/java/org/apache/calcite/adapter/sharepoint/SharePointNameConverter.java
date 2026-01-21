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

import java.util.Locale;

/**
 * Converts between SharePoint display names and SQL-friendly names.
 * Follows PostgreSQL convention of using lowercase names with underscores.
 */
public class SharePointNameConverter {

  /**
   * Converts a SharePoint display name to a SQL-friendly name.
   * Examples:
   * - "Project Tasks" → "project_tasks"
   * - "Due Date" → "due_date"
   * - "Is Complete?" → "is_complete"
   * - "FY2024 Budget" → "fy2024_budget"
   *
   * Limitation: Null or empty input is returned as-is. Callers should provide
   * a fallback value (e.g., internal name) before calling this method.
   */
  public static String toSqlName(String sharePointName) {
    if (sharePointName == null || sharePointName.isEmpty()) {
      return sharePointName;
    }

    // Convert to lowercase
    String result = sharePointName.toLowerCase(Locale.ROOT);

    // Replace spaces with underscores
    result = result.replace(" ", "_");
    result = result.replace("-", "_");

    // Remove special characters that aren't valid in SQL identifiers
    StringBuilder cleaned = new StringBuilder();
    for (char c : result.toCharArray()) {
      if (Character.isLetterOrDigit(c) || c == '_') {
        cleaned.append(c);
      }
    }
    result = cleaned.toString();

    // Remove duplicate underscores
    while (result.contains("__")) {
      result = result.replace("__", "_");
    }

    // Remove leading/trailing underscores
    while (result.startsWith("_")) {
      result = result.substring(1);
    }
    while (result.endsWith("_")) {
      result = result.substring(0, result.length() - 1);
    }

    // Handle empty result (all special chars)
    if (result.isEmpty()) {
      result = "column";
    }

    // Ensure it doesn't start with a number
    if (Character.isDigit(result.charAt(0))) {
      result = "c_" + result;
    }

    return result;
  }

  /**
   * Creates a SharePoint-friendly display name from a SQL name.
   * This is used when creating new lists/columns.
   * Examples:
   * - "project_tasks" → "Project Tasks"
   * - "due_date" → "Due Date"
   * - "is_complete" → "Is Complete"
   */
  public static String toSharePointName(String sqlName) {
    if (sqlName == null || sqlName.isEmpty()) {
      return sqlName;
    }

    // Split by underscores
    String[] parts = sqlName.split("_");
    StringBuilder result = new StringBuilder();

    // Capitalize each part
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        result.append(" ");
      }
      if (!parts[i].isEmpty()) {
        result.append(Character.toUpperCase(parts[i].charAt(0)));
        if (parts[i].length() > 1) {
          result.append(parts[i].substring(1).toLowerCase(Locale.ROOT));
        }
      }
    }

    return result.toString();
  }
}
