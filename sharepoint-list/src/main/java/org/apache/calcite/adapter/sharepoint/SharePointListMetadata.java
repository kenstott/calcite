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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Metadata for a SharePoint list.
 */
public class SharePointListMetadata {
  // List templates whose "rows" are not plain list items (files, survey responses, threads), so
  // INSERT/UPDATE/DELETE through the list-item API does not apply - they are exposed read-only.
  private static final List<String> READ_ONLY_TEMPLATES = Arrays.asList(
      "documentlibrary", "picturelibrary", "webpagelibrary", "mysitedocumentlibrary",
      "xmlform", "survey", "discussionboard");

  private final String listId;
  private final String listName;       // SQL-friendly name (lowercase)
  private final String displayName;    // Original SharePoint display name
  private final String entityTypeName;
  private final List<SharePointColumn> columns;
  private final String template;       // Graph list template (e.g. genericList, documentLibrary)

  public SharePointListMetadata(String listId, String displayName, String entityTypeName,
      List<SharePointColumn> columns) {
    this(listId, displayName, entityTypeName, columns, null);
  }

  public SharePointListMetadata(String listId, String displayName, String entityTypeName,
      List<SharePointColumn> columns, String template) {
    this.listId = listId;
    this.displayName = displayName;
    this.listName = SharePointNameConverter.toSqlName(displayName);
    this.entityTypeName = entityTypeName;
    this.columns = columns;
    this.template = template;
  }

  public String getTemplate() {
    return template;
  }

  /**
   * Whether this list accepts writes (INSERT/UPDATE/DELETE) through the list-item API. Generic
   * lists, task lists and calendars do; document/picture libraries, surveys and discussion boards
   * do not. Unknown templates default to writable to preserve prior behaviour.
   */
  public boolean isWritable() {
    return template == null
        || !READ_ONLY_TEMPLATES.contains(template.toLowerCase(Locale.ROOT));
  }

  public String getListId() {
    return listId;
  }

  public String getListName() {
    return listName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getEntityTypeName() {
    return entityTypeName;
  }

  public List<SharePointColumn> getColumns() {
    return columns;
  }
}
