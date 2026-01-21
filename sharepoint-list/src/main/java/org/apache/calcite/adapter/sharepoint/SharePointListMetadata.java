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

import java.util.List;

/**
 * Metadata for a SharePoint list.
 */
public class SharePointListMetadata {
  private final String listId;
  private final String listName;       // SQL-friendly name (lowercase)
  private final String displayName;    // Original SharePoint display name
  private final String entityTypeName;
  private final List<SharePointColumn> columns;

  public SharePointListMetadata(String listId, String displayName, String entityTypeName,
      List<SharePointColumn> columns) {
    this.listId = listId;
    this.displayName = displayName;
    this.listName = SharePointNameConverter.toSqlName(displayName);
    this.entityTypeName = entityTypeName;
    this.columns = columns;
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
