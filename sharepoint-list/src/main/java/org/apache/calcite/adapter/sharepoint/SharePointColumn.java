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

/**
 * Represents a column in a SharePoint list.
 */
public class SharePointColumn {
  private final String internalName;    // SharePoint's internal field name
  private final String name;             // SQL-friendly name (lowercase)
  private final String displayName;      // Original display name
  private final String type;
  private final boolean required;

  public SharePointColumn(String internalName, String displayName, String type, boolean required) {
    this.internalName = internalName;
    // Use internalName as fallback if displayName is null or empty
    this.displayName = (displayName != null && !displayName.isEmpty()) ? displayName : internalName;
    this.name = SharePointNameConverter.toSqlName(this.displayName);
    this.type = type;
    this.required = required;
  }

  public String getInternalName() {
    return internalName;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getType() {
    return type;
  }

  public boolean isRequired() {
    return required;
  }
}
