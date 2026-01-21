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

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;
import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

/**
 * Factory for creating SharePoint list tables.
 */
public class SharePointListTableFactory implements TableFactory<Table> {

  public static final SharePointListTableFactory INSTANCE = new SharePointListTableFactory();

  private SharePointListTableFactory() {
  }

  @Override public Table create(SchemaPlus schema, String name, Map<String, Object> operand,
      RelDataType rowType) {
    String siteUrl = (String) operand.get("siteUrl");
    String listId = (String) operand.get("listId");
    String listName = (String) operand.get("listName");

    if (siteUrl == null) {
      throw new RuntimeException("siteUrl is required");
    }

    if (listId == null && listName == null) {
      throw new RuntimeException("Either listId or listName is required");
    }

    try {
      // Extract auth config from operand
      Map<String, Object> authConfig = new java.util.HashMap<>();
      authConfig.put("authType", operand.get("authType"));
      authConfig.put("clientId", operand.get("clientId"));
      authConfig.put("clientSecret", operand.get("clientSecret"));
      authConfig.put("tenantId", operand.get("tenantId"));
      authConfig.put("username", operand.get("username"));
      authConfig.put("password", operand.get("password"));
      authConfig.put("certificatePath", operand.get("certificatePath"));
      authConfig.put("certificatePassword", operand.get("certificatePassword"));
      authConfig.put("thumbprint", operand.get("thumbprint"));

      SharePointAuth authenticator = SharePointAuthFactory.createAuth(authConfig);
      MicrosoftGraphListClient client = new MicrosoftGraphListClient(siteUrl, authenticator);

      SharePointListMetadata metadata;
      if (listId != null) {
        metadata = client.getListMetadataById(listId);
      } else {
        // For now, we need to get all lists and find by name
        // This could be optimized with a direct API call
        Map<String, SharePointListMetadata> lists = client.getAvailableLists();
        metadata = lists.get(SharePointNameConverter.toSqlName(listName));
        if (metadata == null) {
          throw new RuntimeException("List not found: " + listName);
        }
      }

      return new SharePointListTable(metadata, client);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SharePoint table", e);
    }
  }
}
