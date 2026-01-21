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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory for creating SharePoint list schemas.
 */
public class SharePointListSchemaFactory implements SchemaFactory {

  public static final SharePointListSchemaFactory INSTANCE = new SharePointListSchemaFactory();

  private SharePointListSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    String siteUrl = (String) operand.get("siteUrl");

    if (siteUrl == null) {
      throw new RuntimeException("siteUrl is required");
    }

    // Pass the entire operand map to support all auth patterns
    // This includes Phase 1-3 auth options and API selection flags
    Map<String, Object> config = new java.util.HashMap<>(operand);

    // Ensure siteUrl is in the config
    config.put("siteUrl", siteUrl);

    // Create the main SharePoint schema with full config
    SharePointListSchema sharePointSchema = new SharePointListSchema(siteUrl, config);

    // Add the metadata schemas as top-level schemas (not sub-schemas)
    SharePointMetadataSchema metadataSchema = new SharePointMetadataSchema(sharePointSchema, null, name);
    parentSchema.add("pg_catalog", metadataSchema);
    parentSchema.add("information_schema", metadataSchema);

    return sharePointSchema;
  }
}
