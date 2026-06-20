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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Schema for Cloud Governance adapter.
 *
 * <p>Provides unified access to cloud resource data across multiple providers
 * (Azure, GCP, AWS) without subjective assessments. Returns raw facts about
 * resource configurations, security settings, and compliance status.
 */
public class CloudOpsSchema extends AbstractSchema {
  private final CloudOpsConfig config;

  public CloudOpsSchema(CloudOpsConfig config) {
    this.config = config;
  }

  @Override public boolean isMutable() {
    return false; // Read-only schema
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // Core resource tables
    builder.put("compute_resources", new ComputeResourcesTable(config));
    builder.put("storage_resources", new StorageResourcesTable(config));
    builder.put("kubernetes_clusters", new KubernetesClustersTable(config));
    builder.put("container_registries", new ContainerRegistriesTable(config));
    builder.put("network_resources", new NetworkResourcesTable(config));
    builder.put("iam_resources", new IAMResourcesTable(config));
    builder.put("database_resources", new DatabaseResourcesTable(config));

    // Junction table: normalizes compute -> security-group associations (FK to compute_resources
    // and network_resources).
    builder.put("compute_security_groups", new ComputeSecurityGroupsTable(config));

    return builder.build();
  }

  /**
   * Provides access to the table map for metadata discovery.
   * Used by the metadata schema to generate information_schema and pg_catalog tables.
   */
  public Map<String, Table> getTableMapForMetadata() {
    return getTableMap();
  }
}
