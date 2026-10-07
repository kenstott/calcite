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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.ops.provider.AWSProvider;
import org.apache.calcite.adapter.ops.provider.AzureProvider;
import org.apache.calcite.adapter.ops.provider.GCPProvider;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsQueryOptimizer;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing Kubernetes cluster information across cloud providers.
 * Returns raw facts without subjective assessments.
 * Supports query optimization through projection, filtering, sorting, and pagination pushdown.
 */
public class KubernetesClustersTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClustersTable.class);

  public KubernetesClustersTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public Enumerable<Object[]> scan(DataContext root,
                                  List<RexNode> filters,
                                  int @Nullable [] projects,
                                  @Nullable RelCollation collation,
                                  @Nullable RexNode offset,
                                  @Nullable RexNode fetch) {
    // Create query optimizer to analyze hints
    CloudOpsQueryOptimizer optimizer =
        new CloudOpsQueryOptimizer(filters, projects, collation, offset, fetch);

    // Log detailed optimization analysis for Kubernetes clusters
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("KubernetesClustersTable query optimization analysis:");

      // Check for specific field filters
      List<CloudOpsQueryOptimizer.FilterInfo> providerFilters =
          optimizer.extractFiltersForField("cloud_provider", 0);
      if (!providerFilters.isEmpty()) {
        LOGGER.debug("  Provider filters: {}", providerFilters);
      }

      List<CloudOpsQueryOptimizer.FilterInfo> regionFilters =
          optimizer.extractFiltersForField("region", 4);
      if (!regionFilters.isEmpty()) {
        LOGGER.debug("  Region filters: {}", regionFilters);
      }

      // Check for sort optimization
      CloudOpsQueryOptimizer.SortInfo sortInfo = optimizer.getSortInfo();
      if (sortInfo != null) {
        LOGGER.debug("  Sort fields: {} field(s)", sortInfo.sortFields.size());
      }

      // Check for pagination
      CloudOpsQueryOptimizer.PaginationInfo paginationInfo = optimizer.getPaginationInfo();
      if (paginationInfo != null) {
        LOGGER.debug("  Pagination - Offset: {}, Limit: {}",
                    paginationInfo.offset, paginationInfo.fetch);
      }

      // Check for projection
      CloudOpsQueryOptimizer.ProjectionInfo projectionInfo = optimizer.getProjectionInfo();
      if (projectionInfo != null) {
        LOGGER.debug("  Projected columns: {} out of {} total columns",
                    projectionInfo.columns.length, getRowType(root.getTypeFactory()).getFieldCount());
      }
    }

    // Call parent implementation with all optimization hints
    return super.scan(root, filters, projects, collation, offset, fetch);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("cluster_name", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("kubernetes_version", SqlTypeName.VARCHAR).nullable(true)
        .add("node_count", SqlTypeName.INTEGER).nullable(true)
        .add("node_pools", SqlTypeName.INTEGER).nullable(true)

        // Security facts (raw boolean/string values)
        .add("rbac_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("private_cluster", SqlTypeName.BOOLEAN).nullable(true)
        .add("public_endpoint", SqlTypeName.BOOLEAN).nullable(true)
        .add("authorized_ip_ranges", SqlTypeName.INTEGER).nullable(true)
        .add("network_policy_provider", SqlTypeName.VARCHAR).nullable(true)

        // Encryption facts
        .add("encryption_at_rest_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("encryption_key_type", SqlTypeName.VARCHAR).nullable(true)

        // Monitoring facts
        .add("logging_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("monitoring_enabled", SqlTypeName.BOOLEAN).nullable(true)

        // Metadata
        .add("created_date", SqlTypeName.TIMESTAMP).nullable(true)
        .add("modified_date", SqlTypeName.TIMESTAMP).nullable(true)
        .add("tags", SqlTypeName.VARCHAR).nullable(true) // JSON string

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                                CloudOpsPaginationHandler paginationHandler,
                                                CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      // Use Azure provider with projection, sort, pagination, and filter support
      AzureProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> aksResults =
          azureProvider.queryKubernetesClusters(subscriptionIds, projectionHandler, sortHandler, paginationHandler, filterHandler);

      // Convert to rows
      for (Map<String, Object> cluster : aksResults) {
        results.add(new Object[]{
            "azure",
            cluster.get("SubscriptionId"),
            cluster.get("ClusterName"),
            cluster.get("Application"),
            cluster.get("Location"),
            cluster.get("ResourceGroup"),
            cluster.get("ResourceId"),
            cluster.get("ClusterVersion"),
            cluster.get("NodeCount"),
            cluster.get("NodePoolCount"),
            cluster.get("RBACEnabled"),
            cluster.get("PrivateCluster"),
            !((Boolean) cluster.getOrDefault("PrivateCluster", false)), // Inverse for public
            cluster.get("AuthorizedIPRanges"),
            cluster.get("NetworkPolicy"),
            cluster.get("DiskEncryption") != null && !cluster.get("DiskEncryption").equals("Platform Managed Key"),
            cluster.get("DiskEncryption"),
            true, // AKS has logging by default
            true, // AKS has monitoring by default
            null, // created_date - not in current query
            null, // modified_date - not in current query
            cluster.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying Azure AKS clusters failed: " + e.getMessage(), e);
    }

    return results;
  }

  @Override protected List<Object[]> queryGCP(List<String> projectIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                              CloudOpsPaginationHandler paginationHandler,
                                              CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      // Use GCP provider with projection, sort, pagination, and filter support
      GCPProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> clusterResults =
          gcpProvider.queryKubernetesClusters(projectIds, projectionHandler, sortHandler, paginationHandler, filterHandler);

      for (Map<String, Object> cluster : clusterResults) {
        results.add(new Object[]{
            "gcp",
            cluster.get("AccountId"),
            cluster.get("ClusterName"),
            cluster.get("Application"),
            cluster.get("Location"),
            null, // resource_group - GCP doesn't have this concept
            cluster.get("ResourceId"),
            cluster.get("KubernetesVersion"),
            cluster.get("NodeCount"),
            cluster.get("NodePools"),
            cluster.get("RBACEnabled"),
            cluster.get("PrivateCluster"),
            cluster.get("PublicEndpoint"),
            cluster.get("AuthorizedIPRanges"),
            cluster.get("NetworkPolicyProvider"),
            cluster.get("EncryptionAtRestEnabled"),
            cluster.get("EncryptionKeyType"),
            cluster.get("LoggingEnabled"),
            cluster.get("MonitoringEnabled"),
            CloudOpsDataConverter.convertValue(cluster.get("CreatedDate"), SqlTypeName.TIMESTAMP),
            null, // modified_date - GKE does not report one
            cluster.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying GCP Kubernetes clusters failed: " + e.getMessage(), e);
    }

    return results;
  }

  @Override protected List<Object[]> queryAWS(List<String> accountIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                              CloudOpsPaginationHandler paginationHandler,
                                              CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      // Use AWS provider with projection, sort, pagination, and filter support
      AWSProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> clusterResults =
          awsProvider.queryKubernetesClusters(accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler);

      for (Map<String, Object> cluster : clusterResults) {
        Integer publicAccessCidrs = (Integer) cluster.get("PublicAccessCidrs");

        results.add(new Object[]{
            "aws",
            cluster.get("AccountId"),
            cluster.get("ClusterName"),
            cluster.get("Application"),
            cluster.get("Region"),
            null, // resource_group - AWS doesn't have this concept for EKS
            cluster.get("ResourceId"),
            cluster.get("ClusterVersion"),
            cluster.get("NodeCount"),
            cluster.get("NodeGroupCount"),
            cluster.get("RBACEnabled"),
            !((Boolean) cluster.getOrDefault("EndpointPublicAccess", true)),
            cluster.get("EndpointPublicAccess"),
            publicAccessCidrs,
            null, // network_policy - EKS doesn't have built-in network policy
            cluster.get("EncryptionEnabled"),
            cluster.get("EncryptionProvider"),
            cluster.get("LoggingEnabled"),
            true, // EKS has CloudWatch monitoring by default
            CloudOpsDataConverter.convertValue(cluster.get("CreatedAt"), SqlTypeName.TIMESTAMP),
            null, // modified_date - not available
            cluster.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying AWS Kubernetes clusters failed: " + e.getMessage(), e);
    }

    return results;
  }
}
