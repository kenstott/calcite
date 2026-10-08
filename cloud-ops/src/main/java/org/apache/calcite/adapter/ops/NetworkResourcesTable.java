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

import org.apache.calcite.adapter.ops.provider.AWSProvider;
import org.apache.calcite.adapter.ops.provider.AzureProvider;
import org.apache.calcite.adapter.ops.provider.CloudProvider;
import org.apache.calcite.adapter.ops.provider.GCPProvider;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Table containing network resource information across cloud providers.
 */
public class NetworkResourcesTable extends AbstractCloudOpsTable {
  public NetworkResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  /**
   * The provider-native identifier is unique per provider, so {@code (cloud_provider, native_id)} is
   * a logical unique key. It is the consistent cross-cloud target of the compute foreign keys (which
   * store the same native identifier — AWS bare id, Azure ARM id, GCP self-link/id).
   */
  @Override protected List<ImmutableBitSet> additionalKeys(List<String> columnNames) {
    final int cloudProvider = columnNames.indexOf("cloud_provider");
    final int nativeId = columnNames.indexOf("native_id");
    if (cloudProvider >= 0 && nativeId >= 0) {
      return Collections.singletonList(ImmutableBitSet.of(cloudProvider, nativeId));
    }
    return Collections.emptyList();
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("network_resource", SqlTypeName.VARCHAR).nullable(true)
        .add("network_resource_type", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)
        // Provider-native stable identifier (AWS bare id, Azure ARM id, GCP self-link/id). The
        // consistent cross-cloud join key referenced by compute_resources / compute_security_groups.
        .add("native_id", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("configuration", SqlTypeName.VARCHAR).nullable(true)
        .add("cidr_block", SqlTypeName.VARCHAR).nullable(true)
        .add("state", SqlTypeName.VARCHAR).nullable(true)
        .add("is_default", SqlTypeName.BOOLEAN).nullable(true)

        // Security facts
        .add("security_findings", SqlTypeName.VARCHAR).nullable(true)
        .add("has_open_ingress", SqlTypeName.BOOLEAN).nullable(true)
        .add("rule_count", SqlTypeName.INTEGER).nullable(true)

        // Metadata
        .add("tags", SqlTypeName.VARCHAR).nullable(true) // JSON

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider azureProvider = new AzureProvider(config.azure, config.cacheManager());
      List<Map<String, Object>> networkResults = azureProvider.queryNetworkResources(subscriptionIds);

      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "azure",
            network.get("SubscriptionId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Location"),
            network.get("ResourceGroup"),
            network.get("ResourceId"),
            network.get("NativeId"),
            network.get("Configuration"),
            network.get("CidrBlock"), // address prefix; the address itself for a public IP
            network.get("State"),
            null, // is_default: Azure has no default network
            network.get("SecurityFindings"),
            network.get("HasOpenIngress"), // network security groups only
            network.get("RuleCount"), // network security groups only
            network.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying Azure network resources failed: " + e.getMessage(), e);
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
      CloudProvider gcpProvider = new GCPProvider(config.gcp, config.cacheManager());
      List<Map<String, Object>> networkResults = gcpProvider.queryNetworkResources(projectIds);

      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "gcp",
            network.get("ProjectId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Location"),
            null, // resource group not applicable
            network.get("ResourceId"),
            network.get("NativeId"),
            network.get("Configuration"),
            network.get("SourceRanges"), // subnet range, or a firewall rule's ranges
            network.get("State"), // firewall rules only: enabled / disabled
            network.get("IsDefault"), // networks only
            null, // security findings not computed
            network.get("HasOpenIngress"), // firewall rules only
            network.get("RuleCount"), // firewall rules only
            null  // networks, subnets and firewall rules carry no labels
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying GCP network resources failed: " + e.getMessage(), e);
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
      CloudProvider awsProvider = new AWSProvider(config.aws, config.cacheManager());
      List<Map<String, Object>> networkResults = awsProvider.queryNetworkResources(accountIds);

      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "aws",
            network.get("AccountId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Region"),
            null, // resource group not applicable
            network.get("ResourceId"),
            network.get("NativeId"),
            network.get("GroupName") != null ?
                "Name: " + network.get("GroupName") + ", Description: " + network.get("Description") :
                network.get("Configuration"),
            network.get("CidrBlock"),
            network.get("State"),
            network.get("IsDefault"),
            null, // security findings not computed
            network.get("HasOpenIngressRule"),
            network.get("IngressRulesCount") != null ? network.get("IngressRulesCount") :
                network.get("EgressRulesCount"),
            network.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying AWS network resources failed: " + e.getMessage(), e);
    }

    return results;
  }
}
