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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Table containing network resource information across cloud providers.
 */
public class NetworkResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkResourcesTable.class);
  public NetworkResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  /**
   * The native resource identifier is unique per provider, so {@code (cloud_provider,
   * network_resource)} is a logical unique key. It is the target of the
   * {@code compute_resources.vpc_id} foreign key (compute stores the bare {@code vpc-...} ID, which
   * matches {@code network_resource}, not the ARN in {@code resource_id}).
   */
  @Override protected List<ImmutableBitSet> additionalKeys(List<String> columnNames) {
    final int cloudProvider = columnNames.indexOf("cloud_provider");
    final int networkResource = columnNames.indexOf("network_resource");
    if (cloudProvider >= 0 && networkResource >= 0) {
      return Collections.singletonList(ImmutableBitSet.of(cloudProvider, networkResource));
    }
    return Collections.emptyList();
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("network_resource", SqlTypeName.VARCHAR)
        .add("network_resource_type", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("configuration", SqlTypeName.VARCHAR)
        .add("cidr_block", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .add("is_default", SqlTypeName.BOOLEAN)

        // Security facts
        .add("security_findings", SqlTypeName.VARCHAR)
        .add("has_open_ingress", SqlTypeName.BOOLEAN)
        .add("rule_count", SqlTypeName.INTEGER)

        // Metadata
        .add("tags", SqlTypeName.VARCHAR) // JSON

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
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
            network.get("Configuration"),
            null, // CIDR block would need parsing from configuration
            null, // state not in query
            null, // is_default not in query
            network.get("SecurityFindings"),
            null, // has_open_ingress would need rule analysis
            null, // rule_count would need parsing
            null  // tags not in query
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure network resources: {}", e.getMessage());
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
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
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
            network.get("Configuration"),
            network.get("SourceRanges"), // for firewall rules
            null, // state not applicable
            null, // is_default would need additional info
            null, // security findings not computed
            null, // has_open_ingress would need rule analysis
            null, // rule_count not computed
            null  // tags would need conversion
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP network resources: {}", e.getMessage());
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
      CloudProvider awsProvider = new AWSProvider(config.aws);
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
            null  // tags would need conversion
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS network resources: {}", e.getMessage());
    }

    return results;
  }
}
