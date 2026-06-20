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
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.mapping.IntPair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Table containing compute resource (VM) information across cloud providers.
 */
public class ComputeResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComputeResourcesTable.class);

  public ComputeResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  /**
   * Logical foreign keys, declared provider-neutrally on the shared tables:
   * <ul>
   *   <li>{@code (cloud_provider, vpc_id) -> network_resources(cloud_provider, network_resource)}
   *   <li>{@code (cloud_provider, subnet_id) -> network_resources(cloud_provider, network_resource)}
   *   <li>{@code iam_role -> iam_resources(resource_id)} (the attached identity's globally-unique ID)
   * </ul>
   * The {@code vpc_id}/{@code subnet_id} columns store the bare native ID (e.g. {@code vpc-...},
   * {@code subnet-...}), which equi-matches {@code network_resource} (not the ARN in
   * {@code resource_id}); {@code cloud_provider} scopes the match per provider. {@code iam_role}
   * holds the attached-identity ARN, matching the {@code resource_id} primary key. These columns are
   * null for providers that have not extracted them, so the constraints only bite on populated rows.
   */
  @Override protected List<RelReferentialConstraint> referentialConstraints(List<String> columnNames) {
    final List<String> networkColumns =
        new NetworkResourcesTable(config).getRowType(METADATA_TYPE_FACTORY).getFieldNames();
    final List<String> iamColumns =
        new IAMResourcesTable(config).getRowType(METADATA_TYPE_FACTORY).getFieldNames();

    final int srcProvider = columnNames.indexOf("cloud_provider");
    final int tgtProvider = networkColumns.indexOf("cloud_provider");
    final int tgtNativeId = networkColumns.indexOf("native_id");

    final List<RelReferentialConstraint> fks = new ArrayList<>();

    addNetworkFk(fks, columnNames.indexOf("vpc_id"), srcProvider, tgtProvider, tgtNativeId);
    addNetworkFk(fks, columnNames.indexOf("subnet_id"), srcProvider, tgtProvider, tgtNativeId);

    final int srcIamRole = columnNames.indexOf("iam_role");
    final int tgtIamResourceId = iamColumns.indexOf("resource_id");
    if (srcIamRole >= 0 && tgtIamResourceId >= 0) {
      fks.add(RelReferentialConstraintImpl.of(
          Arrays.asList("cloud", "compute_resources"),
          Arrays.asList("cloud", "iam_resources"),
          Collections.singletonList(IntPair.of(srcIamRole, tgtIamResourceId))));
    }

    return fks;
  }

  private void addNetworkFk(List<RelReferentialConstraint> fks, int srcNativeId,
      int srcProvider, int tgtProvider, int tgtNativeId) {
    if (srcNativeId < 0 || srcProvider < 0 || tgtProvider < 0 || tgtNativeId < 0) {
      return;
    }
    fks.add(RelReferentialConstraintImpl.of(
        Arrays.asList("cloud", "compute_resources"),
        Arrays.asList("cloud", "network_resources"),
        Arrays.asList(
            IntPair.of(srcProvider, tgtProvider),
            IntPair.of(srcNativeId, tgtNativeId))));
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("instance_id", SqlTypeName.VARCHAR)
        .add("instance_name", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("availability_zone", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("instance_type", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .add("platform", SqlTypeName.VARCHAR)
        .add("architecture", SqlTypeName.VARCHAR)
        .add("virtualization_type", SqlTypeName.VARCHAR)

        // Network facts
        .add("public_ip", SqlTypeName.VARCHAR)
        .add("private_ip", SqlTypeName.VARCHAR)
        .add("vpc_id", SqlTypeName.VARCHAR)
        .add("subnet_id", SqlTypeName.VARCHAR)

        // Security facts
        .add("iam_role", SqlTypeName.VARCHAR)
        .add("security_groups", SqlTypeName.VARCHAR) // JSON array
        .add("disk_encryption_enabled", SqlTypeName.BOOLEAN)
        .add("monitoring_enabled", SqlTypeName.BOOLEAN)

        // Timestamps
        .add("launch_time", SqlTypeName.TIMESTAMP)

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
      List<Map<String, Object>> vmResults = azureProvider.queryComputeInstances(subscriptionIds);

      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "azure",
            vm.get("SubscriptionId"),
            vm.get("VMName"),
            vm.get("VMName"), // Azure doesn't have separate instance name
            vm.get("Application"),
            vm.get("Location"),
            vm.get("AvailabilityZone"),
            vm.get("ResourceGroup"),
            vm.get("ResourceId"),
            vm.get("VMSize"),
            vm.get("PowerState"),
            vm.get("OSType"),
            null, // architecture not in query
            null, // virtualization type not in query
            null, // public IP would need additional query
            null, // private IP would need additional query
            vm.get("VNetId"),   // vpc_id: VNet ARM id (matches network_resources.native_id)
            vm.get("SubnetId"), // subnet_id: subnet ARM id
            vm.get("AttachedIdentity"), // iam_role: attached managed-identity ARM id
            null, // security groups would need additional query
            "Enabled".equals(vm.get("DiskEncryption")),
            vm.get("BootDiagnostics"),
            null  // launch time not in query
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure compute instances: {}", e.getMessage());
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
      List<Map<String, Object>> vmResults = gcpProvider.queryComputeInstances(projectIds);

      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "gcp",
            vm.get("ProjectId"),
            vm.get("VMName"),
            vm.get("VMName"), // GCP uses same name
            vm.get("Application"),
            vm.get("Zone"),
            vm.get("Zone"),
            null, // resource group not applicable
            vm.get("ResourceId"),
            vm.get("MachineType"),
            vm.get("Status"),
            null, // platform not directly available
            vm.get("CpuPlatform"),
            null, // virtualization type not exposed
            vm.get("HasExternalIP") != null && (Boolean) vm.get("HasExternalIP") ? "assigned" : null,
            null, // private IP would need additional query
            vm.get("NetworkId"), // vpc_id: VPC network self-link (matches network_resources.native_id)
            vm.get("SubnetId"),  // subnet_id: subnetwork self-link
            null, // iam_role: GCP service-account row emission is a follow-up (left null, FK-safe)
            null, // security groups as JSON
            "Enabled".equals(vm.get("DiskEncryption")),
            false, // monitoring not in basic query
            CloudOpsDataConverter.convertValue(vm.get("CreationTimestamp"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP compute instances: {}", e.getMessage());
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
      List<Map<String, Object>> vmResults = awsProvider.queryComputeInstances(accountIds);

      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "aws",
            vm.get("AccountId"),
            vm.get("InstanceId"),
            vm.get("InstanceName"),
            vm.get("Application"),
            vm.get("Region"),
            vm.get("AvailabilityZone"),
            null, // resource group not applicable
            vm.get("ResourceId"),
            vm.get("InstanceType"),
            vm.get("State"),
            vm.get("Platform"),
            vm.get("Architecture"),
            vm.get("VirtualizationType"),
            vm.get("PublicIpAddress"),
            vm.get("PrivateIpAddress"),
            vm.get("VpcId"),
            vm.get("SubnetId"),
            vm.get("IamInstanceProfile") != null ? vm.get("IamInstanceProfile").toString() : null,
            vm.get("SecurityGroups") != null ? vm.get("SecurityGroups").toString() : null,
            vm.get("EbsEncrypted"),
            vm.get("MonitoringEnabled"),
            CloudOpsDataConverter.convertValue(vm.get("LaunchTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS compute instances: {}", e.getMessage());
    }

    return results;
  }
}
