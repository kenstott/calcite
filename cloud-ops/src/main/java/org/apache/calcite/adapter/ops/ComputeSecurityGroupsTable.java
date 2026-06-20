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
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Junction table normalizing the many-to-many relationship between compute instances and the
 * security groups attached to them — the relational form of {@code compute_resources.security_groups}
 * (a JSON array, which cannot itself be a scalar foreign key).
 *
 * <p>Provider-neutral by design: each cloud maps its concept into the same two columns — AWS Security
 * Groups, Azure Network Security Groups, GCP firewall rules / tags. Only AWS is populated today; the
 * Azure and GCP scans return empty until those providers extract the associations.
 */
public class ComputeSecurityGroupsTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComputeSecurityGroupsTable.class);

  public ComputeSecurityGroupsTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("instance_id", SqlTypeName.VARCHAR)
        .add("compute_resource_id", SqlTypeName.VARCHAR)
        .add("security_group_id", SqlTypeName.VARCHAR)
        .build();
  }

  /**
   * Logical unique key: a compute instance is paired with each security group at most once.
   */
  @Override protected List<ImmutableBitSet> additionalKeys(List<String> columnNames) {
    final int computeResourceId = columnNames.indexOf("compute_resource_id");
    final int securityGroupId = columnNames.indexOf("security_group_id");
    if (computeResourceId >= 0 && securityGroupId >= 0) {
      return Collections.singletonList(ImmutableBitSet.of(computeResourceId, securityGroupId));
    }
    return Collections.emptyList();
  }

  /**
   * Two foreign keys: {@code compute_resource_id} -> compute_resources.resource_id (the instance
   * ARN, a globally-unique primary key), and {@code (cloud_provider, security_group_id)} ->
   * network_resources(cloud_provider, network_resource) (the bare security-group native ID).
   */
  @Override protected List<RelReferentialConstraint> referentialConstraints(List<String> columnNames) {
    final List<String> computeColumns =
        new ComputeResourcesTable(config).getRowType(METADATA_TYPE_FACTORY).getFieldNames();
    final List<String> networkColumns =
        new NetworkResourcesTable(config).getRowType(METADATA_TYPE_FACTORY).getFieldNames();

    final List<RelReferentialConstraint> fks = new ArrayList<>();

    final int srcComputeId = columnNames.indexOf("compute_resource_id");
    final int tgtResourceId = computeColumns.indexOf("resource_id");
    if (srcComputeId >= 0 && tgtResourceId >= 0) {
      fks.add(RelReferentialConstraintImpl.of(
          Arrays.asList("cloud", "compute_security_groups"),
          Arrays.asList("cloud", "compute_resources"),
          Collections.singletonList(IntPair.of(srcComputeId, tgtResourceId))));
    }

    final int srcProvider = columnNames.indexOf("cloud_provider");
    final int srcSecurityGroupId = columnNames.indexOf("security_group_id");
    final int tgtProvider = networkColumns.indexOf("cloud_provider");
    final int tgtNativeId = networkColumns.indexOf("native_id");
    if (srcProvider >= 0 && srcSecurityGroupId >= 0 && tgtProvider >= 0 && tgtNativeId >= 0) {
      fks.add(RelReferentialConstraintImpl.of(
          Arrays.asList("cloud", "compute_security_groups"),
          Arrays.asList("cloud", "network_resources"),
          Arrays.asList(
              IntPair.of(srcProvider, tgtProvider),
              IntPair.of(srcSecurityGroupId, tgtNativeId))));
    }

    return fks;
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                                CloudOpsPaginationHandler paginationHandler,
                                                CloudOpsFilterHandler filterHandler) {
    // Phase 2: map Azure NIC -> Network Security Group associations here.
    return new ArrayList<>();
  }

  @Override protected List<Object[]> queryGCP(List<String> projectIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                              CloudOpsPaginationHandler paginationHandler,
                                              CloudOpsFilterHandler filterHandler) {
    // Phase 3: map GCP instance -> firewall rule / network tag associations here.
    return new ArrayList<>();
  }

  @Override protected List<Object[]> queryAWS(List<String> accountIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                              CloudOpsPaginationHandler paginationHandler,
                                              CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();
    try {
      AWSProvider awsProvider = new AWSProvider(config.aws);
      for (Map<String, Object> row : awsProvider.queryComputeSecurityGroups(accountIds)) {
        results.add(new Object[]{
            "aws",
            row.get("AccountId"),
            row.get("InstanceId"),
            row.get("ComputeResourceId"),
            row.get("SecurityGroupId")
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS compute security-group associations: {}", e.getMessage());
    }
    return results;
  }
}
