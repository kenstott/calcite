/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops;

import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the logical key / foreign-key metadata exposed by {@code AbstractCloudOpsTable}
 * via {@link org.apache.calcite.schema.Table#getStatistic()}. Dependency-free: no cloud calls — the
 * metadata is derived purely from the static row types.
 */
@Tag("unit")
public class CloudOpsConstraintMetadataTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  /** Minimal config; the metadata never touches provider credentials. */
  private static CloudOpsConfig config() {
    return new CloudOpsConfig(null, null, null, null, null, null, null);
  }

  private static List<String> columns(AbstractCloudOpsTable table) {
    return table.getRowType(TYPE_FACTORY).getFieldNames();
  }

  @Test void testResourceIdIsLogicalPrimaryKeyOnEveryTable() {
    CloudOpsConfig config = config();
    List<AbstractCloudOpsTable> tables = Arrays.asList(
        new ComputeResourcesTable(config),
        new StorageResourcesTable(config),
        new KubernetesClustersTable(config),
        new ContainerRegistriesTable(config),
        new NetworkResourcesTable(config),
        new IAMResourcesTable(config),
        new DatabaseResourcesTable(config));

    for (AbstractCloudOpsTable table : tables) {
      List<String> cols = columns(table);
      Statistic stat = table.getStatistic();
      ImmutableBitSet pk = ImmutableBitSet.of(cols.indexOf("resource_id"));
      assertNotNull(stat.getKeys(), table.getClass().getSimpleName() + " keys");
      assertTrue(stat.getKeys().contains(pk),
          table.getClass().getSimpleName() + " should declare resource_id as a key");
      assertTrue(stat.isKey(pk),
          table.getClass().getSimpleName() + " isKey(resource_id)");
    }
  }

  @Test void testNetworkResourcesHasCompositeNativeIdKey() {
    NetworkResourcesTable table = new NetworkResourcesTable(config());
    List<String> cols = columns(table);
    Statistic stat = table.getStatistic();
    ImmutableBitSet nativeKey =
        ImmutableBitSet.of(cols.indexOf("cloud_provider"), cols.indexOf("native_id"));
    assertTrue(stat.getKeys().contains(nativeKey),
        "network_resources should declare (cloud_provider, native_id) as a unique key");
    assertTrue(stat.isKey(nativeKey));
  }

  @Test void testComputeForeignKeys() {
    CloudOpsConfig config = config();
    ComputeResourcesTable compute = new ComputeResourcesTable(config);
    List<String> srcCols = columns(compute);
    List<String> netCols = columns(new NetworkResourcesTable(config));
    List<String> iamCols = columns(new IAMResourcesTable(config));

    List<RelReferentialConstraint> fks = compute.getStatistic().getReferentialConstraints();
    assertNotNull(fks);
    assertEquals(3, fks.size(), "vpc_id, subnet_id -> network_resources; iam_role -> iam_resources");

    // vpc_id -> network_resources(cloud_provider, network_resource)
    assertTrue(fks.stream().anyMatch(fk ->
        fk.getTargetQualifiedName().equals(Arrays.asList("cloud", "network_resources"))
            && fk.getColumnPairs().equals(Arrays.asList(
                IntPair.of(srcCols.indexOf("cloud_provider"), netCols.indexOf("cloud_provider")),
                IntPair.of(srcCols.indexOf("vpc_id"), netCols.indexOf("native_id"))))),
        "vpc_id FK");

    // subnet_id -> network_resources(cloud_provider, network_resource)
    assertTrue(fks.stream().anyMatch(fk ->
        fk.getTargetQualifiedName().equals(Arrays.asList("cloud", "network_resources"))
            && fk.getColumnPairs().equals(Arrays.asList(
                IntPair.of(srcCols.indexOf("cloud_provider"), netCols.indexOf("cloud_provider")),
                IntPair.of(srcCols.indexOf("subnet_id"), netCols.indexOf("native_id"))))),
        "subnet_id FK");

    // iam_role -> iam_resources(resource_id)
    assertTrue(fks.stream().anyMatch(fk ->
        fk.getTargetQualifiedName().equals(Arrays.asList("cloud", "iam_resources"))
            && fk.getColumnPairs().equals(Collections.singletonList(
                IntPair.of(srcCols.indexOf("iam_role"), iamCols.indexOf("resource_id"))))),
        "iam_role FK");
  }

  @Test void testComputeSecurityGroupsJunction() {
    CloudOpsConfig config = config();
    ComputeSecurityGroupsTable junction = new ComputeSecurityGroupsTable(config);
    List<String> cols = columns(junction);
    List<String> computeCols = columns(new ComputeResourcesTable(config));
    List<String> netCols = columns(new NetworkResourcesTable(config));
    Statistic stat = junction.getStatistic();

    // No resource_id column -> no auto primary key; the unique key is the (compute, sg) pair.
    ImmutableBitSet pairKey =
        ImmutableBitSet.of(cols.indexOf("compute_resource_id"), cols.indexOf("security_group_id"));
    assertTrue(stat.getKeys().contains(pairKey), "junction unique key");

    List<RelReferentialConstraint> fks = stat.getReferentialConstraints();
    assertEquals(2, fks.size(), "FK to compute_resources and to network_resources");

    // compute_resource_id -> compute_resources(resource_id)
    assertTrue(fks.stream().anyMatch(fk ->
        fk.getTargetQualifiedName().equals(Arrays.asList("cloud", "compute_resources"))
            && fk.getColumnPairs().equals(Collections.singletonList(
                IntPair.of(cols.indexOf("compute_resource_id"), computeCols.indexOf("resource_id"))))),
        "compute_resource_id FK");

    // (cloud_provider, security_group_id) -> network_resources(cloud_provider, network_resource)
    assertTrue(fks.stream().anyMatch(fk ->
        fk.getTargetQualifiedName().equals(Arrays.asList("cloud", "network_resources"))
            && fk.getColumnPairs().equals(Arrays.asList(
                IntPair.of(cols.indexOf("cloud_provider"), netCols.indexOf("cloud_provider")),
                IntPair.of(cols.indexOf("security_group_id"), netCols.indexOf("native_id"))))),
        "security_group_id FK");
  }

  @Test void testTablesWithoutForeignKeysDeclareNone() {
    Statistic stat = new StorageResourcesTable(config()).getStatistic();
    // Default hook returns an empty list (not null) for tables with no FKs.
    assertNotNull(stat.getReferentialConstraints());
    assertTrue(stat.getReferentialConstraints().isEmpty());
  }
}
