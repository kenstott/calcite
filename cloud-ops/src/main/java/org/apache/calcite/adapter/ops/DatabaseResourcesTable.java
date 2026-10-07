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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing database resource information across cloud providers.
 */
public class DatabaseResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseResourcesTable.class);
  public DatabaseResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("database_resource", SqlTypeName.VARCHAR).nullable(true)
        .add("database_type", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("engine", SqlTypeName.VARCHAR).nullable(true)
        .add("engine_version", SqlTypeName.VARCHAR).nullable(true)
        .add("instance_class", SqlTypeName.VARCHAR).nullable(true)
        .add("allocated_storage", SqlTypeName.INTEGER).nullable(true)
        .add("multi_az", SqlTypeName.BOOLEAN).nullable(true)
        .add("status", SqlTypeName.VARCHAR).nullable(true)

        // Security facts
        .add("publicly_accessible", SqlTypeName.BOOLEAN).nullable(true)
        .add("encrypted", SqlTypeName.BOOLEAN).nullable(true)
        .add("encryption_key", SqlTypeName.VARCHAR).nullable(true)
        .add("tls_version", SqlTypeName.VARCHAR).nullable(true)

        // Backup facts
        .add("backup_retention_days", SqlTypeName.INTEGER).nullable(true)
        .add("backup_window", SqlTypeName.VARCHAR).nullable(true)

        // Timestamps
        .add("create_time", SqlTypeName.TIMESTAMP).nullable(true)

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
      List<Map<String, Object>> dbResults = azureProvider.queryDatabaseResources(subscriptionIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "azure",
            db.get("SubscriptionId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Location"),
            db.get("ResourceGroup"),
            db.get("ResourceId"),
            null, // engine parsed from database type
            null, // engine version not in query
            db.get("SKU"),
            null, // allocated storage not in query
            null, // multi-AZ concept different in Azure
            null, // status not in query
            null, // publicly accessible would need additional query
            null, // encrypted status would need parsing
            null, // encryption key not in query
            parseMinTlsVersion(db.get("SecurityConfiguration")),
            null, // backup retention would need parsing
            db.get("BackupConfiguration"),
            null  // create time not in query
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying Azure database resources failed: " + e.getMessage(), e);
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
      List<Map<String, Object>> dbResults = gcpProvider.queryDatabaseResources(projectIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "gcp",
            db.get("ProjectId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Location"),
            null, // resource group not applicable
            db.get("ResourceId"),
            db.get("Engine"),
            db.get("EngineVersion"),
            db.get("Tier"),
            db.get("AllocatedStorageGb"), // GB, as for AWS
            db.get("MultiZone"),
            db.get("State"),
            db.get("PubliclyAccessible"),
            db.get("Encrypted"),
            db.get("KmsKeyName"),
            Boolean.TRUE.equals(db.get("RequireSsl")) ? "required" : null,
            db.get("RetainedBackups"), // a count of backups, the unit Cloud SQL retains by
            db.get("BackupStartTime"),
            CloudOpsDataConverter.convertValue(db.get("CreateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying GCP database resources failed: " + e.getMessage(), e);
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
      List<Map<String, Object>> dbResults = awsProvider.queryDatabaseResources(accountIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "aws",
            db.get("AccountId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Region"),
            null, // resource group not applicable
            db.get("ResourceId"),
            db.get("Engine"),
            db.get("EngineVersion"),
            db.get("DBInstanceClass") != null ? db.get("DBInstanceClass") :
                db.get("CacheNodeType"),
            db.get("AllocatedStorage"),
            db.get("MultiAZ"),
            db.get("DBInstanceStatus") != null ? db.get("DBInstanceStatus") :
                db.get("Status"),
            db.get("PubliclyAccessible"),
            db.get("StorageEncrypted") != null ? db.get("StorageEncrypted") :
                db.get("AtRestEncryptionEnabled"),
            db.get("KmsKeyId") != null ? db.get("KmsKeyId") :
                db.get("KMSMasterKeyArn"),
            null, // TLS version not directly exposed
            db.get("BackupRetentionPeriod") != null ?
                ((Number) db.get("BackupRetentionPeriod")).intValue() :
                db.get("SnapshotRetentionLimit") != null ?
                    ((Number) db.get("SnapshotRetentionLimit")).intValue() : null,
            db.get("PreferredBackupWindow") != null ? db.get("PreferredBackupWindow") :
                db.get("SnapshotWindow"),
            CloudOpsDataConverter.convertValue(
                db.get("InstanceCreateTime") != null ? db.get("InstanceCreateTime") :
                db.get("ClusterCreateTime") != null ? db.get("ClusterCreateTime") :
                db.get("CreationDateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying AWS database resources failed: " + e.getMessage(), e);
    }

    return results;
  }

  private String parseMinTlsVersion(Object securityConfig) {
    if (securityConfig instanceof String) {
      String config = (String) securityConfig;
      if (config.contains("Min TLS: ")) {
        int start = config.indexOf("Min TLS: ") + 9;
        int end = config.indexOf(",", start);
        if (end == -1) end = config.length();
        return config.substring(start, end).trim();
      }
    }
    return null;
  }
}
