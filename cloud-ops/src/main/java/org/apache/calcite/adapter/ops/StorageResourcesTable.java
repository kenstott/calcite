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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing storage resource information across cloud providers.
 * Returns raw facts without subjective assessments.
 */
public class StorageResourcesTable extends AbstractCloudOpsTable {
  public StorageResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_name", SqlTypeName.VARCHAR).nullable(true)
        .add("storage_type", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("size_bytes", SqlTypeName.BIGINT).nullable(true)
        .add("storage_class", SqlTypeName.VARCHAR).nullable(true)
        .add("replication_type", SqlTypeName.VARCHAR).nullable(true)

        // Security facts
        .add("encryption_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("encryption_type", SqlTypeName.VARCHAR).nullable(true)
        .add("encryption_key_type", SqlTypeName.VARCHAR).nullable(true)
        .add("public_access_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("public_access_level", SqlTypeName.VARCHAR).nullable(true)
        .add("network_restrictions", SqlTypeName.VARCHAR).nullable(true)
        .add("https_only", SqlTypeName.BOOLEAN).nullable(true)

        // Data protection facts
        .add("versioning_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("soft_delete_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("soft_delete_retention_days", SqlTypeName.INTEGER).nullable(true)
        .add("backup_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("lifecycle_rules_count", SqlTypeName.INTEGER).nullable(true)

        // Access control facts
        .add("access_tier", SqlTypeName.VARCHAR).nullable(true)
        .add("created_date", SqlTypeName.TIMESTAMP).nullable(true)
        .add("modified_date", SqlTypeName.TIMESTAMP).nullable(true)

        // Metadata
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
      // Use native Azure provider
      CloudProvider azureProvider = new AzureProvider(config.azure, config.cacheManager());
      List<Map<String, Object>> storageResults = azureProvider.queryStorageResources(subscriptionIds);

      // Convert to rows
      for (Map<String, Object> storage : storageResults) {
        String storageType = (String) storage.get("StorageType");
        String encryptionMethod = (String) storage.get("EncryptionMethod");

        results.add(new Object[]{
            "azure",
            storage.get("SubscriptionId"),
            storage.get("StorageResource"),
            storageType,
            storage.get("Application"),
            storage.get("Location"),
            storage.get("ResourceGroup"),
            storage.get("ResourceId"),
            storage.get("SizeBytes"), // provisioned size: disks and SQL databases only
            storage.get("StorageClass"), // the SKU, e.g. Standard_LRS
            storage.get("ReplicationType"),
            storage.get("EncryptionEnabled"),
            encryptionMethod,
            encryptionMethod == null || encryptionMethod.isEmpty() ? null
                : encryptionMethod.contains("Customer") ? "customer-managed" : "service-managed",
            storage.get("PublicBlobAccess"), // storage accounts only
            storage.get("PublicNetworkAccess"),
            storage.get("NetworkDefaultAction"),
            storage.get("HttpsOnly"),
            // blob-service settings: storage accounts only
            storage.get("VersioningEnabled"),
            storage.get("SoftDeleteEnabled"),
            storage.get("SoftDeleteRetentionDays"),
            null, // backup_enabled - Azure Backup is configured on a vault, not here
            storage.get("LifecycleRulesCount"),
            storage.get("AccessTier"),
            CloudOpsDataConverter.convertValue(storage.get("CreatedDate"), SqlTypeName.TIMESTAMP),
            null, // modified_date: not reported
            storage.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying Azure storage resources failed: " + e.getMessage(), e);
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
      List<Map<String, Object>> storageResults = gcpProvider.queryStorageResources(projectIds);

      for (Map<String, Object> storage : storageResults) {
        results.add(new Object[]{
            "gcp",
            storage.get("ProjectId"),
            storage.get("StorageResource"),
            storage.get("StorageType"),
            storage.get("Application"),
            storage.get("Location"),
            null, // resource_group - GCP doesn't have this concept
            storage.get("ResourceId"),
            null, // size_bytes - not in current query
            storage.get("StorageClass"),
            storage.get("LocationType"), // region / dual-region / multi-region
            storage.get("EncryptionEnabled"),
            storage.get("EncryptionEnabled") != null && (Boolean) storage.get("EncryptionEnabled") ?
                (storage.get("EncryptionKeyName") != null ? "customer-managed" : "service-managed") : "none",
            storage.get("EncryptionKeyName") != null ? "customer-managed" : "service-managed",
            // Known only when public access prevention is enforced; otherwise it depends on
            // the bucket's IAM policy, which the adapter's read role may not see
            "enforced".equals(storage.get("PublicAccessPrevention")) ? Boolean.FALSE : null,
            storage.get("PublicAccessPrevention"),
            null, // network_restrictions
            true, // https_only - GCS always uses HTTPS
            storage.get("VersioningEnabled"),
            null, // soft_delete_enabled
            null, // soft_delete_retention_days
            storage.get("RetentionPolicy") != null && (Boolean) storage.get("RetentionPolicy"),
            storage.get("LifecycleRuleCount"),
            null, // access_tier
            CloudOpsDataConverter.convertValue(storage.get("TimeCreated"), SqlTypeName.TIMESTAMP),
            CloudOpsDataConverter.convertValue(storage.get("Updated"), SqlTypeName.TIMESTAMP),
            storage.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying GCP storage resources failed: " + e.getMessage(), e);
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
      // Use the optimized AWS provider with projection support
      AWSProvider awsProvider = new AWSProvider(config.aws, config.cacheManager());
      List<Map<String, Object>> storageResults =
          awsProvider.queryStorageResources(accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler);

      for (Map<String, Object> storage : storageResults) {
        // Handle null values gracefully for fields that may not have been fetched
        Boolean publicAccessBlocked = storage.get("PublicAccessBlocked") != null ?
            (Boolean) storage.get("PublicAccessBlocked") : null;

        results.add(new Object[]{
            "aws",
            storage.get("AccountId"),
            storage.get("StorageResource"),
            storage.get("StorageType"),
            storage.get("Application"),
            storage.get("Location"),
            null, // resource_group - AWS doesn't use this concept for S3
            storage.get("ResourceId"),
            storage.get("SizeBytes"), // from CloudWatch; fetched only when projected
            null, // storage_class - in S3 this is per object
            storage.get("Replication"),
            storage.get("EncryptionEnabled"),
            storage.get("EncryptionType"),
            storage.get("KmsKeyId") != null ? "customer-managed" :
                (storage.get("EncryptionEnabled") != null ? "service-managed" : null),
            publicAccessBlocked != null ? !publicAccessBlocked : null,
            publicAccessBlocked != null ? (publicAccessBlocked ? "blocked" : "allowed") : null,
            null, // network_restrictions - would need to check bucket policy
            storage.get("HttpsOnly"), // whether the bucket policy denies plain HTTP
            storage.get("VersioningEnabled"),
            null, // soft_delete_enabled - S3 doesn't have soft delete
            null, // soft_delete_retention_days
            null, // backup_enabled - S3 doesn't have explicit backup
            storage.get("LifecycleRuleCount"),
            null, // access_tier - S3 doesn't have access tiers at bucket level
            CloudOpsDataConverter.convertValue(storage.get("CreationDate"), SqlTypeName.TIMESTAMP),
            null, // modified_date - S3 doesn't track bucket modification
            storage.get("Tags")
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying AWS storage resources failed: " + e.getMessage(), e);
    }

    return results;
  }
}
