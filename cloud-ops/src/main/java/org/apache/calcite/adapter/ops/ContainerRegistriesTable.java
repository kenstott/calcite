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
 * Table containing container registry information across cloud providers.
 */
public class ContainerRegistriesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerRegistriesTable.class);
  public ContainerRegistriesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("registry_name", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)
        .add("registry_uri", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("sku", SqlTypeName.VARCHAR).nullable(true)
        .add("admin_user_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("public_access", SqlTypeName.VARCHAR).nullable(true)
        .add("image_scanning_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("immutable_tags", SqlTypeName.BOOLEAN).nullable(true)

        // Security facts
        .add("encryption_type", SqlTypeName.VARCHAR).nullable(true)
        .add("encryption_key", SqlTypeName.VARCHAR).nullable(true)
        .add("quarantine_policy", SqlTypeName.VARCHAR).nullable(true)
        .add("trust_policy", SqlTypeName.VARCHAR).nullable(true)
        .add("retention_policy", SqlTypeName.VARCHAR).nullable(true)

        // Timestamps
        .add("created_at", SqlTypeName.TIMESTAMP).nullable(true)

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
      List<Map<String, Object>> registryResults = azureProvider.queryContainerRegistries(subscriptionIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "azure",
            registry.get("SubscriptionId"),
            registry.get("RegistryName"),
            registry.get("Application"),
            registry.get("Location"),
            registry.get("ResourceGroup"),
            registry.get("ResourceId"),
            registry.get("LoginServer"),
            registry.get("RegistrySKU"),
            registry.get("AdminUserEnabled"),
            registry.get("PublicNetworkAccess"),
            null, // image scanning: a Defender for Cloud setting, not a registry property
            null, // immutable tags: set per repository in ACR, not per registry
            registry.get("Encryption"),
            registry.get("EncryptionKey"),
            registry.get("QuarantinePolicy"),
            registry.get("TrustPolicy"),
            registry.get("RetentionPolicy"),
            CloudOpsDataConverter.convertValue(registry.get("CreatedAt"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure container registries: {}", e.getMessage());
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
      List<Map<String, Object>> registryResults = gcpProvider.queryContainerRegistries(projectIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "gcp",
            registry.get("ProjectId"),
            registry.get("RegistryName"),
            registry.get("Application"),
            registry.get("Location"),
            null, // resource group not applicable
            registry.get("ResourceId"),
            registry.get("RegistryUri"),
            registry.get("Format"), // GCP uses format instead of SKU
            null, // admin user not a GCP concept
            null, // public access controlled by IAM
            registry.get("ScanningEnabled"),
            registry.get("ImmutableTags"),
            registry.get("Encryption"),
            registry.get("KmsKey"),
            null, // quarantine policy not in GCP
            null, // trust policy not in GCP
            ((Number) registry.get("CleanupPoliciesCount")).intValue() > 0
                ? "Enabled" : "Disabled",
            CloudOpsDataConverter.convertValue(registry.get("CreateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP container registries: {}", e.getMessage());
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
      List<Map<String, Object>> registryResults = awsProvider.queryContainerRegistries(accountIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "aws",
            registry.get("AccountId"),
            registry.get("RepositoryName"),
            registry.get("Application"),
            registry.get("Region"),
            null, // resource group not applicable
            registry.get("ResourceId"),
            registry.get("RepositoryUri"),
            null, // SKU not applicable to ECR
            false, // admin user not applicable to ECR
            "Private", // ECR is always private
            registry.get("ImageScanningEnabled"),
            "IMMUTABLE".equals(registry.get("ImageTagMutability")),
            registry.get("EncryptionType"),
            registry.get("KmsKey"),
            null, // quarantine policy not in ECR
            null, // trust policy not in ECR
            null, // retention policy configured per lifecycle rules
            CloudOpsDataConverter.convertValue(registry.get("CreatedAt"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS container registries: {}", e.getMessage());
    }

    return results;
  }
}
