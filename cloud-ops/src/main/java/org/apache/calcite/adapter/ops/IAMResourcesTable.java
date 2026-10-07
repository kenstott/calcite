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
 * Table containing IAM resource information across cloud providers.
 */
public class IAMResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IAMResourcesTable.class);
  public IAMResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR).nullable(true)
        .add("iam_resource", SqlTypeName.VARCHAR).nullable(true)
        .add("iam_resource_type", SqlTypeName.VARCHAR).nullable(true)
        .add("application", SqlTypeName.VARCHAR).nullable(true)
        .add("region", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_group", SqlTypeName.VARCHAR).nullable(true)
        .add("resource_id", SqlTypeName.VARCHAR).nullable(true)

        // Configuration facts
        .add("configuration", SqlTypeName.VARCHAR).nullable(true)
        .add("security_configuration", SqlTypeName.VARCHAR).nullable(true)

        // IAM specific facts
        .add("principal_type", SqlTypeName.VARCHAR).nullable(true)
        .add("email", SqlTypeName.VARCHAR).nullable(true)
        .add("is_active", SqlTypeName.BOOLEAN).nullable(true)
        .add("mfa_enabled", SqlTypeName.BOOLEAN).nullable(true)
        .add("access_key_count", SqlTypeName.INTEGER).nullable(true)
        .add("active_access_keys", SqlTypeName.INTEGER).nullable(true)

        // Timestamps
        .add("create_date", SqlTypeName.TIMESTAMP).nullable(true)
        .add("password_last_used", SqlTypeName.TIMESTAMP).nullable(true)

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
      List<Map<String, Object>> iamResults = azureProvider.queryIAMResources(subscriptionIds);

      for (Map<String, Object> iam : iamResults) {
        results.add(new Object[]{
            "azure",
            iam.get("SubscriptionId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Location"),
            iam.get("ResourceGroup"),
            iam.get("ResourceId"),
            iam.get("Configuration"),
            iam.get("SecurityConfiguration"),
            iam.get("PrincipalType"),
            null, // email not applicable
            true, // a managed identity or vault exists or it does not; neither can be disabled
            null, // MFA does not apply to managed identities
            null, // access key count not applicable
            null, // active access keys not applicable
            null, // Resource Graph does not report when these were created
            null  // password last used not applicable
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying Azure IAM resources failed: " + e.getMessage(), e);
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
      List<Map<String, Object>> iamResults = gcpProvider.queryIAMResources(projectIds);

      for (Map<String, Object> iam : iamResults) {
        String resourceType = (String) iam.get("IAMResourceType");
        Boolean isActive = true;
        if ("ServiceAccount".equals(resourceType)) {
          isActive = !(Boolean) iam.getOrDefault("Disabled", false);
        }

        results.add(new Object[]{
            "gcp",
            iam.get("ProjectId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Location"),
            null, // resource group not applicable
            iam.get("ResourceId"),
            iam.get("DisplayName") != null ? "Display: " + iam.get("DisplayName") : null,
            iam.get("Description"),
            iam.get("PrincipalType"),
            iam.get("Email"),
            isActive,
            null, // MFA does not apply to service accounts
            iam.get("AccessKeyCount"),
            iam.get("ActiveAccessKeys"),
            null, // the IAM API does not report when a service account was created
            null  // password last used not applicable
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying GCP IAM resources failed: " + e.getMessage(), e);
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
      List<Map<String, Object>> iamResults = awsProvider.queryIAMResources(accountIds);

      for (Map<String, Object> iam : iamResults) {
        String resourceType = (String) iam.get("IAMResourceType");

        results.add(new Object[]{
            "aws",
            iam.get("AccountId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Region"),
            null, // resource group not applicable
            iam.get("ResourceId"),
            iam.get("Path") != null ? "Path: " + iam.get("Path") :
                iam.get("Description") != null ? "Description: " + iam.get("Description") : null,
            null, // security configuration not computed
            resourceType == null ? null : resourceType.replace("IAM ", "").replace(" ", ""),
            null, // email not exposed
            !"IAM Policy".equals(resourceType), // policies aren't active/inactive
            iam.get("MFAEnabled"),
            iam.get("AccessKeyCount") != null ? ((Number) iam.get("AccessKeyCount")).intValue() : null,
            iam.get("ActiveAccessKeys") != null ?
                ((Number) iam.get("ActiveAccessKeys")).intValue() : null,
            CloudOpsDataConverter.convertValue(iam.get("CreateDate"), SqlTypeName.TIMESTAMP),
            CloudOpsDataConverter.convertValue(iam.get("PasswordLastUsed"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (RuntimeException e) {
      throw new IllegalStateException("Querying AWS IAM resources failed: " + e.getMessage(), e);
    }

    return results;
  }
}
