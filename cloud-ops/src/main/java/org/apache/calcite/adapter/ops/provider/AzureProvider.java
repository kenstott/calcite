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
package org.apache.calcite.adapter.ops.provider;

import org.apache.calcite.adapter.ops.CloudOpsConfig;
import org.apache.calcite.adapter.ops.util.CloudOpsCacheManager;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.resourcegraph.ResourceGraphManager;
import com.azure.resourcemanager.resourcegraph.models.QueryRequest;
import com.azure.resourcemanager.resourcegraph.models.QueryRequestOptions;
import com.azure.resourcemanager.resourcegraph.models.QueryResponse;
import com.azure.resourcemanager.resourcegraph.models.ResultFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Azure provider implementation using Azure Resource Graph with KQL queries.
 */
public class AzureProvider implements CloudProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureProvider.class);

  private final CloudOpsConfig.AzureConfig config;
  private final ResourceGraphManager resourceGraphManager;
  private final TokenCredential credential;
  private final ObjectMapper objectMapper;
  private final CloudOpsCacheManager cacheManager;

  public AzureProvider(CloudOpsConfig.AzureConfig config) {
    this.config = config;
    this.objectMapper = new ObjectMapper();

    this.credential = new ClientSecretCredentialBuilder()
        .tenantId(config.tenantId)
        .clientId(config.clientId)
        .clientSecret(config.clientSecret)
        .build();

    AzureProfile profile = new AzureProfile(config.tenantId, null, AzureEnvironment.AZURE);

    this.resourceGraphManager = ResourceGraphManager
        .authenticate(credential, profile);

    // Initialize cache manager with defaults (will be updated to use CloudOpsConfig)
    this.cacheManager = new CloudOpsCacheManager(5, false);
  }

  public AzureProvider(CloudOpsConfig.AzureConfig config, CloudOpsCacheManager cacheManager) {
    this.config = config;
    this.objectMapper = new ObjectMapper();
    this.cacheManager = cacheManager;

    this.credential = new ClientSecretCredentialBuilder()
        .tenantId(config.tenantId)
        .clientId(config.clientId)
        .clientSecret(config.clientSecret)
        .build();

    AzureProfile profile = new AzureProfile(config.tenantId, null, AzureEnvironment.AZURE);

    this.resourceGraphManager = ResourceGraphManager
        .authenticate(credential, profile);
  }

  private List<Map<String, Object>> executeKqlQuery(String kql, List<String> subscriptionIds) {
    // Build cache key for the query
    String cacheKey =
        CloudOpsCacheManager.buildCacheKey("azure", "kql", kql.hashCode(), subscriptionIds);

    return cacheManager.getOrCompute(cacheKey, () -> {
      List<Map<String, Object>> results = new ArrayList<>();

      // Resource Graph returns at most 1000 rows per call and a skip token for the rest
      String skipToken = null;
      do {
        QueryRequestOptions options = new QueryRequestOptions()
            .withResultFormat(ResultFormat.OBJECT_ARRAY)
            .withTop(1000)
            .withSkipToken(skipToken);

        QueryRequest queryRequest = new QueryRequest()
            .withSubscriptions(subscriptionIds)
            .withQuery(kql)
            .withOptions(options);

        final QueryResponse response;
        try {
          response = resourceGraphManager.resourceProviders().resources(queryRequest);
        } catch (RuntimeException e) {
          throw new IllegalStateException(
              "Azure Resource Graph query failed: " + e.getMessage(), e);
        }

        if (!(response.data() instanceof List)) {
          throw new IllegalStateException("Azure Resource Graph returned "
              + (response.data() == null ? "no data" : response.data().getClass().getName())
              + " instead of a list of rows");
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> page = (List<Map<String, Object>>) response.data();
        results.addAll(page);
        skipToken = response.skipToken();
      } while (skipToken != null && !skipToken.isEmpty());

      return results;
    });
  }

  @Override public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds) {
    return queryKubernetesClusters(subscriptionIds, null);
  }

  /**
   * Query Kubernetes clusters with projection support.
   * Azure Resource Graph supports full projection via KQL project clause.
   */
  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, sortHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, sortHandler, paginationHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                                          @Nullable CloudOpsFilterHandler filterHandler) {

    // Build comprehensive cache key including all optimization parameters
    String cacheKey =
        CloudOpsCacheManager.buildComprehensiveCacheKey("azure", "kubernetes_clusters", projectionHandler, sortHandler, paginationHandler, filterHandler, subscriptionIds);

    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);

    if (shouldCache) {
      return cacheManager.getOrCompute(
          cacheKey, () -> executeKubernetesClusterQuery(
          subscriptionIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeKubernetesClusterQuery(
          subscriptionIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeKubernetesClusterQuery(List<String> subscriptionIds,
                                                                 @Nullable CloudOpsProjectionHandler projectionHandler,
                                                                 @Nullable CloudOpsSortHandler sortHandler,
                                                                 @Nullable CloudOpsPaginationHandler paginationHandler,
                                                                 @Nullable CloudOpsFilterHandler filterHandler) {
    String kql = buildKubernetesClusterKql(projectionHandler, sortHandler, paginationHandler, filterHandler);

    if (LOGGER.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        LOGGER.debug("Azure KQL with projection optimization: {}", metrics);
      }

      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, 0);
        LOGGER.debug("Azure KQL with filter optimization: {}", metrics);
      }
    }

    return executeKqlQuery(kql, subscriptionIds);
  }

  /**
   * Get cache metrics for monitoring.
   */
  public CloudOpsCacheManager.CacheMetrics getCacheMetrics() {
    return cacheManager.getCacheMetrics();
  }

  /**
   * Invalidate cache entries for a specific subscription.
   */
  public void invalidateSubscriptionCache(String subscriptionId) {
    // Invalidate all cache entries that contain this subscription ID
    // This is a simplified approach - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated Azure cache for subscription: {}", subscriptionId);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAllCache() {
    cacheManager.invalidateAll();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Invalidated all Azure cache entries");
    }
  }

  /**
   * Build KQL query for Kubernetes clusters with optional projection, sort, pagination, and filtering.
   */
  private String buildKubernetesClusterKql(@Nullable CloudOpsProjectionHandler projectionHandler,
                                          @Nullable CloudOpsSortHandler sortHandler,
                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return buildKubernetesClusterKql(projectionHandler, sortHandler, paginationHandler, null);
  }

  private String buildKubernetesClusterKql(@Nullable CloudOpsProjectionHandler projectionHandler,
                                          @Nullable CloudOpsSortHandler sortHandler,
                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                          @Nullable CloudOpsFilterHandler filterHandler) {
    StringBuilder kql = new StringBuilder();
    kql.append("Resources\n")
       .append("| where type == 'microsoft.containerservice/managedclusters'\n")
       .append("| extend Application = case(\n")
       .append("    isnotempty(tags.Application), tags.Application,\n")
       .append("    isnotempty(tags.app), tags.app,\n")
       .append("    'Untagged/Orphaned'\n")
       .append(")\n")
       .append("| extend ClusterVersion = tostring(properties.kubernetesVersion)\n")
       .append("| extend NodeResourceGroup = tostring(properties.nodeResourceGroup)\n")
       // A cluster without an access profile is a public one
       .append("| extend PrivateCluster = "
           + "tobool(properties.apiServerAccessProfile.enablePrivateCluster) == true\n")
       .append("| extend PublicEndpoint = not(PrivateCluster)\n")
       // Container insights (the omsagent add-on) collects logs and metrics; managed
       // Prometheus collects metrics alone
       .append("| extend LoggingEnabled = "
           + "tobool(properties.addonProfiles.omsagent.enabled) == true\n")
       .append("| extend MonitoringEnabled = LoggingEnabled "
           + "or tobool(properties.azureMonitorProfile.metrics.enabled) == true\n")
       .append("| extend CreatedDate = tostring(systemData.createdAt)\n")
       .append("| extend ModifiedDate = tostring(systemData.lastModifiedAt)\n")
       .append("| extend NetworkPlugin = tostring(properties.networkProfile.networkPlugin)\n")
       .append("| extend NetworkPolicy = tostring(properties.networkProfile.networkPolicy)\n")
       .append("| extend ServiceCidr = tostring(properties.networkProfile.serviceCidr)\n")
       .append("| extend PodCidr = tostring(properties.networkProfile.podCidr)\n")
       .append("| extend RBACEnabled = tobool(properties.enableRBAC)\n")
       .append("| extend AADEnabled = tobool(properties.aadProfile.managed)\n")
       .append("| extend AuthorizedIPRanges = coalesce("
           + "array_length(properties.apiServerAccessProfile.authorizedIPRanges), 0)\n")
       .append("| extend DiskEncryption = case(\n")
       .append("    isnotempty(properties.diskEncryptionSetID), 'Customer Managed Key',\n")
       .append("    'Platform Managed Key'\n")
       .append(")\n")
       .append("| extend NodePoolCount = array_length(properties.agentPoolProfiles)\n")
       // Resource Graph has no mv-apply: total the node pools in a sub-query and join it
       .append("| join kind=leftouter (\n")
       .append("    Resources\n")
       .append("    | where type == 'microsoft.containerservice/managedclusters'\n")
       .append("    | mv-expand pool = properties.agentPoolProfiles\n")
       .append("    | summarize NodeCount = sum(toint(pool['count'])) by id\n")
       .append(") on id\n")
       .append("| extend Tags = tostring(tags)\n");

    // Add filter WHERE clause if specified
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      String whereClause = filterHandler.buildAzureKqlWhereClause();
      if (whereClause != null) {
        kql.append(whereClause).append("\n");
      }
    }

    // Add projection clause if specified
    if (projectionHandler != null && !projectionHandler.isSelectAll()) {
      String projectionClause = projectionHandler.buildAzureKqlProjectClause();
      if (projectionClause != null) {
        kql.append(projectionClause).append("\n");
      } else {
        // Fallback to default projection
        kql.append(getDefaultKubernetesProjectClause()).append("\n");
      }
    } else {
      // Default projection for SELECT * or no projection handler
      kql.append(getDefaultKubernetesProjectClause()).append("\n");
    }

    // Rows are sorted once, by the table, across all providers
    kql.append("| order by Application, ClusterName");

    // Add pagination clause if specified
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      String paginationClause = paginationHandler.buildAzureKqlPaginationClause();
      if (paginationClause != null) {
        kql.append(" ").append(paginationClause);
        if (LOGGER.isDebugEnabled()) {
          CloudOpsPaginationHandler.PaginationMetrics metrics =
              paginationHandler.calculateMetrics(true, 10000); // Assume large dataset
          LOGGER.debug("Azure KQL with pagination optimization: {}", metrics);
        }
      }
    }

    return kql.toString();
  }

  /**
   * Get default project clause for Kubernetes clusters.
   */
  private String getDefaultKubernetesProjectClause() {
    return "| project\n"
  +
           "    SubscriptionId = subscriptionId,\n"
  +
           "    ClusterName = name,\n"
  +
           "    ResourceGroup = resourceGroup,\n"
  +
           "    Location = location,\n"
  +
           "    ResourceId = id,\n"
  +
           "    Application,\n"
  +
           "    ClusterVersion,\n"
  +
           "    NodePoolCount,\n"
  +
           "    RBACEnabled,\n"
  +
           "    AADEnabled,\n"
  +
           "    PrivateCluster,\n"
  +
           "    PublicEndpoint,\n"
  +
           "    LoggingEnabled,\n"
  +
           "    MonitoringEnabled,\n"
  +
           "    CreatedDate,\n"
  +
           "    ModifiedDate,\n"
  +
           "    NetworkPlugin,\n"
  +
           "    NetworkPolicy,\n"
  +
           "    ServiceCidr,\n"
  +
           "    PodCidr,\n"
  +
           "    AuthorizedIPRanges,\n"
  +
           "    DiskEncryption,\n"
  +
        "    NodeCount,\n"
  +
        "    Tags";
  }

  @Override public List<Map<String, Object>> queryStorageResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type in (\n"
        + "    'microsoft.storage/storageaccounts',\n"
        + "    'microsoft.sql/servers/databases',\n"
        + "    'microsoft.documentdb/databaseaccounts',\n"
        + "    'microsoft.compute/disks'\n"
        + ")\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend StorageType = case(\n"
        + "    type == 'microsoft.storage/storageaccounts', 'Storage Account',\n"
        + "    type == 'microsoft.sql/servers/databases', 'SQL Database',\n"
        + "    type == 'microsoft.documentdb/databaseaccounts', 'Cosmos DB',\n"
        + "    type == 'microsoft.compute/disks', 'Managed Disk',\n"
        + "    type\n"
        + ")\n"
        + "| extend EncryptionEnabled = case(\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        isnotnull(properties.encryption.services.blob.enabled),\n"
        // Resource Graph does not carry a SQL database's transparent data encryption
        + "    type == 'microsoft.sql/servers/databases',\n"
        + "        bool(null),\n"
        + "    type == 'microsoft.documentdb/databaseaccounts',\n"
        + "        true,\n"
        + "    type == 'microsoft.compute/disks',\n"
        + "        isnotnull(properties.encryption),\n"
        + "    false\n"
        + ")\n"
        + "| extend EncryptionMethod = case(\n"
        + "    type == 'microsoft.storage/storageaccounts' and properties.encryption.keySource == 'Microsoft.Keyvault',\n"
        + "        'Customer Managed Key',\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        tostring(properties.encryption.keySource),\n"
        + "    type == 'microsoft.compute/disks' and isnotnull(properties.encryption.diskEncryptionSetId),\n"
        + "        'Customer Managed Key',\n"
        + "    EncryptionEnabled == true,\n"
        + "        'Service Managed Key',\n"
        + "    isnull(EncryptionEnabled), '',\n"
        + "    'None'\n"
        + ")\n"
        + "| extend HttpsOnly = case(\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        tobool(properties.supportsHttpsTrafficOnly),\n"
        // SQL and Cosmos DB accept only encrypted connections; a disk has no endpoint
        + "    type == 'microsoft.compute/disks', bool(null),\n"
        + "    true\n"
        + ")\n"
        + "| extend MinimumTlsVersion = case(\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        tostring(properties.minimumTlsVersion),\n"
        + "    ''\n"
        + ")\n"
        + "| extend NetworkDefaultAction = case(\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        tostring(properties.networkAcls.defaultAction),\n"
        + "    ''\n"
        + ")\n"
        + "| extend PublicNetworkAccess = case(\n"
        + "    type == 'microsoft.storage/storageaccounts',\n"
        + "        tostring(properties.publicNetworkAccess),\n"
        + "    type == 'microsoft.sql/servers/databases',\n"
        + "        tostring(properties.publicNetworkAccess),\n"
        + "    type == 'microsoft.compute/disks',\n"
        + "        tostring(properties.publicNetworkAccess),\n"
        + "    ''\n"
        + ")\n"
        + "| extend PublicBlobAccess = iff(type == 'microsoft.storage/storageaccounts',\n"
        + "    tobool(properties.allowBlobPublicAccess), bool(null))\n"
        + "| extend StorageClass = tostring(sku.name)\n"
        + "| extend ReplicationType = iff(\n"
        + "    type in ('microsoft.storage/storageaccounts', 'microsoft.compute/disks'),\n"
        + "    tostring(split(tostring(sku.name), '_')[1]), '')\n"
        + "| extend AccessTier = case(\n"
        + "    type == 'microsoft.storage/storageaccounts', tostring(properties.accessTier),\n"
        + "    type == 'microsoft.compute/disks', tostring(properties.tier),\n"
        + "    ''\n"
        + ")\n"
        + "| extend CreatedDate = case(\n"
        + "    type == 'microsoft.storage/storageaccounts', tostring(properties.creationTime),\n"
        + "    type == 'microsoft.compute/disks', tostring(properties.timeCreated),\n"
        + "    type == 'microsoft.sql/servers/databases', tostring(properties.creationDate),\n"
        + "    ''\n"
        + ")\n"
        + "| extend SizeBytes = case(\n"
        + "    type == 'microsoft.compute/disks', tolong(properties.diskSizeBytes),\n"
        + "    type == 'microsoft.sql/servers/databases', tolong(properties.maxSizeBytes),\n"
        + "    long(null)\n"
        + ")\n"
        + "| extend Tags = tostring(tags)\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    StorageResource = name,\n"
        + "    StorageType,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    EncryptionEnabled,\n"
        + "    EncryptionMethod,\n"
        + "    HttpsOnly,\n"
        + "    MinimumTlsVersion,\n"
        + "    NetworkDefaultAction,\n"
        + "    PublicNetworkAccess,\n"
        + "    PublicBlobAccess,\n"
        + "    StorageClass,\n"
        + "    ReplicationType,\n"
        + "    AccessTier,\n"
        + "    CreatedDate,\n"
        + "    SizeBytes,\n"
        + "    Tags\n"
        + "| order by Application, StorageType, StorageResource";

    List<Map<String, Object>> accounts = new ArrayList<>();
    for (Map<String, Object> row : executeKqlQuery(kql, subscriptionIds)) {
      if (!"Storage Account".equals(row.get("StorageType"))) {
        accounts.add(row);
        continue;
      }
      // Versioning, soft delete and lifecycle rules are settings of an account's blob service
      // and management policy, which Resource Graph does not index: read them from ARM
      Map<String, Object> account = new HashMap<>(row);
      String id = String.valueOf(row.get("ResourceId"));
      JsonNode blobService = armGet(id + "/blobServices/default?api-version=2023-05-01");
      if (blobService == null) {
        throw new IllegalStateException("Storage account " + id + " has no blob service");
      }
      JsonNode settings = blobService.path("properties");
      // ARM leaves a switch out of the answer while it has never been turned on
      account.put("VersioningEnabled", settings.path("isVersioningEnabled").asBoolean(false));
      JsonNode retention = settings.path("deleteRetentionPolicy");
      boolean softDelete = retention.path("enabled").asBoolean(false);
      account.put("SoftDeleteEnabled", softDelete);
      account.put("SoftDeleteRetentionDays",
          softDelete && retention.has("days") ? retention.get("days").asInt() : null);
      // An account without a management policy answers 404: it has no lifecycle rules
      JsonNode policy = armGet(id + "/managementPolicies/default?api-version=2023-05-01");
      account.put("LifecycleRulesCount",
          policy == null ? 0 : policy.path("properties").path("policy").path("rules").size());
      accounts.add(account);
    }
    return accounts;
  }

  /**
   * Reads one Azure Resource Manager resource.
   *
   * @param path the resource id with its api-version, starting with a slash
   * @return the resource, or null when Azure answers 404
   */
  private @Nullable JsonNode armGet(String path) {
    String token = credential
        .getToken(new TokenRequestContext().addScopes("https://management.azure.com/.default"))
        .block()
        .getToken();
    try {
      HttpURLConnection connection =
          (HttpURLConnection) new URL("https://management.azure.com" + path).openConnection();
      connection.setConnectTimeout(15000);
      connection.setReadTimeout(30000);
      connection.setRequestProperty("Authorization", "Bearer " + token);
      connection.setRequestProperty("Accept", "application/json");
      int status = connection.getResponseCode();
      if (status == HttpURLConnection.HTTP_NOT_FOUND) {
        return null;
      }
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IllegalStateException("Azure Resource Manager answered " + status + " for "
            + path + ": " + readAll(connection.getErrorStream()));
      }
      try (InputStream body = connection.getInputStream()) {
        return objectMapper.readTree(body);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Reading " + path + " from Azure Resource Manager failed: " + e.getMessage(), e);
    }
  }

  private static String readAll(@Nullable InputStream stream) throws IOException {
    if (stream == null) {
      return "";
    }
    try (InputStream in = stream) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) != -1) {
        bytes.write(buffer, 0, read);
      }
      return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  @Override public List<Map<String, Object>> queryComputeInstances(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type == 'microsoft.compute/virtualmachines'\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend VMSize = tostring(properties.hardwareProfile.vmSize)\n"
        + "| extend OSType = tostring(properties.storageProfile.osDisk.osType)\n"
        + "| extend PowerState = tostring(properties.extended.instanceView.powerState.displayStatus)\n"
        + "| extend DiskEncryption = case(\n"
        + "    isnotnull(properties.storageProfile.osDisk.encryptionSettings.enabled) and\n"
        + "        tobool(properties.storageProfile.osDisk.encryptionSettings.enabled) == true,\n"
        + "        'Enabled',\n"
        + "    isnotnull(properties.storageProfile.osDisk.managedDisk.diskEncryptionSet),\n"
        + "        'Enabled',\n"
        + "    'Disabled'\n"
        + ")\n"
        + "| extend ManagedDisk = isnotnull(properties.storageProfile.osDisk.managedDisk)\n"
        + "| extend BootDiagnostics = tobool(properties.diagnosticsProfile.bootDiagnostics.enabled)\n"
        + "| extend AvailabilitySet = tostring(properties.availabilitySet.id)\n"
        + "| extend AvailabilityZone = tostring(zones[0])\n"
        + "| extend LaunchTime = tostring(properties.timeCreated)\n"
        + "| extend AttachedIdentity = tostring(bag_keys(identity.userAssignedIdentities)[0])\n"
        + "| extend PrimaryNicId = tolower(tostring(properties.networkProfile.networkInterfaces[0].id))\n"
        + "| join kind=leftouter (\n"
        + "    Resources\n"
        + "    | where type == 'microsoft.network/networkinterfaces'\n"
        + "    | extend NicSubnetId = tostring(properties.ipConfigurations[0].properties.subnet.id)\n"
        + "    | extend PrivateIp = tostring(properties.ipConfigurations[0].properties.privateIPAddress)\n"
        + "    | extend PublicIpId = tolower(tostring(properties.ipConfigurations[0].properties.publicIPAddress.id))\n"
        + "    | extend NicNsgId = tostring(properties.networkSecurityGroup.id)\n"
        + "    | project PrimaryNicId = tolower(id), NicSubnetId, PrivateIp, PublicIpId, NicNsgId\n"
        + ") on PrimaryNicId\n"
        + "| join kind=leftouter (\n"
        + "    Resources\n"
        + "    | where type == 'microsoft.network/publicipaddresses'\n"
        + "    | project PublicIpId = tolower(id), PublicIp = tostring(properties.ipAddress)\n"
        + ") on PublicIpId\n"
        + "| extend SubnetId = NicSubnetId\n"
        + "| extend VNetId = iff(isnotempty(NicSubnetId), strcat_array(array_slice(split(NicSubnetId, '/'), 0, 8), '/'), '')\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    VMName = name,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    VMSize,\n"
        + "    OSType,\n"
        + "    PowerState,\n"
        + "    DiskEncryption,\n"
        + "    ManagedDiskEnabled = ManagedDisk,\n"
        + "    BootDiagnostics,\n"
        + "    AvailabilitySet,\n"
        + "    AvailabilityZone,\n"
        + "    LaunchTime,\n"
        + "    SubnetId,\n"
        + "    VNetId,\n"
        + "    AttachedIdentity,\n"
        + "    PrivateIp,\n"
        + "    PublicIp,\n"
        + "    SecurityGroups = NicNsgId\n"
        + "| order by Application, VMName";

    return executeKqlQuery(kql, subscriptionIds);
  }

  /**
   * Emits one row per (VM, Network Security Group) association, resolved through the VM's network
   * interface. Feeds the compute_security_groups junction. {@code ComputeResourceId} is the VM ARM id
   * (-> compute_resources.resource_id); {@code SecurityGroupId} is the NSG ARM id
   * (-> network_resources.native_id).
   */
  public List<Map<String, Object>> queryComputeSecurityGroups(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type == 'microsoft.network/networkinterfaces'\n"
        + "| where isnotempty(properties.virtualMachine.id) and isnotempty(properties.networkSecurityGroup.id)\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    InstanceId = tostring(split(tostring(properties.virtualMachine.id), '/')[8]),\n"
        + "    ComputeResourceId = tostring(properties.virtualMachine.id),\n"
        + "    SecurityGroupId = tostring(properties.networkSecurityGroup.id)";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryNetworkResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type in (\n"
        + "    'microsoft.network/virtualnetworks',\n"
        + "    'microsoft.network/networksecuritygroups',\n"
        + "    'microsoft.network/publicipaddresses',\n"
        + "    'microsoft.network/loadbalancers',\n"
        + "    'microsoft.network/applicationgateways'\n"
        + ")\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend NetworkResourceType = case(\n"
        + "    type == 'microsoft.network/virtualnetworks', 'Virtual Network',\n"
        + "    type == 'microsoft.network/networksecuritygroups', 'Network Security Group',\n"
        + "    type == 'microsoft.network/publicipaddresses', 'Public IP',\n"
        + "    type == 'microsoft.network/loadbalancers', 'Load Balancer',\n"
        + "    type == 'microsoft.network/applicationgateways', 'Application Gateway',\n"
        + "    type\n"
        + ")\n"
        + "| extend Configuration = case(\n"
        + "    type == 'microsoft.network/virtualnetworks',\n"
        + "        strcat('Address Space: ', tostring(properties.addressSpace.addressPrefixes)),\n"
        + "    type == 'microsoft.network/networksecuritygroups',\n"
        + "        strcat('Rules: ', tostring(array_length(properties.securityRules))),\n"
        + "    type == 'microsoft.network/publicipaddresses',\n"
        + "        strcat('Allocation: ', tostring(properties.publicIPAllocationMethod)),\n"
        + "    type == 'microsoft.network/loadbalancers',\n"
        + "        strcat('SKU: ', tostring(sku.name)),\n"
        + "    ''\n"
        + ")\n"
        + "| extend SecurityFindings = case(\n"
        + "    type == 'microsoft.network/networksecuritygroups' and\n"
        + "        array_length(properties.securityRules) == 0,\n"
        + "        'No security rules defined',\n"
        + "    type == 'microsoft.network/publicipaddresses' and\n"
        + "        properties.publicIPAllocationMethod == 'Static',\n"
        + "        'Static public IP',\n"
        + "    ''\n"
        + ")\n"
        + "| extend CidrBlock = case(\n"
        + "    type == 'microsoft.network/virtualnetworks',\n"
        + "        tostring(properties.addressSpace.addressPrefixes[0]),\n"
        + "    type == 'microsoft.network/publicipaddresses',\n"
        + "        tostring(properties.ipAddress),\n"
        + "    ''\n"
        + ")\n"
        + "| extend State = tostring(properties.provisioningState)\n"
        + "| extend RuleCount = iff(type == 'microsoft.network/networksecuritygroups',\n"
        // toint: array_length is a long, and the union below declares the column int;
        // differing types would split it into two columns
        + "    toint(array_length(properties.securityRules)), int(null))\n"
        + "| join kind=leftouter (\n"
        + "    Resources\n"
        + "    | where type == 'microsoft.network/networksecuritygroups'\n"
        + "    | mv-expand rule = properties.securityRules\n"
        + "    | where rule.properties.direction =~ 'Inbound' and rule.properties.access =~ 'Allow'\n"
        + "        and tostring(rule.properties.sourceAddressPrefix) in~ ('*', '0.0.0.0/0', 'Internet', 'Any')\n"
        + "    | summarize OpenIngressRules = count() by id\n"
        + ") on id\n"
        + "| extend HasOpenIngress = iff(type == 'microsoft.network/networksecuritygroups',\n"
        + "    coalesce(OpenIngressRules, 0) > 0, bool(null))\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    NetworkResource = name,\n"
        + "    NativeId = id,\n"
        + "    NetworkResourceType,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    Configuration,\n"
        + "    SecurityFindings,\n"
        + "    CidrBlock,\n"
        + "    State,\n"
        + "    RuleCount,\n"
        + "    HasOpenIngress,\n"
        + "    Tags = tostring(tags)\n"
        + "| union (\n"
        + "    Resources\n"
        + "    | where type == 'microsoft.network/virtualnetworks'\n"
        + "    | mv-expand subnet = properties.subnets\n"
        + "    | project\n"
        + "        SubscriptionId = subscriptionId,\n"
        + "        NetworkResource = tostring(subnet.name),\n"
        + "        NativeId = tostring(subnet.id),\n"
        + "        NetworkResourceType = 'Subnet',\n"
        + "        ResourceGroup = resourceGroup,\n"
        + "        Location = location,\n"
        + "        ResourceId = tostring(subnet.id),\n"
        + "        Application = 'Untagged/Orphaned',\n"
        + "        Configuration = strcat('CIDR: ', tostring(subnet.properties.addressPrefix)),\n"
        + "        SecurityFindings = '',\n"
        + "        CidrBlock = tostring(subnet.properties.addressPrefix),\n"
        + "        State = tostring(subnet.properties.provisioningState),\n"
        + "        RuleCount = int(null),\n"
        + "        HasOpenIngress = bool(null),\n"
        + "        Tags = ''\n"
        + ")\n"
        + "| order by Application, NetworkResourceType, NetworkResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryIAMResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type in (\n"
        + "    'microsoft.managedidentity/userassignedidentities',\n"
        + "    'microsoft.keyvault/vaults'\n"
        + ")\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend IAMResourceType = case(\n"
        + "    type == 'microsoft.managedidentity/userassignedidentities', 'Managed Identity',\n"
        + "    type == 'microsoft.keyvault/vaults', 'Key Vault',\n"
        + "    type\n"
        + ")\n"
        + "| extend Configuration = case(\n"
        + "    type == 'microsoft.managedidentity/userassignedidentities',\n"
        + "        strcat('ClientId: ', tostring(properties.clientId)),\n"
        + "    type == 'microsoft.keyvault/vaults',\n"
        + "        strcat('SKU: ', tostring(properties.sku.name)),\n"
        + "    ''\n"
        + ")\n"
        + "| extend SecurityConfiguration = case(\n"
        + "    type == 'microsoft.keyvault/vaults',\n"
        + "        strcat('Purge Protection: ',\n"
        + "            case(tobool(properties.enablePurgeProtection) == true, 'Enabled', 'Disabled'),\n"
        + "            ' | Network: ', tostring(properties.networkAcls.defaultAction)),\n"
        + "    ''\n"
        + ")\n"
        + "| extend PrincipalType = iff(type == 'microsoft.managedidentity/userassignedidentities',\n"
        + "    'ManagedIdentity', '')\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    IAMResource = name,\n"
        + "    IAMResourceType,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    Configuration,\n"
        + "    SecurityConfiguration,\n"
        + "    PrincipalType\n"
        + "| order by Application, IAMResourceType, IAMResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryDatabaseResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type in (\n"
        + "    'microsoft.sql/servers',\n"
        + "    'microsoft.sql/servers/databases',\n"
        + "    'microsoft.dbforpostgresql/flexibleservers',\n"
        + "    'microsoft.dbformysql/flexibleservers',\n"
        + "    'microsoft.documentdb/databaseaccounts',\n"
        + "    'microsoft.cache/redis',\n"
        + "    'microsoft.cache/redisenterprise'\n"
        + ")\n"
        + "// every SQL server has a system database of this name\n"
        + "| where not(type == 'microsoft.sql/servers/databases' and name == 'master')\n"
        + "| extend SqlServer = type == 'microsoft.sql/servers'\n"
        + "| extend SqlDatabase = type == 'microsoft.sql/servers/databases'\n"
        + "| extend Postgres = type == 'microsoft.dbforpostgresql/flexibleservers'\n"
        + "| extend MySql = type == 'microsoft.dbformysql/flexibleservers'\n"
        + "| extend Flexible = Postgres or MySql\n"
        + "| extend Cosmos = type == 'microsoft.documentdb/databaseaccounts'\n"
        + "| extend Redis = type == 'microsoft.cache/redis'\n"
        + "| extend ManagedRedis = type == 'microsoft.cache/redisenterprise'\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend DatabaseType = case(\n"
        + "    SqlServer, 'SQL Server',\n"
        + "    SqlDatabase, 'SQL Database',\n"
        + "    Postgres, 'PostgreSQL Flexible Server',\n"
        + "    MySql, 'MySQL Flexible Server',\n"
        + "    Cosmos, 'Cosmos DB',\n"
        + "    Redis, 'Redis Cache',\n"
        + "    ManagedRedis, 'Azure Managed Redis',\n"
        + "    type\n"
        + ")\n"
        + "| extend Engine = case(\n"
        + "    SqlServer or SqlDatabase, 'sqlserver',\n"
        + "    Postgres, 'postgres',\n"
        + "    MySql, 'mysql',\n"
        + "    Cosmos, tostring(properties.EnabledApiTypes),\n"
        + "    Redis or ManagedRedis, 'redis',\n"
        + "    ''\n"
        + ")\n"
        + "| extend EngineVersion = case(\n"
        + "    SqlServer or Flexible, tostring(properties.version),\n"
        + "    Cosmos, tostring(properties.apiProperties.serverVersion),\n"
        + "    Redis or ManagedRedis, tostring(properties.redisVersion),\n"
        + "    ''\n"
        + ")\n"
        + "| extend RedisSku = strcat(tostring(properties.sku.name), '_',\n"
        + "    tostring(properties.sku.family), tostring(properties.sku.capacity))\n"
        + "| extend InstanceClass = case(\n"
        + "    SqlDatabase or Flexible or ManagedRedis, tostring(sku.name),\n"
        + "    Cosmos, tostring(properties.databaseAccountOfferType),\n"
        + "    Redis, RedisSku,\n"
        + "    ''\n"
        + ")\n"
        + "| extend AllocatedStorageGb = case(\n"
        + "    SqlDatabase, toint(tolong(properties.maxSizeBytes) / 1073741824),\n"
        + "    Flexible, toint(properties.storage.storageSizeGB),\n"
        + "    int(null)\n"
        + ")\n"
        + "| extend MultiAz = case(\n"
        + "    SqlDatabase, tobool(properties.zoneRedundant),\n"
        + "    Flexible, tostring(properties.highAvailability.mode) =~ 'ZoneRedundant',\n"
        + "    Cosmos, tobool(properties.locations[0].isZoneRedundant),\n"
        + "    Redis or ManagedRedis, array_length(zones) > 1,\n"
        + "    bool(null)\n"
        + ")\n"
        + "| extend Status = case(\n"
        + "    SqlServer or Flexible, tostring(properties.state),\n"
        + "    SqlDatabase, tostring(properties.status),\n"
        + "    tostring(properties.provisioningState)\n"
        + ")\n"
        + "| extend PublicNetworkAccess = case(\n"
        + "    Flexible, tostring(properties.network.publicNetworkAccess),\n"
        + "    SqlDatabase, '',\n"
        + "    tostring(properties.publicNetworkAccess)\n"
        + ")\n"
        + "| extend PubliclyAccessible = iff(isempty(PublicNetworkAccess), bool(null),\n"
        + "    PublicNetworkAccess =~ 'Enabled')\n"
        + "| extend EncryptionKey = case(\n"
        + "    SqlServer, tostring(properties.keyId),\n"
        + "    ManagedRedis, tostring(\n"
        + "        properties.encryption.customerManagedKeyEncryption.keyEncryptionKeyUrl),\n"
        + "    Flexible, tostring(properties.dataEncryption.primaryKeyURI),\n"
        + "    Cosmos, tostring(properties.keyVaultKeyUri),\n"
        + "    ''\n"
        + ")\n"
        + "| extend Encrypted = iff(Flexible or Cosmos, true, bool(null))\n"
        + "| extend TlsVersion = case(\n"
        + "    SqlServer or Cosmos, tostring(properties.minimalTlsVersion),\n"
        + "    Redis or ManagedRedis, tostring(properties.minimumTlsVersion),\n"
        + "    ''\n"
        + ")\n"
        + "| extend CosmosPeriodic = properties.backupPolicy.periodicModeProperties\n"
        + "| extend CosmosBackupHours = toint(CosmosPeriodic.backupRetentionIntervalInHours)\n"
        + "| extend CosmosBackupMode = tostring(properties.backupPolicy.type)\n"
        + "| extend CosmosTier = tostring(properties.backupPolicy.continuousModeProperties.tier)\n"
        + "| extend BackupRetentionDays = case(\n"
        + "    Flexible, toint(properties.backup.backupRetentionDays),\n"
        + "    Cosmos and CosmosBackupMode =~ 'Continuous' and CosmosTier contains '30', 30,\n"
        + "    Cosmos and CosmosBackupMode =~ 'Continuous', 7,\n"
        + "    Cosmos, toint(ceiling(todouble(CosmosBackupHours) / 24)),\n"
        + "    int(null)\n"
        + ")\n"
        + "| extend CreateTime = case(\n"
        + "    SqlDatabase, tostring(properties.creationDate),\n"
        + "    Flexible or Cosmos or ManagedRedis, tostring(systemData.createdAt),\n"
        + "    ''\n"
        + ")\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    DatabaseResource = name,\n"
        + "    DatabaseType,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    Engine,\n"
        + "    EngineVersion,\n"
        + "    InstanceClass,\n"
        + "    AllocatedStorageGb,\n"
        + "    MultiAz,\n"
        + "    Status,\n"
        + "    PubliclyAccessible,\n"
        + "    Encrypted,\n"
        + "    EncryptionKey,\n"
        + "    TlsVersion,\n"
        + "    BackupRetentionDays,\n"
        + "    CreateTime\n"
        + "| order by Application, DatabaseType, DatabaseResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryContainerRegistries(List<String> subscriptionIds) {
    String kql = "Resources\n"
        + "| where type == 'microsoft.containerregistry/registries'\n"
        + "| extend Application = case(\n"
        + "    isnotempty(tags.Application), tostring(tags.Application),\n"
        + "    isnotempty(tags.application), tostring(tags.application),\n"
        + "    isnotempty(tags.app), tostring(tags.app),\n"
        + "    'Untagged/Orphaned'\n"
        + ")\n"
        + "| extend RegistrySKU = tostring(sku.name)\n"
        + "| extend AdminUserEnabled = tobool(properties.adminUserEnabled)\n"
        + "| extend PublicNetworkAccess = tostring(properties.publicNetworkAccess)\n"
        + "| extend NetworkRuleSetDefaultAction = tostring(properties.networkRuleSet.defaultAction)\n"
        + "| extend ZoneRedundancy = tostring(properties.zoneRedundancy)\n"
        + "| extend DataEndpointEnabled = tobool(properties.dataEndpointEnabled)\n"
        + "| extend Encryption = case(\n"
        + "    properties.encryption.status =~ 'enabled',\n"
        + "        'Customer Managed Key',\n"
        + "    'Service Managed Key'\n"
        + ")\n"
        + "| extend EncryptionKey = tostring(properties.encryption.keyVaultProperties.keyIdentifier)\n"
        + "| extend QuarantinePolicy = tostring(properties.policies.quarantinePolicy.status)\n"
        + "| extend TrustPolicy = tostring(properties.policies.trustPolicy.status)\n"
        + "| extend RetentionPolicy = tostring(properties.policies.retentionPolicy.status)\n"
        + "| extend SecurityConfiguration = strcat(\n"
        + "    'Admin User: ', case(AdminUserEnabled == true, 'Enabled', 'Disabled'),\n"
        + "    ' | Network: ', case(\n"
        + "        PublicNetworkAccess == 'Disabled', 'Private Only',\n"
        + "        NetworkRuleSetDefaultAction == 'Deny', 'Restricted',\n"
        + "        'Public'\n"
        + "    ),\n"
        + "    ' | Encryption: ', Encryption\n"
        + ")\n"
        + "| project\n"
        + "    SubscriptionId = subscriptionId,\n"
        + "    RegistryName = name,\n"
        + "    ResourceGroup = resourceGroup,\n"
        + "    Location = location,\n"
        + "    ResourceId = id,\n"
        + "    Application,\n"
        + "    RegistrySKU,\n"
        + "    AdminUserEnabled,\n"
        + "    PublicNetworkAccess,\n"
        + "    NetworkRuleSetDefaultAction,\n"
        + "    ZoneRedundancy,\n"
        + "    DataEndpointEnabled,\n"
        + "    Encryption,\n"
        + "    EncryptionKey,\n"
        + "    QuarantinePolicy,\n"
        + "    TrustPolicy,\n"
        + "    RetentionPolicy,\n"
        + "    SecurityConfiguration,\n"
        + "    LoginServer = tostring(properties.loginServer),\n"
        + "    CreatedAt = tostring(properties.creationDate)\n"
        + "| order by Application, RegistryName";

    return executeKqlQuery(kql, subscriptionIds);
  }
}
