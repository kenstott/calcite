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
import org.apache.calcite.adapter.ops.CloudOpsDataConverter;
import org.apache.calcite.adapter.ops.util.CloudOpsCacheManager;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.AccessConfig;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.FirewallsSettings;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.InstancesScopedList;
import com.google.cloud.compute.v1.InstancesSettings;
import com.google.cloud.compute.v1.Network;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.NetworksClient;
import com.google.cloud.compute.v1.NetworksSettings;
import com.google.cloud.compute.v1.Subnetwork;
import com.google.cloud.compute.v1.SubnetworksClient;
import com.google.cloud.compute.v1.SubnetworksScopedList;
import com.google.cloud.compute.v1.SubnetworksSettings;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.container.v1.Cluster;
import com.google.container.v1.ListClustersResponse;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
/**
 * GCP provider implementation using Google Cloud SDK for Java, and Google's REST APIs for the
 * services the adapter has no client library for (IAM, Cloud SQL Admin, Artifact Registry).
 */
public class GCPProvider implements CloudProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCPProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int HTTP_ATTEMPTS = 3;

  private final CloudOpsConfig.GCPConfig config;
  private final GoogleCredentials credentials;
  private final CloudOpsCacheManager cacheManager;

  public GCPProvider(CloudOpsConfig.GCPConfig config) {
    this.config = config;
    this.cacheManager = new CloudOpsCacheManager(5, false);
    try {
      this.credentials =
          GoogleCredentials.fromStream(new FileInputStream(config.credentialsPath))
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize GCP credentials", e);
    }
  }

  public GCPProvider(CloudOpsConfig.GCPConfig config, CloudOpsCacheManager cacheManager) {
    this.config = config;
    this.cacheManager = cacheManager;
    try {
      this.credentials =
          GoogleCredentials.fromStream(new FileInputStream(config.credentialsPath))
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize GCP credentials", e);
    }
  }

  @Override public List<Map<String, Object>> queryKubernetesClusters(List<String> projectIds) {
    return queryKubernetesClusters(projectIds, null);
  }

  /**
   * Query Kubernetes clusters with projection support.
   * GCP Container Engine API supports partial projection via fields parameter.
   */
  public List<Map<String, Object>> queryKubernetesClusters(List<String> projectIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler) {
    return queryKubernetesClusters(projectIds, projectionHandler, null, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> projectIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler) {
    return queryKubernetesClusters(projectIds, projectionHandler, sortHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> projectIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return queryKubernetesClusters(projectIds, projectionHandler, sortHandler, paginationHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> projectIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                                          @Nullable CloudOpsFilterHandler filterHandler) {

    // Build comprehensive cache key including all optimization parameters
    String cacheKey =
        CloudOpsCacheManager.buildComprehensiveCacheKey("gcp", "kubernetes_clusters", projectionHandler, sortHandler, paginationHandler, filterHandler, projectIds);

    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);

    if (shouldCache) {
      return cacheManager.getOrCompute(
          cacheKey, () -> executeKubernetesClusterQuery(
          projectIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeKubernetesClusterQuery(
          projectIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeKubernetesClusterQuery(List<String> projectIds,
                                                                 @Nullable CloudOpsProjectionHandler projectionHandler,
                                                                 @Nullable CloudOpsSortHandler sortHandler,
                                                                 @Nullable CloudOpsPaginationHandler paginationHandler,
                                                                 @Nullable CloudOpsFilterHandler filterHandler) {
    List<Map<String, Object>> results = new ArrayList<>();

    // Extract filter parameters for GCP API optimization
    Map<String, Object> filterParams = new HashMap<>();
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      filterParams = filterHandler.getGCPFilterParameters();
    }

    if (LOGGER.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        LOGGER.debug("GCP GKE querying with projection: {}", metrics);

        String fieldsParam = projectionHandler.buildGcpFieldsParameter();
        if (fieldsParam != null) {
          LOGGER.debug("GCP fields parameter: {}", fieldsParam);
        } else {
          LOGGER.debug("GCP projection: Falling back to full query (no compatible fields)");
        }
      } else {
        LOGGER.debug("GCP GKE querying: SELECT * (all fields)");
      }

      // Debug pagination optimization
      if (paginationHandler != null && paginationHandler.hasPagination()) {
        CloudOpsPaginationHandler.PaginationStrategy strategy = paginationHandler.getGCPStrategy();
        LOGGER.debug("GCP pagination: {}", strategy);
        LOGGER.debug("GCP pageSize parameter: {}", paginationHandler.getGCPPageSize());

        if (paginationHandler.needsGCPMultiPageFetch()) {
          LOGGER.debug("GCP pagination: Multi-page fetch required for offset handling");
        }
      }

      // Debug filter optimization
      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, filterParams.size());
        LOGGER.debug("GCP GKE with filter parameters: {} -> {}", filterParams.keySet(), metrics);
      }
    }

    // Query GKE clusters using the Container API
    for (String projectId : projectIds) {
      try {
        // Create GKE client with credentials
        ClusterManagerSettings settings = ClusterManagerSettings.newBuilder()
            .setCredentialsProvider(() -> credentials)
            .build();

        try (ClusterManagerClient clusterClient = ClusterManagerClient.create(settings)) {
          // List clusters in all zones (use "-" for all zones)
          String parent = String.format("projects/%s/locations/-", projectId);
          ListClustersResponse response = clusterClient.listClusters(parent);

          for (Cluster cluster : response.getClustersList()) {
            Map<String, Object> clusterData = new HashMap<>();

            // Identity fields
            clusterData.put("CloudProvider", "gcp");
            clusterData.put("AccountId", projectId);
            clusterData.put("ClusterName", cluster.getName());
            clusterData.put(
                "Application", cluster.getResourceLabelsOrDefault("app",
                cluster.getResourceLabelsOrDefault("application", "Untagged/Orphaned")));
            clusterData.put("Location", cluster.getLocation());
            clusterData.put("ResourceGroup", null); // GCP doesn't have resource groups
            clusterData.put("ResourceId", cluster.getSelfLink());

            // Configuration facts
            clusterData.put("KubernetesVersion", cluster.getCurrentMasterVersion());
            // Calculate total node count from node pools
            int totalNodes = 0;
            for (int i = 0; i < cluster.getNodePoolsCount(); i++) {
              totalNodes += cluster.getNodePools(i).getInitialNodeCount();
            }
            clusterData.put("NodeCount", totalNodes);
            clusterData.put("NodePools", cluster.getNodePoolsCount());
            clusterData.put("MinNodes", null); // Would need to aggregate from node pools
            clusterData.put("MaxNodes", null); // Would need to aggregate from node pools

            // Security facts
            clusterData.put("RBACEnabled", !cluster.hasLegacyAbac() || !cluster.getLegacyAbac().getEnabled());
            clusterData.put("PrivateCluster", cluster.hasPrivateClusterConfig() &&
                cluster.getPrivateClusterConfig().getEnablePrivateNodes());
            clusterData.put("PublicEndpoint", !cluster.hasPrivateClusterConfig() ||
                !cluster.getPrivateClusterConfig().getEnablePrivateEndpoint());
            clusterData.put("AuthorizedIPRanges", cluster.hasMasterAuthorizedNetworksConfig() ?
                cluster.getMasterAuthorizedNetworksConfig().getCidrBlocksCount() : 0);

            // Network configuration
            clusterData.put("NetworkPolicyProvider", cluster.hasNetworkPolicy() ?
                cluster.getNetworkPolicy().getProvider().name() : null);

            // Encryption and logging
            // Google encrypts a cluster's storage at rest itself; a cluster may add
            // encryption of its secrets under a Cloud KMS key of the project
            clusterData.put("EncryptionAtRestEnabled", true);
            clusterData.put("EncryptionKeyType", cluster.hasDatabaseEncryption()
                && cluster.getDatabaseEncryption().getState().name().equals("ENCRYPTED")
                ? "customer-managed" : "service-managed");
            clusterData.put("LoggingEnabled", cluster.getLoggingService() != null &&
                !cluster.getLoggingService().equals("none"));
            clusterData.put("MonitoringEnabled", cluster.getMonitoringService() != null &&
                !cluster.getMonitoringService().equals("none"));

            // Timestamps
            clusterData.put("CreatedDate", cluster.getCreateTime());
            clusterData.put("ModifiedDate", null); // Not directly available

            // Tags
            clusterData.put("Tags", toJson(cluster.getResourceLabelsMap()));

            results.add(clusterData);
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying GKE clusters in project " + projectId
            + " failed: " + e.getMessage(), e);
      }
    }

    // Apply client-side pagination if needed (when actual implementation is added)
    if (paginationHandler != null) {
      results = paginationHandler.applyClientSidePagination(results);

      if (LOGGER.isDebugEnabled() && paginationHandler.hasPagination()) {
        CloudOpsPaginationHandler.PaginationMetrics metrics =
            paginationHandler.calculateMetrics(false, results.size());
        LOGGER.debug("GCP GKE pagination metrics: {}", metrics);
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryStorageResources(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String projectId : projectIds) {
      try {
        Storage storage = StorageOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();

        for (Bucket bucket : storage.list().iterateAll()) {
          Map<String, Object> storageData = new HashMap<>();

          // Identity fields
          storageData.put("ProjectId", projectId);
          storageData.put("StorageResource", bucket.getName());
          storageData.put("StorageType", "Cloud Storage Bucket");
          storageData.put("Location", bucket.getLocation());
          storageData.put("ResourceId", bucket.getSelfLink());

          // Labels
          Map<String, String> labels = bucket.getLabels();
          String application = labels != null ?
              labels.getOrDefault("application",
                  labels.getOrDefault("app", "Untagged/Orphaned")) :
              "Untagged/Orphaned";
          storageData.put("Application", application);
          storageData.put("Tags", toJson(labels));

          // Storage facts
          storageData.put("StorageClass", bucket.getStorageClass());
          storageData.put("LocationType", bucket.getLocationType());

          // Encryption facts - always enabled in GCS
          storageData.put("EncryptionEnabled", true);
          storageData.put("EncryptionKeyName", bucket.getDefaultKmsKeyName());

          // Access control
          if (bucket.getIamConfiguration() != null) {
            storageData.put("UniformBucketLevelAccess",
                bucket.getIamConfiguration().isUniformBucketLevelAccessEnabled() != null ?
                bucket.getIamConfiguration().isUniformBucketLevelAccessEnabled() : false);
            storageData.put("PublicAccessPrevention",
                bucket.getIamConfiguration().getPublicAccessPrevention() != null ?
                bucket.getIamConfiguration().getPublicAccessPrevention().toString() : null);
          }

          // Versioning
          storageData.put("VersioningEnabled",
              bucket.versioningEnabled() != null ? bucket.versioningEnabled() : false);

          // Lifecycle
          storageData.put("LifecycleRuleCount",
              bucket.getLifecycleRules() != null ? bucket.getLifecycleRules().size() : 0);

          // Retention - simplified
          storageData.put("RetentionPolicy", false);

          // Timestamps
          storageData.put("TimeCreated", bucket.getCreateTimeOffsetDateTime());
          storageData.put("Updated", bucket.getUpdateTimeOffsetDateTime());

          results.add(storageData);
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying storage resources in project " + projectId
            + " failed: " + e.getMessage(), e);
      }
    }

    return results;
  }

  /** Last path segment of a GCP resource URL (e.g. zone/machineType/region URLs). */
  private static String lastSegment(String url) {
    if (url == null || url.isEmpty()) {
      return url;
    }
    int slash = url.lastIndexOf('/');
    return slash >= 0 ? url.substring(slash + 1) : url;
  }

  @Override public List<Map<String, Object>> queryComputeInstances(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (String projectId : projectIds) {
      try {
        InstancesSettings settings = InstancesSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
        try (InstancesClient client = InstancesClient.create(settings)) {
          for (Map.Entry<String, InstancesScopedList> entry
              : client.aggregatedList(projectId).iterateAll()) {
            for (Instance instance : entry.getValue().getInstancesList()) {
              Map<String, Object> vm = new HashMap<>();
              vm.put("ProjectId", projectId);
              vm.put("VMName", instance.getName());
              vm.put("Zone", lastSegment(instance.getZone()));
              vm.put("ResourceId", instance.getSelfLink());
              vm.put("MachineType", lastSegment(instance.getMachineType()));
              vm.put("Status", instance.getStatus());
              vm.put("CpuPlatform", instance.getCpuPlatform());
              vm.put("CreationTimestamp", instance.getCreationTimestamp());

              Map<String, String> labels = instance.getLabelsMap();
              vm.put("Application", labels.getOrDefault("application",
                  labels.getOrDefault("app", "Untagged/Orphaned")));

              if (!instance.getNetworkInterfacesList().isEmpty()) {
                NetworkInterface nic = instance.getNetworkInterfaces(0);
                // network / subnetwork URLs equal the Network/Subnetwork selfLinks (native_id).
                vm.put("NetworkId", nic.getNetwork());
                vm.put("SubnetId", nic.getSubnetwork());
                vm.put("PrivateIp", nic.getNetworkIP().isEmpty() ? null : nic.getNetworkIP());
                for (AccessConfig accessConfig : nic.getAccessConfigsList()) {
                  if (!accessConfig.getNatIP().isEmpty()) {
                    vm.put("PublicIp", accessConfig.getNatIP());
                  }
                }
              }
              // The identity the instance runs as, and the network tags firewall rules target
              if (!instance.getServiceAccountsList().isEmpty()) {
                // The service account's resource name, which is iam_resources.resource_id
                vm.put("ServiceAccount", "projects/" + projectId + "/serviceAccounts/"
                    + instance.getServiceAccounts(0).getEmail());
              }
              if (!instance.getTags().getItemsList().isEmpty()) {
                vm.put("NetworkTags", String.join(",", instance.getTags().getItemsList()));
              }

              boolean diskEncryption = instance.getDisksList().stream()
                  .anyMatch(disk -> disk.hasDiskEncryptionKey());
              vm.put("DiskEncryption", diskEncryption ? "Enabled" : "Disabled");

              results.add(vm);
            }
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying GCP compute instances in project " + projectId
            + " failed: " + e.getMessage(), e);
      }
    }
    return results;
  }

  @Override public List<Map<String, Object>> queryNetworkResources(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (String projectId : projectIds) {
      // VPC networks
      try {
        NetworksSettings settings = NetworksSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
        try (NetworksClient client = NetworksClient.create(settings)) {
          for (Network network : client.list(projectId).iterateAll()) {
            Map<String, Object> row = new HashMap<>();
            row.put("ProjectId", projectId);
            row.put("NetworkResource", network.getName());
            row.put("NativeId", network.getSelfLink());
            row.put("NetworkResourceType", "VPC Network");
            row.put("ResourceId", network.getSelfLink());
            row.put("Application", "Untagged/Orphaned");
            row.put("Configuration",
                network.getAutoCreateSubnetworks() ? "Auto subnets" : "Custom subnets");
            // The network every project is created with
            row.put("IsDefault", "default".equals(network.getName()));
            results.add(row);
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying GCP networks in project " + projectId
            + " failed: " + e.getMessage(), e);
      }

      // Firewall rules: GCP's counterpart of a security group
      try {
        FirewallsSettings settings = FirewallsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
        try (FirewallsClient client = FirewallsClient.create(settings)) {
          for (Firewall firewall : client.list(projectId).iterateAll()) {
            final boolean ingress = !"EGRESS".equals(firewall.getDirection());
            Map<String, Object> row = new HashMap<>();
            row.put("ProjectId", projectId);
            row.put("NetworkResource", firewall.getName());
            row.put("NativeId", firewall.getSelfLink());
            row.put("NetworkResourceType", "Firewall Rule");
            row.put("ResourceId", firewall.getSelfLink());
            row.put("Application", "Untagged/Orphaned");
            row.put("Configuration", firewall.getDirection() + ", priority "
                + firewall.getPriority() + ", network " + lastSegment(firewall.getNetwork()));
            row.put("SourceRanges", ingress
                ? String.join(",", firewall.getSourceRangesList())
                : String.join(",", firewall.getDestinationRangesList()));
            row.put("State", firewall.getDisabled() ? "disabled" : "enabled");
            row.put("HasOpenIngress", ingress && !firewall.getDisabled()
                && !firewall.getAllowedList().isEmpty()
                && (firewall.getSourceRangesList().contains("0.0.0.0/0")
                    || firewall.getSourceRangesList().contains("::/0")));
            row.put("RuleCount",
                firewall.getAllowedList().size() + firewall.getDeniedList().size());
            results.add(row);
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying GCP firewall rules in project " + projectId
            + " failed: " + e.getMessage(), e);
      }

      // Subnetworks (across all regions)
      try {
        SubnetworksSettings settings = SubnetworksSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
        try (SubnetworksClient client = SubnetworksClient.create(settings)) {
          for (Map.Entry<String, SubnetworksScopedList> entry
              : client.aggregatedList(projectId).iterateAll()) {
            for (Subnetwork subnet : entry.getValue().getSubnetworksList()) {
              Map<String, Object> row = new HashMap<>();
              row.put("ProjectId", projectId);
              row.put("NetworkResource", subnet.getName());
              row.put("NativeId", subnet.getSelfLink());
              row.put("NetworkResourceType", "Subnet");
              row.put("ResourceId", subnet.getSelfLink());
              row.put("Location", lastSegment(subnet.getRegion()));
              row.put("Application", "Untagged/Orphaned");
              row.put("SourceRanges", subnet.getIpCidrRange());
              results.add(row);
            }
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Querying GCP subnetworks in project " + projectId
            + " failed: " + e.getMessage(), e);
      }
    }
    return results;
  }

  @Override public List<Map<String, Object>> queryIAMResources(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (String projectId : projectIds) {
      try {
        for (JsonNode account : listAll(
            "https://iam.googleapis.com/v1/projects/" + projectId + "/serviceAccounts",
            "accounts")) {
          Map<String, Object> row = new HashMap<>();
          row.put("ProjectId", projectId);
          row.put("IAMResource", account.path("email").asText());
          row.put("IAMResourceType", "ServiceAccount");
          row.put("PrincipalType", "ServiceAccount");
          row.put("Application", "Untagged/Orphaned"); // service accounts carry no labels
          row.put("Location", "global");
          row.put("ResourceId", account.path("name").asText());
          row.put("DisplayName", textOrNull(account, "displayName"));
          row.put("Description", textOrNull(account, "description"));
          row.put("Email", account.path("email").asText());
          row.put("Disabled", account.path("disabled").asBoolean(false));

          // User-managed keys are the long-lived credentials of a service account
          int keys = 0;
          int activeKeys = 0;
          final long now = System.currentTimeMillis();
          for (JsonNode key : getJson("https://iam.googleapis.com/v1/"
              + account.path("name").asText() + "/keys?keyTypes=USER_MANAGED").path("keys")) {
            keys++;
            final String expires = textOrNull(key, "validBeforeTime");
            final boolean expired =
                expires != null && java.time.Instant.parse(expires).toEpochMilli() <= now;
            if (!key.path("disabled").asBoolean(false) && !expired) {
              activeKeys++;
            }
          }
          row.put("AccessKeyCount", keys);
          row.put("ActiveAccessKeys", activeKeys);
          results.add(row);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Listing GCP service accounts of project " + projectId + " failed", e);
      }
    }
    return results;
  }

  @Override public List<Map<String, Object>> queryDatabaseResources(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (String projectId : projectIds) {
      try {
        for (JsonNode instance : listAll(
            "https://sqladmin.googleapis.com/v1/projects/" + projectId + "/instances", "items")) {
          final JsonNode settings = instance.path("settings");
          final JsonNode ip = settings.path("ipConfiguration");
          final JsonNode backup = settings.path("backupConfiguration");
          final Map<String, String> labels = stringMap(settings.path("userLabels"));

          Map<String, Object> row = new HashMap<>();
          row.put("ProjectId", projectId);
          row.put("DatabaseResource", instance.path("name").asText());
          row.put("DatabaseType", "Cloud SQL");
          row.put("Application", applicationOf(labels));
          row.put("Location", textOrNull(instance, "region"));
          row.put("ResourceId", textOrNull(instance, "selfLink"));
          // databaseVersion is e.g. POSTGRES_15, MYSQL_8_0, SQLSERVER_2019_STANDARD
          final String version = textOrNull(instance, "databaseVersion");
          if (version != null) {
            final int split = version.indexOf('_');
            row.put("Engine", split < 0 ? version : version.substring(0, split));
            row.put("EngineVersion", split < 0 ? null : version.substring(split + 1));
          }
          row.put("Tier", textOrNull(settings, "tier"));
          row.put("AllocatedStorageGb",
              settings.has("dataDiskSizeGb") ? settings.path("dataDiskSizeGb").asLong() : null);
          row.put("MultiZone", "REGIONAL".equals(textOrNull(settings, "availabilityType")));
          row.put("State", textOrNull(instance, "state"));
          row.put("PubliclyAccessible",
              ip.has("ipv4Enabled") ? ip.path("ipv4Enabled").asBoolean() : null);
          // Encrypted at rest always; the key is Google's unless a KMS key is named
          row.put("Encrypted", true);
          row.put("KmsKeyName",
              textOrNull(instance.path("diskEncryptionConfiguration"), "kmsKeyName"));
          final String sslMode = textOrNull(ip, "sslMode");
          row.put("RequireSsl", sslMode != null
              ? !"ALLOW_UNENCRYPTED_AND_ENCRYPTED".equals(sslMode)
              : ip.has("requireSsl") ? ip.path("requireSsl").asBoolean() : null);
          row.put("BackupEnabled",
              backup.has("enabled") ? backup.path("enabled").asBoolean() : null);
          final JsonNode retained = backup.path("backupRetentionSettings").path("retainedBackups");
          row.put("RetainedBackups", retained.isNumber() ? retained.asInt() : null);
          row.put("BackupStartTime", textOrNull(backup, "startTime"));
          row.put("CreateTime", textOrNull(instance, "createTime"));
          results.add(row);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Listing Cloud SQL instances of project " + projectId + " failed", e);
      }
    }
    return results;
  }

  @Override public List<Map<String, Object>> queryContainerRegistries(List<String> projectIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (final String projectId : projectIds) {
      try {
        // Artifact Registry has no "all locations" listing: discover them, then ask each
        final String base = "https://artifactregistry.googleapis.com/v1/projects/" + projectId;
        final List<String> locations = new ArrayList<>();
        for (JsonNode location : listAll(base + "/locations", "locations")) {
          locations.add(location.path("locationId").asText());
        }
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
          final List<Future<List<JsonNode>>> pending = new ArrayList<>();
          for (final String location : locations) {
            pending.add(executor.submit(new Callable<List<JsonNode>>() {
              @Override public List<JsonNode> call() throws IOException {
                return listAll(base + "/locations/" + location + "/repositories",
                    "repositories");
              }
            }));
          }
          for (int i = 0; i < pending.size(); i++) {
            for (JsonNode repository : pending.get(i).get()) {
              results.add(registryRow(projectId, locations.get(i), repository));
            }
          }
        } finally {
          executor.shutdownNow();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Listing Artifact Registry repositories of project " + projectId + " failed", e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(
            "Listing Artifact Registry repositories of project " + projectId + " failed",
            e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted listing Artifact Registry repositories", e);
      }
    }
    return results;
  }

  private static Map<String, Object> registryRow(String projectId, String location,
      JsonNode repository) {
    final String name = lastSegment(repository.path("name").asText());
    final String format = textOrNull(repository, "format");
    final Map<String, String> labels = stringMap(repository.path("labels"));
    final String kmsKey = textOrNull(repository, "kmsKeyName");

    Map<String, Object> row = new HashMap<>();
    row.put("ProjectId", projectId);
    row.put("RegistryName", name);
    row.put("Application", applicationOf(labels));
    row.put("Location", location);
    row.put("ResourceId", repository.path("name").asText());
    // Only Docker repositories are addressed by a registry host
    row.put("RegistryUri", "DOCKER".equals(format)
        ? location + "-docker.pkg.dev/" + projectId + "/" + name : null);
    row.put("Format", format);
    row.put("Mode", textOrNull(repository, "mode"));
    final String scanning =
        textOrNull(repository.path("vulnerabilityScanningConfig"), "enablementState");
    row.put("ScanningEnabled", scanning == null ? null : "SCANNING_ACTIVE".equals(scanning));
    row.put("ImmutableTags", "DOCKER".equals(format)
        ? repository.path("dockerConfig").path("immutableTags").asBoolean(false) : null);
    row.put("Encryption", kmsKey != null ? "customer-managed" : "google-managed");
    row.put("KmsKey", kmsKey);
    row.put("CleanupPoliciesCount", repository.path("cleanupPolicies").size());
    row.put("CreateTime", textOrNull(repository, "createTime"));
    row.put("Tags", toJson(labels));
    return row;
  }

  /** The application a resource belongs to, from its labels. */
  private static String applicationOf(Map<String, String> labels) {
    String application = labels.get("application");
    if (application == null) {
      application = labels.get("app");
    }
    return application != null ? application : "Untagged/Orphaned";
  }

  /** Labels as a JSON object, or null when there are none. */
  static String toJson(Map<String, String> labels) {
    return CloudOpsDataConverter.tagsToJson(labels);
  }

  private static String textOrNull(JsonNode node, String field) {
    final JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }

  private static Map<String, String> stringMap(JsonNode object) {
    final Map<String, String> map = new HashMap<>();
    final java.util.Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      map.put(field.getKey(), field.getValue().asText());
    }
    return map;
  }

  /**
   * GETs a Google REST API with the adapter's credentials. Used for the services the adapter
   * has no client library for (IAM, Cloud SQL Admin, Artifact Registry).
   */
  private JsonNode getJson(String url) throws IOException {
    // These are idempotent reads, so a timeout or a "try again" status is retried; the last
    // failure is what the caller sees
    IOException last = null;
    for (int attempt = 1; attempt <= HTTP_ATTEMPTS; attempt++) {
      if (attempt > 1) {
        try {
          Thread.sleep(500L * attempt);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while retrying GET " + url, e);
        }
      }
      try {
        return getJsonOnce(url);
      } catch (RetryableHttpException | java.net.SocketTimeoutException e) {
        last = e;
      }
    }
    throw new IOException("GET " + url + " failed after " + HTTP_ATTEMPTS + " attempts: "
        + last.getMessage(), last);
  }

  private JsonNode getJsonOnce(String url) throws IOException {
    final String token;
    synchronized (credentials) {
      credentials.refreshIfExpired();
      token = credentials.getAccessToken().getTokenValue();
    }
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    try {
      connection.setRequestProperty("Authorization", "Bearer " + token);
      connection.setRequestProperty("Accept", "application/json");
      connection.setConnectTimeout(15000);
      connection.setReadTimeout(20000);
      final int status = connection.getResponseCode();
      final InputStream stream =
          status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final JsonNode body;
      try {
        body = stream == null ? MAPPER.createObjectNode() : MAPPER.readTree(stream);
      } finally {
        if (stream != null) {
          stream.close();
        }
      }
      if (status == 200) {
        return body;
      }
      final String message = "GET " + url + " returned " + status + ": "
          + body.path("error").path("message").asText();
      if (status == 429 || status >= 500) {
        throw new RetryableHttpException(message);
      }
      throw new IOException(message);
    } finally {
      connection.disconnect();
    }
  }

  /** A response that asks for the request to be tried again (429 or 5xx). */
  private static class RetryableHttpException extends IOException {
    RetryableHttpException(String message) {
      super(message);
    }
  }

  /** Follows {@code nextPageToken} and returns every item of a list call. */
  private List<JsonNode> listAll(String url, String itemsField) throws IOException {
    final List<JsonNode> items = new ArrayList<>();
    String pageToken = null;
    do {
      final String pageUrl = pageToken == null ? url
          : url + (url.indexOf('?') < 0 ? '?' : '&') + "pageToken="
              + URLEncoder.encode(pageToken, "UTF-8");
      final JsonNode page = getJson(pageUrl);
      for (JsonNode item : page.path(itemsField)) {
        items.add(item);
      }
      pageToken = textOrNull(page, "nextPageToken");
    } while (pageToken != null && !pageToken.isEmpty());
    return items;
  }

  /**
   * Get cache metrics for monitoring.
   */
  public CloudOpsCacheManager.CacheMetrics getCacheMetrics() {
    return cacheManager.getCacheMetrics();
  }

  /**
   * Invalidate cache entries for a specific project.
   */
  public void invalidateProjectCache(String projectId) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated GCP cache for project: {}", projectId);
    }
  }

  /**
   * Invalidate cache entries for a specific region.
   */
  public void invalidateRegionCache(String region) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated GCP cache for region: {}", region);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAllCache() {
    cacheManager.invalidateAll();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Invalidated all GCP cache entries");
    }
  }
}
