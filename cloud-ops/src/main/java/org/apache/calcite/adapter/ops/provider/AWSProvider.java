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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoveryDescription;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoveryStatus;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.Repository;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.Cluster;
import software.amazon.awssdk.services.eks.model.DescribeClusterRequest;
import software.amazon.awssdk.services.eks.model.VpcConfigResponse;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CacheCluster;
import software.amazon.awssdk.services.elasticache.model.MultiAZStatus;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * AWS provider implementation using AWS SDK for Java v2.
 */
public class AWSProvider implements CloudProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSProvider.class);

  private final CloudOpsConfig.AWSConfig config;
  /** The regions named in the configuration; empty means every region enabled for an account. */
  private final List<Region> configuredRegions;
  /** Where the calls that have no region of their own are made: STS, region and bucket listing. */
  private final Region homeRegion;
  private final Map<String, List<Region>> enabledRegions = new ConcurrentHashMap<>();
  private final AwsCredentials baseCredentials;
  private final Map<String, AwsCredentials> accountCredentials;
  private final CloudOpsCacheManager cacheManager;

  public AWSProvider(CloudOpsConfig.AWSConfig config) {
    this.config = config;
    this.configuredRegions = regionsIn(config.region);
    this.homeRegion = configuredRegions.isEmpty() ? Region.US_EAST_1 : configuredRegions.get(0);
    this.baseCredentials =
        AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey);
    this.accountCredentials = new ConcurrentHashMap<>();
    this.cacheManager = new CloudOpsCacheManager(5, false);
    initializeAccountCredentials();
  }

  public AWSProvider(CloudOpsConfig.AWSConfig config, CloudOpsCacheManager cacheManager) {
    this.config = config;
    this.configuredRegions = regionsIn(config.region);
    this.homeRegion = configuredRegions.isEmpty() ? Region.US_EAST_1 : configuredRegions.get(0);
    this.baseCredentials =
        AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey);
    this.accountCredentials = new ConcurrentHashMap<>();
    this.cacheManager = cacheManager;
    initializeAccountCredentials();
  }

  /**
   * Regional queries of all accounts share these threads; a query is mostly network waiting.
   * Kept small: opening connections to every region for several queries at once has been
   * seen to end in refused and reset connections.
   */
  private static final ExecutorService REGION_POOL =
      Executors.newFixedThreadPool(6, runnable -> {
        Thread thread = new Thread(runnable, "cloudops-aws-region");
        thread.setDaemon(true);
        return thread;
      });

  /** One regional query: the rows of one account in one region. */
  private interface RegionQuery {
    List<Map<String, Object>> run(String accountId, AwsCredentials credentials, Region region,
        Clients clients);
  }

  /** The clients a regional query opened, closed together when it ends. */
  private static final class Clients implements AutoCloseable {
    private final List<SdkAutoCloseable> opened = new ArrayList<>();

    <C extends SdkAutoCloseable> C add(C client) {
      opened.add(client);
      return client;
    }

    @Override public void close() {
      for (SdkAutoCloseable client : opened) {
        client.close();
      }
    }
  }

  /**
   * Parses the {@code region} setting: one region, several separated by commas, or — when it is
   * absent or {@code all} — none, which stands for every region enabled for an account.
   */
  static List<Region> regionsIn(@Nullable String setting) {
    List<Region> regions = new ArrayList<>();
    if (setting == null || setting.trim().isEmpty() || "all".equalsIgnoreCase(setting.trim())) {
      return regions;
    }
    for (String name : setting.split(",", -1)) {
      if (name.trim().isEmpty()) {
        throw new IllegalArgumentException("Empty region name in AWS region setting: " + setting);
      }
      regions.add(Region.of(name.trim()));
    }
    return regions;
  }

  /** The regions to query for one account. */
  private List<Region> regionsOf(String accountId, AwsCredentials credentials) {
    if (!configuredRegions.isEmpty()) {
      return configuredRegions;
    }
    return enabledRegions.computeIfAbsent(accountId, id -> {
      try (Ec2Client ec2Client = Ec2Client.builder()
          .region(homeRegion)
          .credentialsProvider(StaticCredentialsProvider.create(credentials))
          .build()) {
        List<Region> regions = new ArrayList<>();
        // Without the all-regions flag this lists the regions the account can use
        for (software.amazon.awssdk.services.ec2.model.Region enabled
            : ec2Client.describeRegions().regions()) {
          regions.add(Region.of(enabled.regionName()));
        }
        return regions;
      } catch (RuntimeException e) {
        throw new IllegalStateException("Listing the regions enabled for AWS account " + id
            + " failed: " + e.getMessage(), e);
      }
    });
  }

  /**
   * Runs a regional query in every region of every given account, side by side, and
   * concatenates the rows. An account this provider has no credentials for is another
   * provider's (an account filter is handed to all of them) and contributes nothing.
   */
  private List<Map<String, Object>> inEveryRegion(List<String> accountIds, String what,
      RegionQuery query) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) {
        continue;
      }
      List<Region> regions = regionsOf(accountId, credentials);
      List<CompletableFuture<List<Map<String, Object>>>> pending = new ArrayList<>();
      for (Region region : regions) {
        pending.add(
            CompletableFuture.supplyAsync(() -> {
              try (Clients clients = new Clients()) {
                return query.run(accountId, credentials, region, clients);
              }
            }, REGION_POOL));
      }
      for (int i = 0; i < regions.size(); i++) {
        try {
          results.addAll(pending.get(i).join());
        } catch (CompletionException e) {
          Throwable cause = e.getCause() == null ? e : e.getCause();
          throw new IllegalStateException("Querying " + what + " in AWS account " + accountId
              + ", region " + regions.get(i) + " failed: " + cause.getMessage(), cause);
        }
      }
    }
    return results;
  }

  /**
   * Whether a bucket policy has a Deny statement conditioned on
   * {@code aws:SecureTransport} being false, which is how a bucket is made HTTPS-only.
   */
  static boolean deniesInsecureTransport(String policyJson) {
    final JsonNode statements;
    try {
      statements = POLICY_READER.readTree(policyJson).path("Statement");
    } catch (IOException e) {
      throw new IllegalStateException("Bucket policy is not JSON: " + e.getMessage(), e);
    }
    // A policy holds one statement or a list of them
    for (JsonNode statement : statements.isArray()
        ? statements : Collections.singletonList(statements)) {
      if (!"Deny".equals(statement.path("Effect").asText())) {
        continue;
      }
      JsonNode secure = statement.path("Condition").path("Bool").path("aws:SecureTransport");
      for (JsonNode value : secure.isArray() ? secure : Collections.singletonList(secure)) {
        if ("false".equalsIgnoreCase(value.asText())) {
          return true;
        }
      }
    }
    return false;
  }

  private static final ObjectMapper POLICY_READER = new ObjectMapper();

  /** Whether an AWS error is one of the given error codes ("there is none" answers). */
  private static boolean isAwsError(AwsServiceException e, String... codes) {
    final String code = e.awsErrorDetails() == null ? null : e.awsErrorDetails().errorCode();
    for (String candidate : codes) {
      if (candidate.equals(code)) {
        return true;
      }
    }
    return false;
  }

  /** The application a resource belongs to, from its tags. */
  private static String applicationOf(Map<String, String> tags) {
    for (String key : new String[] {"Application", "application", "app"}) {
      if (tags.containsKey(key)) {
        return tags.get(key);
      }
    }
    return "Untagged/Orphaned";
  }

  private void initializeAccountCredentials() {
    if (config.roleArn != null && !config.roleArn.isEmpty()) {
      // If using cross-account role assumption
      StsClient stsClient = StsClient.builder()
          .region(homeRegion)
          .credentialsProvider(StaticCredentialsProvider.create(baseCredentials))
          .build();

      for (String accountId : config.accountIds) {
        String roleArn = config.roleArn.replace("{account-id}", accountId);
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName("cloud-governance-adapter")
            .build();

        try {
          AssumeRoleResponse response = stsClient.assumeRole(assumeRoleRequest);
          // Temporary credentials are rejected without their session token
          AwsCredentials assumedCredentials =
              AwsSessionCredentials.create(response.credentials().accessKeyId(),
                  response.credentials().secretAccessKey(),
                  response.credentials().sessionToken());
          accountCredentials.put(accountId, assumedCredentials);
        } catch (RuntimeException e) {
          // Falling back to the base credentials would read the base account and label
          // its resources with this account's id
          throw new IllegalStateException("Assuming role " + roleArn + " for AWS account "
              + accountId + " failed: " + e.getMessage(), e);
        }
      }
    } else {
      // One key reads one account: with several, every account's rows would be the same
      // resources under different account ids
      if (config.accountIds.size() > 1) {
        throw new IllegalStateException("AWS accounts " + config.accountIds
            + " are configured without aws.roleArn; a key reads only its own account");
      }
      for (String accountId : config.accountIds) {
        accountCredentials.put(accountId, baseCredentials);
      }
    }
  }

  @Override public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds) {
    return queryKubernetesClusters(accountIds, null);
  }

  /**
   * Query Kubernetes clusters with projection support.
   * AWS EKS doesn't support server-side projection, so we apply client-side projection.
   */
  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, sortHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, sortHandler, paginationHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                                          @Nullable CloudOpsFilterHandler filterHandler) {

    // Build comprehensive cache key including all optimization parameters
    String cacheKey =
        CloudOpsCacheManager.buildComprehensiveCacheKey("aws", "kubernetes_clusters", projectionHandler, sortHandler, paginationHandler, filterHandler, accountIds);

    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);

    if (shouldCache) {
      return cacheManager.getOrCompute(
          cacheKey, () -> executeKubernetesClusterQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeKubernetesClusterQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeKubernetesClusterQuery(List<String> accountIds,
                                                                 @Nullable CloudOpsProjectionHandler projectionHandler,
                                                                 @Nullable CloudOpsSortHandler sortHandler,
                                                                 @Nullable CloudOpsPaginationHandler paginationHandler,
                                                                 @Nullable CloudOpsFilterHandler filterHandler) {
    List<Map<String, Object>> results = new ArrayList<>();

    // Extract filter parameters for AWS API optimization
    Map<String, Object> filterParams = new HashMap<>();
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      filterParams = filterHandler.getAWSFilterParameters();
    }

    if (LOGGER.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        LOGGER.debug("AWS EKS querying with client-side projection: {}", metrics);
      }

      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, filterParams.size());
        LOGGER.debug("AWS EKS with filter parameters: {} -> {}", filterParams.keySet(), metrics);
      }
    }

    // A filter on the region narrows the regions asked
    final String onlyRegion =
        filterParams.containsKey("region") ? filterParams.get("region").toString() : null;
    // AWS cannot sort: stopping the listing early would drop rows a requested order needs
    final boolean stopEarly = paginationHandler != null && paginationHandler.hasPagination()
        && (sortHandler == null || !sortHandler.hasSort());

    results.addAll(
        inEveryRegion(accountIds, "EKS clusters", (accountId, credentials, region, clients) -> {
      List<Map<String, Object>> found = new ArrayList<>();
      if (onlyRegion != null && !onlyRegion.equals(region.id())) {
        return found;
      }

      EksClient eksClient = clients.add(EksClient.builder()
          .region(region)
          .credentialsProvider(StaticCredentialsProvider.create(credentials))
          .build());

      // List clusters with pagination support
      List<String> clusterNames = new ArrayList<>();
      String nextToken = null;
      int maxResults = (paginationHandler != null) ?
          paginationHandler.getAWSMaxResults() : 100; // Default AWS page size

      do {
        software.amazon.awssdk.services.eks.model.ListClustersRequest.Builder requestBuilder =
            software.amazon.awssdk.services.eks.model.ListClustersRequest.builder()
                .maxResults(maxResults);

        if (nextToken != null) {
          requestBuilder.nextToken(nextToken);
        }

        software.amazon.awssdk.services.eks.model.ListClustersResponse response =
            eksClient.listClusters(requestBuilder.build());

        clusterNames.addAll(response.clusters());
        nextToken = response.nextToken();

        // Stop if we have enough results for pagination
        if (stopEarly) {
          long totalNeeded = paginationHandler.getOffset() + paginationHandler.getLimit();
          if (clusterNames.size() >= totalNeeded) {
            break;
          }
        }

      } while (nextToken != null);

      for (String clusterName : clusterNames) {
        DescribeClusterRequest describeRequest = DescribeClusterRequest.builder()
            .name(clusterName)
            .build();

        Cluster cluster = eksClient.describeCluster(describeRequest).cluster();

        // Build full cluster data (no server-side projection available)
        Map<String, Object> clusterData = buildClusterData(accountId, region, cluster);

        // Node groups are resources of their own: count them and total their desired sizes
        int nodeGroups = 0;
        int nodes = 0;
        for (String nodegroup
            : eksClient.listNodegroupsPaginator(r -> r.clusterName(clusterName)).nodegroups()) {
          nodeGroups++;
          software.amazon.awssdk.services.eks.model.NodegroupScalingConfig scaling =
              eksClient.describeNodegroup(
                  r -> r.clusterName(clusterName).nodegroupName(nodegroup))
                  .nodegroup().scalingConfig();
          if (scaling != null && scaling.desiredSize() != null) {
            nodes += scaling.desiredSize();
          }
        }
        clusterData.put("NodeGroupCount", nodeGroups);

        // Metrics of a cluster are collected by the CloudWatch Observability add-on
        boolean observability = false;
        for (String addon
            : eksClient.listAddonsPaginator(r -> r.clusterName(clusterName)).addons()) {
          observability |= "amazon-cloudwatch-observability".equals(addon);
        }
        clusterData.put("MonitoringEnabled", observability);
        clusterData.put("NodeCount", nodes);

        // Apply client-side filtering for non-pushable filters
        if (filterHandler == null || passesClientSideFilters(clusterData, filterHandler)) {
          found.add(clusterData);
        }
      }
      return found;
    }));

    // Apply client-side pagination if needed
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      List<Map<String, Object>> paginatedResults = paginationHandler.applyClientSidePagination(results);

      if (LOGGER.isDebugEnabled()) {
        CloudOpsPaginationHandler.PaginationMetrics metrics =
            paginationHandler.calculateMetrics(false, results.size());
        LOGGER.debug("AWS EKS with pagination optimization: {}", metrics);

        CloudOpsPaginationHandler.PaginationStrategy strategy = paginationHandler.getAWSStrategy();
        LOGGER.debug("AWS pagination strategy: {}", strategy);
      }

      return paginatedResults;
    }

    return results;
  }

  /**
   * Build complete cluster data from AWS EKS Cluster object.
   */
  private Map<String, Object> buildClusterData(String accountId, Region region, Cluster cluster) {
    Map<String, Object> clusterData = new HashMap<>();

    // Identity fields
    clusterData.put("AccountId", accountId);
    clusterData.put("ClusterName", cluster.name());
    clusterData.put("Region", region.toString());
    clusterData.put("ResourceId", cluster.arn());

    // Tags
    Map<String, String> tags = cluster.tags();
    String application =
        applicationOf(tags);
    clusterData.put("Application", application);
    clusterData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));

    // Configuration facts
    clusterData.put("ClusterVersion", cluster.version());
    clusterData.put("PlatformVersion", cluster.platformVersion());
    clusterData.put("Status", cluster.status());

    // Security facts
    clusterData.put("RBACEnabled", true); // EKS has RBAC enabled by default

    // Network configuration: every cluster has one
    VpcConfigResponse network = cluster.resourcesVpcConfig();
    clusterData.put("EndpointPublicAccess", network.endpointPublicAccess());
    clusterData.put("PrivateCluster", !network.endpointPublicAccess());
    clusterData.put("PublicAccessCidrs", network.publicAccessCidrs().size());

    // EKS encrypts the volumes of its control plane itself; a cluster may add envelope
    // encryption of its secrets under a KMS key of the account
    clusterData.put("EncryptionEnabled", true);
    clusterData.put("EncryptionKeyType",
        cluster.hasEncryptionConfig() && !cluster.encryptionConfig().isEmpty()
            ? "Customer Managed Key" : "AWS Managed Key");

    // Control-plane logging: on when any log type is sent to CloudWatch
    boolean loggingEnabled = false;
    if (cluster.logging() != null) {
      for (software.amazon.awssdk.services.eks.model.LogSetup setup
          : cluster.logging().clusterLogging()) {
        loggingEnabled |= Boolean.TRUE.equals(setup.enabled());
      }
    }
    clusterData.put("LoggingEnabled", loggingEnabled);

    // Timestamps
    clusterData.put("CreatedAt", cluster.createdAt());

    return clusterData;
  }

  @Override public List<Map<String, Object>> queryStorageResources(List<String> accountIds) {
    return queryStorageResources(accountIds, null, null, null, null);
  }

  /**
   * Query storage resources with projection-aware optimization.
   * Only fetches expensive API details when those columns are actually projected.
   */
  public List<Map<String, Object>> queryStorageResources(List<String> accountIds,
                                                         @Nullable CloudOpsProjectionHandler projectionHandler,
                                                         @Nullable CloudOpsSortHandler sortHandler,
                                                         @Nullable CloudOpsPaginationHandler paginationHandler,
                                                         @Nullable CloudOpsFilterHandler filterHandler) {

    // Build comprehensive cache key including all optimization parameters
    String cacheKey =
        CloudOpsCacheManager.buildComprehensiveCacheKey("aws", "storage_resources", projectionHandler, sortHandler, paginationHandler, filterHandler, accountIds);

    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);

    if (shouldCache) {
      return cacheManager.getOrCompute(
          cacheKey, () -> executeStorageResourceQuery(
          accountIds, projectionHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeStorageResourceQuery(
          accountIds, projectionHandler);
    }
  }

  private List<Map<String, Object>> executeStorageResourceQuery(List<String> accountIds,
                                                               @Nullable CloudOpsProjectionHandler projectionHandler) {
    List<Map<String, Object>> results = new ArrayList<>();

    // Determine which fields are needed based on projection
    boolean needsBasicInfo = true; // Always need basic info
    boolean needsLocation = isFieldProjected(projectionHandler, "region", "Location");
    boolean needsTags = isFieldProjected(projectionHandler, "application", "Application", "tags");
    boolean needsEncryption =
                                              isFieldProjected(projectionHandler, "encryption_enabled", "encryption_type", "encryption_key_type", "EncryptionEnabled", "EncryptionType", "KmsKeyId");
    boolean needsPublicAccess =
                                                isFieldProjected(projectionHandler, "public_access_enabled", "public_access_level", "PublicAccessBlocked");
    boolean needsHttpsOnly = isFieldProjected(projectionHandler, "https_only");
    boolean needsVersioning = isFieldProjected(projectionHandler, "versioning_enabled", "VersioningEnabled");
    boolean needsLifecycle = isFieldProjected(projectionHandler, "lifecycle_rules_count", "LifecycleRuleCount");
    boolean needsReplication = isFieldProjected(projectionHandler, "replication_type");
    boolean needsSizeMetrics = isFieldProjected(projectionHandler, "size_bytes", "SizeBytes", "size_gb", "SizeGB");

    // Log optimization decisions
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("AWS S3 query optimization - Fetching: basic={}, location={}, tags={}, encryption={}, " +
                   "publicAccess={}, versioning={}, lifecycle={}, sizeMetrics={}",
                   needsBasicInfo, needsLocation, needsTags, needsEncryption,
                   needsPublicAccess, needsVersioning, needsLifecycle, needsSizeMetrics);
    }

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        // Buckets are listed globally but configured per region: let the client follow a
        // bucket to the region it lives in
        S3Client s3Client = S3Client.builder()
            .region(homeRegion)
            .crossRegionAccessEnabled(true)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // List buckets with pagination support
        ListBucketsRequest listRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listRequest);

        List<Bucket> buckets = listBucketsResponse.buckets();

        for (Bucket bucket : buckets) {

          Map<String, Object> storageData = new HashMap<>();

          // Always include basic identity fields (low cost)
          storageData.put("AccountId", accountId);
          storageData.put("StorageResource", bucket.name());
          storageData.put("StorageType", "S3 Bucket");
          storageData.put("ResourceId", "arn:aws:s3:::" + bucket.name());
          storageData.put("CreationDate", bucket.creationDate());

          try {
            // Only fetch location if needed
            if (needsLocation || needsSizeMetrics) {
              GetBucketLocationRequest locationRequest = GetBucketLocationRequest.builder()
                  .bucket(bucket.name())
                  .build();
              String location = s3Client.getBucketLocation(locationRequest).locationConstraintAsString();
              // us-east-1 is reported as no location constraint at all
              storageData.put("Location",
                  location != null && !location.isEmpty() ? location : "us-east-1");
            } else {
              storageData.put("Location", null);
            }

            // Only fetch tags if needed
            if (needsTags) {
              GetBucketTaggingRequest taggingRequest = GetBucketTaggingRequest.builder()
                  .bucket(bucket.name())
                  .build();
              Map<String, String> tags = new HashMap<>();
              try {
                GetBucketTaggingResponse taggingResponse = s3Client.getBucketTagging(taggingRequest);
                taggingResponse.tagSet().forEach(tag -> tags.put(tag.key(), tag.value()));
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "NoSuchTagSet")) {
                  throw e;
                }
              }
              storageData.put("Application", applicationOf(tags));
              storageData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));
            } else {
              storageData.put("Application", null);
            }

            // Only fetch encryption if needed
            if (needsEncryption) {
              GetBucketEncryptionRequest encryptionRequest = GetBucketEncryptionRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetBucketEncryptionResponse encryptionResponse = s3Client.getBucketEncryption(encryptionRequest);
                storageData.put("EncryptionEnabled", true);
                if (encryptionResponse.serverSideEncryptionConfiguration() != null &&
                    !encryptionResponse.serverSideEncryptionConfiguration().rules().isEmpty()) {
                  ServerSideEncryptionRule rule = encryptionResponse.serverSideEncryptionConfiguration().rules().get(0);
                  storageData.put("EncryptionType", rule.applyServerSideEncryptionByDefault().sseAlgorithmAsString());
                  storageData.put("KmsKeyId", rule.applyServerSideEncryptionByDefault().kmsMasterKeyID());
                }
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "ServerSideEncryptionConfigurationNotFoundError")) {
                  throw e;
                }
                storageData.put("EncryptionEnabled", false);
              }
            } else {
              storageData.put("EncryptionEnabled", null);
              storageData.put("EncryptionType", null);
              storageData.put("KmsKeyId", null);
            }

            // Only fetch public access block if needed
            if (needsPublicAccess) {
              GetPublicAccessBlockRequest publicAccessRequest = GetPublicAccessBlockRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetPublicAccessBlockResponse publicAccessResponse = s3Client.getPublicAccessBlock(publicAccessRequest);
                PublicAccessBlockConfiguration config = publicAccessResponse.publicAccessBlockConfiguration();
                storageData.put("PublicAccessBlocked",
                    config.blockPublicAcls() && config.blockPublicPolicy() &&
                    config.ignorePublicAcls() && config.restrictPublicBuckets());
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "NoSuchPublicAccessBlockConfiguration")) {
                  throw e;
                }
                storageData.put("PublicAccessBlocked", false);
              }
            } else {
              storageData.put("PublicAccessBlocked", null);
            }

            // S3 answers plain HTTP unless the bucket policy denies insecure transport
            if (needsHttpsOnly) {
              try {
                storageData.put("HttpsOnly", deniesInsecureTransport(
                    s3Client.getBucketPolicy(r -> r.bucket(bucket.name())).policy()));
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "NoSuchBucketPolicy")) {
                  throw e;
                }
                storageData.put("HttpsOnly", false);
              }
            }

            // Only fetch versioning if needed
            if (needsVersioning) {
              GetBucketVersioningRequest versioningRequest = GetBucketVersioningRequest.builder()
                  .bucket(bucket.name())
                  .build();
              GetBucketVersioningResponse versioningResponse = s3Client.getBucketVersioning(versioningRequest);
              storageData.put("VersioningEnabled",
                  BucketVersioningStatus.ENABLED == versioningResponse.status());
            } else {
              storageData.put("VersioningEnabled", null);
            }

            if (needsReplication) {
              try {
                int rules = s3Client.getBucketReplication(r -> r.bucket(bucket.name()))
                    .replicationConfiguration().rules().size();
                storageData.put("Replication", "replicated (" + rules + " rules)");
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "ReplicationConfigurationNotFoundError")) {
                  throw e;
                }
                storageData.put("Replication", "none");
              }
            }

            // Only fetch lifecycle if needed
            if (needsLifecycle) {
              GetBucketLifecycleConfigurationRequest lifecycleRequest =
                  GetBucketLifecycleConfigurationRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetBucketLifecycleConfigurationResponse lifecycleResponse =
                    s3Client.getBucketLifecycleConfiguration(lifecycleRequest);
                storageData.put("LifecycleRuleCount",
                    lifecycleResponse.rules() != null ? lifecycleResponse.rules().size() : 0);
              } catch (AwsServiceException e) {
                if (!isAwsError(e, "NoSuchLifecycleConfiguration")) {
                  throw e;
                }
                storageData.put("LifecycleRuleCount", 0);
              }
            } else {
              storageData.put("LifecycleRuleCount", null);
            }

            // Only fetch size metrics if needed (expensive CloudWatch API call)
            if (needsSizeMetrics) {
              Long sizeBytes = fetchBucketSizeFromCloudWatch(accountId, bucket.name(),
                  Region.of(String.valueOf(storageData.get("Location"))), credentials);
              storageData.put("SizeBytes", sizeBytes);
              if (sizeBytes != null) {
                storageData.put("SizeGB", sizeBytes / (1024.0 * 1024.0 * 1024.0));
              } else {
                storageData.put("SizeGB", null);
              }
            } else {
              storageData.put("SizeBytes", null);
              storageData.put("SizeGB", null);
            }

          } catch (RuntimeException e) {
            throw new IllegalStateException("Reading the configuration of bucket "
                + bucket.name() + " failed: " + e.getMessage(), e);
          }

          results.add(storageData);
        }
      } catch (RuntimeException e) {
        throw new IllegalStateException("Querying S3 buckets in AWS account " + accountId
            + " failed: " + e.getMessage(), e);
      }
    }

    // Log performance metrics
    if (LOGGER.isDebugEnabled()) {
      int totalApiCalls = results.size() *
          (1 + (needsLocation ? 1 : 0) + (needsTags ? 1 : 0) + (needsEncryption ? 1 : 0) +
           (needsPublicAccess ? 1 : 0) + (needsVersioning ? 1 : 0) + (needsLifecycle ? 1 : 0) +
           (needsSizeMetrics ? 1 : 0));
      int maxPossibleApiCalls = results.size() * 8; // All API calls for all buckets (including CloudWatch)
      double reductionPercent = (1.0 - (double)totalApiCalls / maxPossibleApiCalls) * 100;

      LOGGER.debug("AWS S3 query completed: {} buckets, {} API calls (vs {} max), {:.1f}% reduction",
                   results.size(), totalApiCalls, maxPossibleApiCalls, reductionPercent);
    }

    return results;
  }

  /**
   * Check if a field is projected in the query.
   */
  private boolean isFieldProjected(@Nullable CloudOpsProjectionHandler projectionHandler, String... fieldNames) {
    if (projectionHandler == null || projectionHandler.isSelectAll()) {
      return true; // All fields are needed for SELECT *
    }

    List<String> projectedFields = projectionHandler.getProjectedFieldNames();
    for (String fieldName : fieldNames) {
      if (projectedFields.contains(fieldName)) {
        return true;
      }
    }
    return false;
  }

  @Override public List<Map<String, Object>> queryComputeInstances(List<String> accountIds) {
    return inEveryRegion(accountIds, "EC2 instances", this::computeInstancesIn);
  }

  private List<Map<String, Object>> computeInstancesIn(String accountId,
      AwsCredentials credentials, Region region, Clients clients) {
    List<Map<String, Object>> results = new ArrayList<>();

    Ec2Client ec2Client = clients.add(Ec2Client.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    // Describe all instances
    for (Reservation reservation : ec2Client.describeInstancesPaginator().reservations()) {
      for (Instance instance : reservation.instances()) {
        Map<String, Object> vmData = new HashMap<>();

        // Identity fields
        vmData.put("AccountId", accountId);
        vmData.put("InstanceId", instance.instanceId());
        vmData.put("Region", region.toString());
        vmData.put("AvailabilityZone", instance.placement().availabilityZone());
        vmData.put("ResourceId",
            String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:instance/%s",
                region, accountId, instance.instanceId()));

        // Tags
        Map<String, String> tags = new HashMap<>();
        instance.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
        String application =
            applicationOf(tags);
        vmData.put("Application", application);
        vmData.put("InstanceName", tags.getOrDefault("Name", instance.instanceId()));

        // Configuration facts
        vmData.put("InstanceType", instance.instanceTypeAsString());
        vmData.put("State", instance.state().nameAsString());
        vmData.put("Architecture", instance.architectureAsString());
        // platform is only set for Windows; platformDetails names every OS ("Linux/UNIX")
        vmData.put("Platform", instance.platformDetails());
        vmData.put("VirtualizationType", instance.virtualizationTypeAsString());

        // Network facts
        vmData.put("PublicIpAddress", instance.publicIpAddress());
        vmData.put("PrivateIpAddress", instance.privateIpAddress());
        vmData.put("VpcId", instance.vpcId());
        vmData.put("SubnetId", instance.subnetId());

        // Security facts. Store the instance-profile ARN (not the SDK object) so it equi-matches
        // iam_resources.resource_id for the compute_resources.iam_role foreign key.
        vmData.put("SecurityGroups", instance.securityGroups().stream()
            .map(GroupIdentifier::groupId)
            .collect(java.util.stream.Collectors.joining(",")));
        vmData.put("IamInstanceProfile",
            instance.iamInstanceProfile() != null ? instance.iamInstanceProfile().arn() : null);

        // EBS encryption: the instance only names its volumes; whether each is encrypted
        // is a property of the volume
        List<String> volumeIds = instance.blockDeviceMappings().stream()
            .filter(bdm -> bdm.ebs() != null)
            .map(bdm -> bdm.ebs().volumeId())
            .collect(java.util.stream.Collectors.toList());
        if (volumeIds.isEmpty()) {
          vmData.put("EbsEncrypted", null); // instance store only
        } else {
          vmData.put("EbsEncrypted",
              ec2Client.describeVolumes(r -> r.volumeIds(volumeIds)).volumes().stream()
                  .allMatch(volume -> Boolean.TRUE.equals(volume.encrypted())));
        }

        // Monitoring
        vmData.put("MonitoringEnabled",
            instance.monitoring() != null &&
            "enabled".equals(instance.monitoring().stateAsString()));

        // Timestamps
        vmData.put("LaunchTime", instance.launchTime());

        results.add(vmData);
      }
    }

    return results;
  }

  /**
   * Emits one row per (instance, security-group) association — the normalized form of the
   * compute_resources.security_groups array. Feeds the compute_security_groups junction table whose
   * foreign keys point at compute_resources and network_resources.
   */
  public List<Map<String, Object>> queryComputeSecurityGroups(List<String> accountIds) {
    return inEveryRegion(accountIds, "EC2 instance security groups", this::computeSecurityGroupsIn);
  }

  private List<Map<String, Object>> computeSecurityGroupsIn(String accountId,
      AwsCredentials credentials, Region region, Clients clients) {
    List<Map<String, Object>> results = new ArrayList<>();

    Ec2Client ec2Client = clients.add(Ec2Client.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    for (Reservation reservation : ec2Client.describeInstancesPaginator().reservations()) {
      for (Instance instance : reservation.instances()) {
        String computeResourceId =
            String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:instance/%s",
                region, accountId, instance.instanceId());
        for (GroupIdentifier group : instance.securityGroups()) {
          Map<String, Object> row = new HashMap<>();
          row.put("AccountId", accountId);
          row.put("InstanceId", instance.instanceId());
          row.put("ComputeResourceId", computeResourceId);
          row.put("SecurityGroupId", group.groupId());
          results.add(row);
        }
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryNetworkResources(List<String> accountIds) {
    return inEveryRegion(accountIds, "network resources", this::networkResourcesIn);
  }

  private List<Map<String, Object>> networkResourcesIn(String accountId,
      AwsCredentials credentials, Region region, Clients clients) {
    List<Map<String, Object>> results = new ArrayList<>();

    Ec2Client ec2Client = clients.add(Ec2Client.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    // Query VPCs
    for (Vpc vpc : ec2Client.describeVpcsPaginator().vpcs()) {
      Map<String, Object> networkData = new HashMap<>();

      networkData.put("AccountId", accountId);
      networkData.put("NetworkResource", vpc.vpcId());
      networkData.put("NativeId", vpc.vpcId());
      networkData.put("NetworkResourceType", "VPC");
      networkData.put("Region", region.toString());
      networkData.put("ResourceId",
          String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:vpc/%s", region, accountId, vpc.vpcId()));

      // Tags
      Map<String, String> tags = new HashMap<>();
      vpc.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
      String application =
          applicationOf(tags);
      networkData.put("Application", application);
      networkData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));

      // VPC Configuration
      networkData.put("CidrBlock", vpc.cidrBlock());
      networkData.put("State", vpc.stateAsString());
      networkData.put("IsDefault", vpc.isDefault());
      // DNS settings are not directly available on VPC object
      networkData.put("EnableDnsHostnames", true);
      networkData.put("EnableDnsSupport", true);

      results.add(networkData);
    }

    // Query Security Groups
    for (SecurityGroup sg : ec2Client.describeSecurityGroupsPaginator().securityGroups()) {
      Map<String, Object> networkData = new HashMap<>();

      networkData.put("AccountId", accountId);
      networkData.put("NetworkResource", sg.groupId());
      networkData.put("NativeId", sg.groupId());
      networkData.put("NetworkResourceType", "Security Group");
      networkData.put("Region", region.toString());
      networkData.put("ResourceId",
          String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:security-group/%s", region, accountId, sg.groupId()));

      // Tags
      Map<String, String> tags = new HashMap<>();
      sg.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
      String application =
          applicationOf(tags);
      networkData.put("Application", application);
      networkData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));

      // Security Group Configuration
      networkData.put("GroupName", sg.groupName());
      networkData.put("Description", sg.description());
      networkData.put("VpcId", sg.vpcId());
      networkData.put("IngressRulesCount", sg.ipPermissions().size());
      networkData.put("EgressRulesCount", sg.ipPermissionsEgress().size());

      // Check for overly permissive rules
      boolean hasOpenIngress = sg.ipPermissions().stream()
          .anyMatch(rule -> rule.ipRanges().stream()
                  .anyMatch(range -> "0.0.0.0/0".equals(range.cidrIp()))
              || rule.ipv6Ranges().stream()
                  .anyMatch(range -> "::/0".equals(range.cidrIpv6())));
      networkData.put("HasOpenIngressRule", hasOpenIngress);

      results.add(networkData);
    }

    // Query Elastic IPs
    for (Address address : ec2Client.describeAddresses().addresses()) {
      Map<String, Object> networkData = new HashMap<>();

      networkData.put("AccountId", accountId);
      networkData.put("NetworkResource", address.allocationId());
      networkData.put("NativeId", address.allocationId());
      networkData.put("NetworkResourceType", "Elastic IP");
      networkData.put("Region", region.toString());
      networkData.put("ResourceId", address.allocationId());

      // Tags
      Map<String, String> tags = new HashMap<>();
      address.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
      String application =
          applicationOf(tags);
      networkData.put("Application", application);
      networkData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));

      // Elastic IP Configuration
      networkData.put("PublicIp", address.publicIp());
      networkData.put("Domain", address.domainAsString());
      networkData.put("AssociationId", address.associationId());
      networkData.put("InstanceId", address.instanceId());
      networkData.put("NetworkInterfaceId", address.networkInterfaceId());
      networkData.put("IsAssociated", address.associationId() != null);

      results.add(networkData);
    }

    // Query Subnets. Emitting these as rows lets compute_resources.subnet_id reference a
    // network_resources row by its bare native ID (subnet-...), the same pattern as VPCs.
    for (software.amazon.awssdk.services.ec2.model.Subnet subnet
        : ec2Client.describeSubnetsPaginator().subnets()) {
      Map<String, Object> networkData = new HashMap<>();

      networkData.put("AccountId", accountId);
      networkData.put("NetworkResource", subnet.subnetId());
      networkData.put("NativeId", subnet.subnetId());
      networkData.put("NetworkResourceType", "Subnet");
      networkData.put("Region", region.toString());
      networkData.put("ResourceId",
          String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:subnet/%s", region, accountId, subnet.subnetId()));

      // Tags
      Map<String, String> tags = new HashMap<>();
      subnet.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
      String application =
          applicationOf(tags);
      networkData.put("Application", application);
      networkData.put("Tags", CloudOpsDataConverter.tagsToJson(tags));

      // Subnet Configuration
      networkData.put("CidrBlock", subnet.cidrBlock());
      networkData.put("State", subnet.stateAsString());
      networkData.put("VpcId", subnet.vpcId());
      networkData.put("IsDefault", subnet.defaultForAz());

      results.add(networkData);
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryIAMResources(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        IamClient iamClient = IamClient.builder()
            .region(Region.AWS_GLOBAL)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // Query IAM Users
        for (software.amazon.awssdk.services.iam.model.User user : iamClient.listUsersPaginator().users()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", user.userName());
          iamData.put("IAMResourceType", "IAM User");
          iamData.put("Region", "global");
          iamData.put("ResourceId", user.arn());

          // Get user tags
          Map<String, String> tags = new HashMap<>();
          iamClient.listUserTags(r -> r.userName(user.userName())).tags()
              .forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              applicationOf(tags);
          iamData.put("Application", application);

          // User configuration
          iamData.put("CreateDate", user.createDate());
          iamData.put("PasswordLastUsed", user.passwordLastUsed());
          iamData.put("Path", user.path());

          // Check for access keys
          List<AccessKeyMetadata> accessKeys = iamClient.listAccessKeys(r -> r.userName(user.userName())).accessKeyMetadata();
          iamData.put("AccessKeyCount", accessKeys.size());
          iamData.put("ActiveAccessKeys", accessKeys.stream()
              .filter(key -> key.statusAsString().equals("Active"))
              .count());

          // Check for MFA devices
          List<MFADevice> mfaDevices = iamClient.listMFADevices(r -> r.userName(user.userName())).mfaDevices();
          iamData.put("MFAEnabled", !mfaDevices.isEmpty());

          results.add(iamData);
        }

        // Query IAM Roles
        for (Role role : iamClient.listRolesPaginator().roles()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", role.roleName());
          iamData.put("IAMResourceType", "IAM Role");
          iamData.put("Region", "global");
          iamData.put("ResourceId", role.arn());

          // Get role tags
          Map<String, String> tags = new HashMap<>();
          iamClient.listRoleTags(r -> r.roleName(role.roleName())).tags()
              .forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              applicationOf(tags);
          iamData.put("Application", application);

          // Role configuration
          iamData.put("CreateDate", role.createDate());
          iamData.put("Path", role.path());
          iamData.put("MaxSessionDuration", role.maxSessionDuration());
          iamData.put("Description", role.description());

          // Parse trust policy for principal info
          if (role.assumeRolePolicyDocument() != null) {
            iamData.put("TrustPolicyDocument", role.assumeRolePolicyDocument());
          }

          results.add(iamData);
        }

        // Query IAM Policies (customer managed only)
        for (Policy policy : iamClient.listPoliciesPaginator(r -> r.scope(PolicyScopeType.LOCAL)).policies()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", policy.policyName());
          iamData.put("IAMResourceType", "IAM Policy");
          iamData.put("Region", "global");
          iamData.put("ResourceId", policy.arn());

          // Get policy tags
          Map<String, String> tags = new HashMap<>();
          iamClient.listPolicyTags(r -> r.policyArn(policy.arn())).tags()
              .forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              applicationOf(tags);
          iamData.put("Application", application);

          // Policy configuration
          iamData.put("CreateDate", policy.createDate());
          iamData.put("UpdateDate", policy.updateDate());
          iamData.put("AttachmentCount", policy.attachmentCount());
          iamData.put("PermissionsBoundaryUsageCount", policy.permissionsBoundaryUsageCount());
          iamData.put("DefaultVersionId", policy.defaultVersionId());
          iamData.put("IsAttachable", policy.isAttachable());
          iamData.put("Description", policy.description());

          results.add(iamData);
        }

        // Query Instance Profiles. These are the identity actually attached to EC2 instances, so
        // emitting them lets compute_resources.iam_role reference iam_resources by the profile ARN.
        for (InstanceProfile instanceProfile : iamClient.listInstanceProfilesPaginator().instanceProfiles()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", instanceProfile.instanceProfileName());
          iamData.put("IAMResourceType", "Instance Profile");
          iamData.put("Region", "global");
          iamData.put("ResourceId", instanceProfile.arn());
          iamData.put("Application", "Untagged/Orphaned");
          iamData.put("CreateDate", instanceProfile.createDate());
          iamData.put("Path", instanceProfile.path());

          results.add(iamData);
        }

      } catch (RuntimeException e) {
        throw new IllegalStateException("Querying IAM resources in AWS account " + accountId
            + " failed: " + e.getMessage(), e);
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryDatabaseResources(List<String> accountIds) {
    return inEveryRegion(accountIds, "database resources", this::databaseResourcesIn);
  }

  private List<Map<String, Object>> databaseResourcesIn(String accountId,
      AwsCredentials credentials, Region region, Clients clients) {
    List<Map<String, Object>> results = new ArrayList<>();

    RdsClient rdsClient = clients.add(RdsClient.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    for (DBInstance dbInstance : rdsClient.describeDBInstancesPaginator().dbInstances()) {
      Map<String, Object> dbData = new HashMap<>();

      dbData.put("AccountId", accountId);
      dbData.put("DatabaseResource", dbInstance.dbInstanceIdentifier());
      dbData.put("DatabaseType", "RDS Instance");
      dbData.put("Region", region.toString());
      dbData.put("ResourceId", dbInstance.dbInstanceArn());

      Map<String, String> tags = new HashMap<>();
      for (software.amazon.awssdk.services.rds.model.Tag tag : dbInstance.tagList()) {
        tags.put(tag.key(), tag.value());
      }
      dbData.put("Application", applicationOf(tags));

      dbData.put("Engine", dbInstance.engine());
      dbData.put("EngineVersion", dbInstance.engineVersion());
      dbData.put("InstanceClass", dbInstance.dbInstanceClass());
      dbData.put("AllocatedStorage", dbInstance.allocatedStorage());
      dbData.put("MultiAZ", dbInstance.multiAZ());
      dbData.put("Status", dbInstance.dbInstanceStatus());
      dbData.put("PubliclyAccessible", dbInstance.publiclyAccessible());
      dbData.put("Encrypted", dbInstance.storageEncrypted());
      dbData.put("EncryptionKey", dbInstance.kmsKeyId());
      dbData.put("BackupRetentionDays", dbInstance.backupRetentionPeriod());
      dbData.put("BackupWindow", dbInstance.preferredBackupWindow());
      dbData.put("CreateTime", dbInstance.instanceCreateTime());

      results.add(dbData);
    }

    // Aurora and Multi-AZ DB clusters
    for (DBCluster dbCluster : rdsClient.describeDBClustersPaginator().dbClusters()) {
      Map<String, Object> dbData = new HashMap<>();

      dbData.put("AccountId", accountId);
      dbData.put("DatabaseResource", dbCluster.dbClusterIdentifier());
      dbData.put("DatabaseType", "RDS Cluster");
      dbData.put("Region", region.toString());
      dbData.put("ResourceId", dbCluster.dbClusterArn());

      Map<String, String> tags = new HashMap<>();
      for (software.amazon.awssdk.services.rds.model.Tag tag : dbCluster.tagList()) {
        tags.put(tag.key(), tag.value());
      }
      dbData.put("Application", applicationOf(tags));

      dbData.put("Engine", dbCluster.engine());
      dbData.put("EngineVersion", dbCluster.engineVersion());
      // An instance class, an allocated size and public access belong to a Multi-AZ DB
      // cluster; an Aurora cluster has them on its member instances, which are rows of their own
      dbData.put("InstanceClass", dbCluster.dbClusterInstanceClass());
      dbData.put("AllocatedStorage", dbCluster.allocatedStorage());
      dbData.put("MultiAZ", dbCluster.multiAZ());
      dbData.put("Status", dbCluster.status());
      dbData.put("PubliclyAccessible", dbCluster.publiclyAccessible());
      dbData.put("Encrypted", dbCluster.storageEncrypted());
      dbData.put("EncryptionKey", dbCluster.kmsKeyId());
      dbData.put("BackupRetentionDays", dbCluster.backupRetentionPeriod());
      dbData.put("BackupWindow", dbCluster.preferredBackupWindow());
      dbData.put("CreateTime", dbCluster.clusterCreateTime());

      results.add(dbData);
    }

    DynamoDbClient dynamoClient = clients.add(DynamoDbClient.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    for (String tableName : dynamoClient.listTablesPaginator().tableNames()) {
      try {
        TableDescription table = dynamoClient.describeTable(r -> r.tableName(tableName)).table();
        Map<String, Object> dbData = new HashMap<>();

        dbData.put("AccountId", accountId);
        dbData.put("DatabaseResource", table.tableName());
        dbData.put("DatabaseType", "DynamoDB Table");
        dbData.put("Region", region.toString());
        dbData.put("ResourceId", table.tableArn());

        Map<String, String> tags = new HashMap<>();
        dynamoClient.listTagsOfResource(r -> r.resourceArn(table.tableArn())).tags()
            .forEach(tag -> tags.put(tag.key(), tag.value()));
        dbData.put("Application", applicationOf(tags));

        // A table has no engine version, instance class, allocated size, network exposure
        // or backup window: it is a serverless API
        dbData.put("Engine", "dynamodb");
        // DynamoDB keeps every table in three availability zones and encrypts every table at
        // rest; the key is reported only when it is a KMS key rather than the AWS-owned one
        dbData.put("MultiAZ", true);
        dbData.put("Status", table.tableStatusAsString());
        dbData.put("Encrypted", true);
        if (table.sseDescription() != null) {
          dbData.put("EncryptionKey", table.sseDescription().kmsMasterKeyArn());
        }
        // Point-in-time recovery is DynamoDB's retained backup: its window when on, 0 when off
        PointInTimeRecoveryDescription recovery =
            dynamoClient.describeContinuousBackups(r -> r.tableName(tableName))
                .continuousBackupsDescription().pointInTimeRecoveryDescription();
        dbData.put("BackupRetentionDays",
            recovery.pointInTimeRecoveryStatus() == PointInTimeRecoveryStatus.ENABLED
                ? recovery.recoveryPeriodInDays() : Integer.valueOf(0));
        dbData.put("CreateTime", table.creationDateTime());

        results.add(dbData);
      } catch (RuntimeException e) {
        throw new IllegalStateException("Describing DynamoDB table " + tableName
            + " failed: " + e.getMessage(), e);
      }
    }

    ElastiCacheClient elastiCacheClient = clients.add(ElastiCacheClient.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    for (CacheCluster cluster : elastiCacheClient.describeCacheClustersPaginator().cacheClusters()) {
      Map<String, Object> dbData = new HashMap<>();

      dbData.put("AccountId", accountId);
      dbData.put("DatabaseResource", cluster.cacheClusterId());
      dbData.put("DatabaseType", "ElastiCache Cluster");
      dbData.put("Region", region.toString());
      dbData.put("ResourceId", cluster.arn());

      Map<String, String> tags = new HashMap<>();
      elastiCacheClient.listTagsForResource(r -> r.resourceName(cluster.arn())).tagList()
          .forEach(tag -> tags.put(tag.key(), tag.value()));
      dbData.put("Application", applicationOf(tags));

      // A cache has no allocated disk size and is reachable only from inside its VPC
      dbData.put("Engine", cluster.engine());
      dbData.put("EngineVersion", cluster.engineVersion());
      dbData.put("InstanceClass", cluster.cacheNodeType());
      dbData.put("Status", cluster.cacheClusterStatus());
      dbData.put("PubliclyAccessible", false);
      dbData.put("Encrypted", cluster.atRestEncryptionEnabled());
      // Multi-AZ failover and the encryption key are settings of the replication group a
      // node belongs to; a cluster outside one spans zones only when its nodes do
      String replicationGroupId = cluster.replicationGroupId();
      if (replicationGroupId != null) {
        ReplicationGroup group = elastiCacheClient
            .describeReplicationGroups(r -> r.replicationGroupId(replicationGroupId))
            .replicationGroups().get(0);
        dbData.put("MultiAZ", group.multiAZ() == MultiAZStatus.ENABLED);
        dbData.put("EncryptionKey", group.kmsKeyId());
      } else {
        dbData.put("MultiAZ", "Multiple".equals(cluster.preferredAvailabilityZone()));
      }
      dbData.put("BackupRetentionDays", cluster.snapshotRetentionLimit());
      dbData.put("BackupWindow", cluster.snapshotWindow());
      dbData.put("CreateTime", cluster.cacheClusterCreateTime());

      results.add(dbData);
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryContainerRegistries(List<String> accountIds) {
    return inEveryRegion(accountIds, "ECR repositories", this::containerRegistriesIn);
  }

  private List<Map<String, Object>> containerRegistriesIn(String accountId,
      AwsCredentials credentials, Region region, Clients clients) {
    List<Map<String, Object>> results = new ArrayList<>();

    EcrClient ecrClient = clients.add(EcrClient.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build());

    // List all repositories
    for (Repository repository : ecrClient.describeRepositoriesPaginator().repositories()) {
      Map<String, Object> registryData = new HashMap<>();

      // Identity fields
      registryData.put("AccountId", accountId);
      registryData.put("RepositoryName", repository.repositoryName());
      registryData.put("Region", region.toString());
      registryData.put("ResourceId", repository.repositoryArn());
      registryData.put("RepositoryUri", repository.repositoryUri());

      // Configuration facts
      registryData.put("ImageScanningEnabled",
          repository.imageScanningConfiguration() != null &&
          repository.imageScanningConfiguration().scanOnPush());
      registryData.put("ImageTagMutability", repository.imageTagMutabilityAsString());

      // Encryption
      if (repository.encryptionConfiguration() != null) {
        registryData.put("EncryptionType",
            repository.encryptionConfiguration().encryptionTypeAsString());
        registryData.put("KmsKey", repository.encryptionConfiguration().kmsKey());
      }

      // Tags are not part of the repository description
      Map<String, String> tags = new HashMap<>();
      ecrClient.listTagsForResource(r -> r.resourceArn(repository.repositoryArn())).tags()
          .forEach(tag -> tags.put(tag.key(), tag.value()));
      registryData.put("Application", applicationOf(tags));

      // A lifecycle policy is what expires images
      try {
        ecrClient.getLifecyclePolicy(r -> r.repositoryName(repository.repositoryName()));
        registryData.put("RetentionPolicy", "Enabled");
      } catch (AwsServiceException e) {
        if (!isAwsError(e, "LifecyclePolicyNotFoundException")) {
          throw e;
        }
        registryData.put("RetentionPolicy", "Disabled");
      }

      // Timestamps
      registryData.put("CreatedAt", repository.createdAt());

      results.add(registryData);
    }

    return results;
  }

  /**
   * Apply client-side filtering for filters that cannot be pushed to AWS API.
   */
  private boolean passesClientSideFilters(Map<String, Object> data, CloudOpsFilterHandler filterHandler) {
    // For now, implement basic filtering logic
    // This would be enhanced to handle all remaining filters that weren't pushed down

    // Check application filters (tag-based)
    List<CloudOpsFilterHandler.FilterInfo> appFilters = filterHandler.getFiltersForField("application");
    for (CloudOpsFilterHandler.FilterInfo filter : appFilters) {
      Object appValue = data.get("Application");
      if (appValue == null) appValue = "Untagged/Orphaned";

      switch (filter.operation) {
        case EQUALS:
          if (!appValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case NOT_EQUALS:
          if (appValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case LIKE:
          String pattern = filter.value.toString().replace("%", ".*");
          if (!appValue.toString().matches(pattern)) {
            return false;
          }
          break;
        case IN:
          if (filter.values != null && !filter.values.contains(appValue.toString())) {
            return false;
          }
          break;
        case IS_NULL:
          if (!"Untagged/Orphaned".equals(appValue.toString())) {
            return false;
          }
          break;
        case IS_NOT_NULL:
          if ("Untagged/Orphaned".equals(appValue.toString())) {
            return false;
          }
          break;
        default:
          break;
      }
    }

    // Check cluster name filters
    List<CloudOpsFilterHandler.FilterInfo> nameFilters = filterHandler.getFiltersForField("cluster_name");
    for (CloudOpsFilterHandler.FilterInfo filter : nameFilters) {
      Object nameValue = data.get("ClusterName");
      if (nameValue == null) continue;

      switch (filter.operation) {
        case EQUALS:
          if (!nameValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case NOT_EQUALS:
          if (nameValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case LIKE:
          String pattern = filter.value.toString().replace("%", ".*");
          if (!nameValue.toString().matches(pattern)) {
            return false;
          }
          break;
        case IN:
          if (filter.values != null && !filter.values.contains(nameValue.toString())) {
            return false;
          }
          break;
        default:
          break;
      }
    }

    return true; // Passes all client-side filters
  }

  /**
   * Get cache metrics for monitoring.
   */
  public CloudOpsCacheManager.CacheMetrics getCacheMetrics() {
    return cacheManager.getCacheMetrics();
  }

  /**
   * Invalidate cache entries for a specific account.
   */
  public void invalidateAccountCache(String accountId) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated AWS cache for account: {}", accountId);
    }
  }

  /**
   * Invalidate cache entries for a specific region.
   */
  public void invalidateRegionCache(String region) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated AWS cache for region: {}", region);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAllCache() {
    cacheManager.invalidateAll();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Invalidated all AWS cache entries");
    }
  }

  /**
   * Fetch S3 bucket size from CloudWatch metrics.
   * This is expensive and should only be called when size_bytes is projected.
   */
  private Long fetchBucketSizeFromCloudWatch(String accountId, String bucketName,
      Region bucketRegion, AwsCredentials credentials) {
    // A bucket's metrics are kept in the region the bucket lives in
    try (CloudWatchClient cloudWatchClient = CloudWatchClient.builder()
          .region(bucketRegion)
          .credentialsProvider(StaticCredentialsProvider.create(credentials))
          .build()) {

      // Get the bucket size metric from CloudWatch
      Dimension bucketNameDimension = Dimension.builder()
          .name("BucketName")
          .value(bucketName)
          .build();

      Dimension storageTypeDimension = Dimension.builder()
          .name("StorageType")
          .value("StandardStorage") // Could also be "StandardIAStorage", "ReducedRedundancyStorage"
          .build();

      // Get the most recent metric data point (last 7 days)
      java.time.Instant endTime = java.time.Instant.now();
      java.time.Instant startTime = endTime.minus(java.time.Duration.ofDays(7));

      GetMetricStatisticsRequest request = GetMetricStatisticsRequest.builder()
          .namespace("AWS/S3")
          .metricName("BucketSizeBytes")
          .dimensions(bucketNameDimension, storageTypeDimension)
          .startTime(startTime)
          .endTime(endTime)
          .period(86400) // Daily data points
          .statistics(Statistic.MAXIMUM) // Use maximum value
          .build();

      GetMetricStatisticsResponse response = cloudWatchClient.getMetricStatistics(request);

      // Get the most recent datapoint
      if (!response.datapoints().isEmpty()) {
        Datapoint latestDatapoint = response.datapoints().stream()
            .max(java.util.Comparator.comparing(Datapoint::timestamp))
            .orElse(null);

        if (latestDatapoint != null) {
          return latestDatapoint.maximum().longValue();
        }
      }

      LOGGER.debug("No CloudWatch size metrics found for bucket {} in account {}", bucketName, accountId);
      return null;

    } catch (RuntimeException e) {
      throw new IllegalStateException("Reading the CloudWatch size metric of bucket "
          + bucketName + " failed: " + e.getMessage(), e);
    }
  }
}
