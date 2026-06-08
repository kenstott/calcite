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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration for Cloud Governance adapter.
 */
public class CloudOpsConfig {
  @JsonProperty("providers")
  public final List<String> providers;

  @JsonProperty("azure")
  public final AzureConfig azure;

  @JsonProperty("gcp")
  public final GCPConfig gcp;

  @JsonProperty("aws")
  public final AWSConfig aws;

  @JsonProperty("cacheEnabled")
  public final boolean cacheEnabled;

  @JsonProperty("cacheTtlMinutes")
  public final int cacheTtlMinutes;

  @JsonProperty("cacheDebugMode")
  public final boolean cacheDebugMode;

  @JsonCreator
  public CloudOpsConfig(
      @JsonProperty("providers") List<String> providers,
      @JsonProperty("azure") AzureConfig azure,
      @JsonProperty("gcp") GCPConfig gcp,
      @JsonProperty("aws") AWSConfig aws,
      @JsonProperty("cacheEnabled") Boolean cacheEnabled,
      @JsonProperty("cacheTtlMinutes") Integer cacheTtlMinutes,
      @JsonProperty("cacheDebugMode") Boolean cacheDebugMode) {
    this.providers = providers != null ? providers : List.of("azure", "gcp", "aws");
    this.azure = azure;
    this.gcp = gcp;
    this.aws = aws;
    this.cacheEnabled = cacheEnabled != null ? cacheEnabled : true;
    this.cacheTtlMinutes = cacheTtlMinutes != null ? cacheTtlMinutes : 5;
    this.cacheDebugMode = cacheDebugMode != null ? cacheDebugMode : false;
  }

  public static class AzureConfig {
    @JsonProperty("tenantId")
    public final String tenantId;

    @JsonProperty("clientId")
    public final String clientId;

    @JsonProperty("clientSecret")
    public final String clientSecret;

    @JsonProperty("subscriptionIds")
    public final List<String> subscriptionIds;

    @JsonCreator
    public AzureConfig(
        @JsonProperty("tenantId") String tenantId,
        @JsonProperty("clientId") String clientId,
        @JsonProperty("clientSecret") String clientSecret,
        @JsonProperty("subscriptionIds") List<String> subscriptionIds) {
      this.tenantId = tenantId;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.subscriptionIds = subscriptionIds;
    }
  }

  public static class GCPConfig {
    @JsonProperty("projectIds")
    public final List<String> projectIds;

    @JsonProperty("credentialsPath")
    public final String credentialsPath;

    @JsonCreator
    public GCPConfig(
        @JsonProperty("projectIds") List<String> projectIds,
        @JsonProperty("credentialsPath") String credentialsPath) {
      this.projectIds = projectIds;
      this.credentialsPath = credentialsPath;
    }
  }

  public static class AWSConfig {
    @JsonProperty("accountIds")
    public final List<String> accountIds;

    @JsonProperty("region")
    public final String region;

    @JsonProperty("accessKeyId")
    public final String accessKeyId;

    @JsonProperty("secretAccessKey")
    public final String secretAccessKey;

    @JsonProperty("roleArn")
    public final String roleArn;

    @JsonCreator
    public AWSConfig(
        @JsonProperty("accountIds") List<String> accountIds,
        @JsonProperty("region") String region,
        @JsonProperty("accessKeyId") String accessKeyId,
        @JsonProperty("secretAccessKey") String secretAccessKey,
        @JsonProperty("roleArn") String roleArn) {
      this.accountIds = accountIds;
      this.region = region != null ? region : "us-east-1";
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.roleArn = roleArn;
    }
  }
}
