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
package org.apache.calcite.adapter.ops.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

/**
 * Catalog configuration for the Cloud Ops Trino connector. These friendly properties are mapped onto
 * a {@code jdbc:cloudops:} URL for {@code CloudOpsDriver}, so the user never writes a raw JDBC URL.
 * The dotted property names mirror the flat operand keys the CloudOps adapter reads
 * ({@code azure.tenantId}, {@code aws.accessKeyId}, ...); list values are comma-separated.
 *
 * <p>All properties are optional at the bean level: a catalog configures only the providers it uses.
 * Cross-field requirements (a provider needs its full credential set; at least one provider must be
 * present) are enforced at startup by {@code CloudOpsClientModule}, not here.
 */
public class CloudOpsConfig
{
    private String providers;

    private String azureTenantId;
    private String azureClientId;
    private String azureClientSecret;
    private String azureSubscriptionIds;

    private String gcpProjectIds;
    private String gcpCredentialsPath;

    private String awsAccountIds;
    private String awsRegion;
    private String awsAccessKeyId;
    private String awsSecretAccessKey;
    private String awsRoleArn;

    private Boolean cacheEnabled;
    private Integer cacheTtlMinutes;
    private Boolean cacheDebugMode;

    public String getProviders()
    {
        return providers;
    }

    @Config("providers")
    @ConfigDescription("Comma-separated providers to query, e.g. azure,aws,gcp (default: all configured)")
    public CloudOpsConfig setProviders(String providers)
    {
        this.providers = providers;
        return this;
    }

    public String getAzureTenantId()
    {
        return azureTenantId;
    }

    @Config("azure.tenant-id")
    @ConfigDescription("Azure AD tenant ID (presence enables the Azure provider)")
    public CloudOpsConfig setAzureTenantId(String azureTenantId)
    {
        this.azureTenantId = azureTenantId;
        return this;
    }

    public String getAzureClientId()
    {
        return azureClientId;
    }

    @Config("azure.client-id")
    @ConfigDescription("Azure AD app registration client ID")
    public CloudOpsConfig setAzureClientId(String azureClientId)
    {
        this.azureClientId = azureClientId;
        return this;
    }

    public String getAzureClientSecret()
    {
        return azureClientSecret;
    }

    @Config("azure.client-secret")
    @ConfigSecuritySensitive
    @ConfigDescription("Azure AD app registration client secret")
    public CloudOpsConfig setAzureClientSecret(String azureClientSecret)
    {
        this.azureClientSecret = azureClientSecret;
        return this;
    }

    public String getAzureSubscriptionIds()
    {
        return azureSubscriptionIds;
    }

    @Config("azure.subscription-ids")
    @ConfigDescription("Comma-separated Azure subscription IDs to inventory")
    public CloudOpsConfig setAzureSubscriptionIds(String azureSubscriptionIds)
    {
        this.azureSubscriptionIds = azureSubscriptionIds;
        return this;
    }

    public String getGcpProjectIds()
    {
        return gcpProjectIds;
    }

    @Config("gcp.project-ids")
    @ConfigDescription("Comma-separated GCP project IDs to inventory")
    public CloudOpsConfig setGcpProjectIds(String gcpProjectIds)
    {
        this.gcpProjectIds = gcpProjectIds;
        return this;
    }

    public String getGcpCredentialsPath()
    {
        return gcpCredentialsPath;
    }

    @Config("gcp.credentials-path")
    @ConfigDescription("Path to a GCP service-account JSON key file (presence enables the GCP provider)")
    public CloudOpsConfig setGcpCredentialsPath(String gcpCredentialsPath)
    {
        this.gcpCredentialsPath = gcpCredentialsPath;
        return this;
    }

    public String getAwsAccountIds()
    {
        return awsAccountIds;
    }

    @Config("aws.account-ids")
    @ConfigDescription("Comma-separated AWS account IDs to inventory")
    public CloudOpsConfig setAwsAccountIds(String awsAccountIds)
    {
        this.awsAccountIds = awsAccountIds;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("aws.region")
    @ConfigDescription("AWS region, e.g. us-east-1")
    public CloudOpsConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public String getAwsAccessKeyId()
    {
        return awsAccessKeyId;
    }

    @Config("aws.access-key-id")
    @ConfigDescription("AWS access key ID (presence enables the AWS provider)")
    public CloudOpsConfig setAwsAccessKeyId(String awsAccessKeyId)
    {
        this.awsAccessKeyId = awsAccessKeyId;
        return this;
    }

    public String getAwsSecretAccessKey()
    {
        return awsSecretAccessKey;
    }

    @Config("aws.secret-access-key")
    @ConfigSecuritySensitive
    @ConfigDescription("AWS secret access key")
    public CloudOpsConfig setAwsSecretAccessKey(String awsSecretAccessKey)
    {
        this.awsSecretAccessKey = awsSecretAccessKey;
        return this;
    }

    public String getAwsRoleArn()
    {
        return awsRoleArn;
    }

    @Config("aws.role-arn")
    @ConfigDescription("Optional IAM role ARN to assume for cross-account access")
    public CloudOpsConfig setAwsRoleArn(String awsRoleArn)
    {
        this.awsRoleArn = awsRoleArn;
        return this;
    }

    public Boolean getCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Enable the adapter result cache (adapter default: true)")
    public CloudOpsConfig setCacheEnabled(Boolean cacheEnabled)
    {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public Integer getCacheTtlMinutes()
    {
        return cacheTtlMinutes;
    }

    @Config("cache.ttl-minutes")
    @ConfigDescription("Adapter result-cache TTL in minutes (adapter default: 5)")
    public CloudOpsConfig setCacheTtlMinutes(Integer cacheTtlMinutes)
    {
        this.cacheTtlMinutes = cacheTtlMinutes;
        return this;
    }

    public Boolean getCacheDebugMode()
    {
        return cacheDebugMode;
    }

    @Config("cache.debug-mode")
    @ConfigDescription("Enable adapter cache debug logging (adapter default: false)")
    public CloudOpsConfig setCacheDebugMode(Boolean cacheDebugMode)
    {
        this.cacheDebugMode = cacheDebugMode;
        return this;
    }
}
