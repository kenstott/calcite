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
package org.apache.calcite.adapter.salesforce.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Catalog configuration for the Salesforce Trino connector. These friendly properties are mapped
 * onto a {@code jdbc:salesforce:} URL for {@code SalesforceDriver}, so the user never writes a raw
 * JDBC URL.
 *
 * <p>Authenticate with one of: {@code client-id} + {@code client-secret} (OAuth client credentials
 * flow; {@code login-url} must be the org's My Domain URL), {@code username} + {@code password}
 * (+ {@code security-token}) with {@code client-id} + {@code client-secret} (OAuth username-password
 * flow), or {@code access-token} + {@code instance-url}.
 */
public class SalesforceConfig
{
    private String loginUrl;
    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
    private String securityToken;
    private String accessToken;
    private String instanceUrl;
    private String apiVersion;
    private String schema;
    private Integer cacheMaxSize;
    private String describeCacheDirectory;
    private Integer describeCacheTtlMinutes;

    @NotNull
    public String getLoginUrl()
    {
        return loginUrl;
    }

    @Config("login-url")
    @ConfigDescription("Salesforce login URL; the org's My Domain URL for the client credentials flow, "
            + "e.g. https://mydomain.my.salesforce.com")
    public SalesforceConfig setLoginUrl(String loginUrl)
    {
        this.loginUrl = loginUrl;
        return this;
    }

    public String getClientId()
    {
        return clientId;
    }

    @Config("client-id")
    @ConfigDescription("Connected app consumer key")
    public SalesforceConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("client-secret")
    @ConfigSecuritySensitive
    @ConfigDescription("Connected app consumer secret")
    public SalesforceConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("username")
    @ConfigDescription("Salesforce username (username-password flow only)")
    public SalesforceConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("password")
    @ConfigSecuritySensitive
    @ConfigDescription("Salesforce password (username-password flow only)")
    public SalesforceConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getSecurityToken()
    {
        return securityToken;
    }

    @Config("security-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Salesforce user security token (username-password flow only)")
    public SalesforceConfig setSecurityToken(String securityToken)
    {
        this.securityToken = securityToken;
        return this;
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    @Config("access-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Pre-issued OAuth access token (used with instance-url)")
    public SalesforceConfig setAccessToken(String accessToken)
    {
        this.accessToken = accessToken;
        return this;
    }

    public String getInstanceUrl()
    {
        return instanceUrl;
    }

    @Config("instance-url")
    @ConfigDescription("Org instance URL for a pre-issued access token")
    public SalesforceConfig setInstanceUrl(String instanceUrl)
    {
        this.instanceUrl = instanceUrl;
        return this;
    }

    public String getApiVersion()
    {
        return apiVersion;
    }

    @Config("api-version")
    @ConfigDescription("REST API version, e.g. v61.0 (adapter default: v58.0)")
    public SalesforceConfig setApiVersion(String apiVersion)
    {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getSchema()
    {
        return schema;
    }

    @Config("schema")
    @ConfigDescription("The schema name to register the org's sObjects under (default: \"salesforce\")")
    public SalesforceConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    public Integer getCacheMaxSize()
    {
        return cacheMaxSize;
    }

    @Config("cache-max-size")
    @ConfigDescription("Maximum sObject describe results cached per connection (adapter default: 1000)")
    public SalesforceConfig setCacheMaxSize(Integer cacheMaxSize)
    {
        this.cacheMaxSize = cacheMaxSize;
        return this;
    }

    public String getDescribeCacheDirectory()
    {
        return describeCacheDirectory;
    }

    @Config("describe-cache-directory")
    @ConfigDescription("Directory where sObject describe results are kept between restarts "
            + "(adapter default: ~/.calcite/salesforce/describe-cache)")
    public SalesforceConfig setDescribeCacheDirectory(String describeCacheDirectory)
    {
        this.describeCacheDirectory = describeCacheDirectory;
        return this;
    }

    @Min(0)
    public Integer getDescribeCacheTtlMinutes()
    {
        return describeCacheTtlMinutes;
    }

    @Config("describe-cache-ttl-minutes")
    @ConfigDescription("How long an sObject describe result on disk is used for; 0 keeps nothing "
            + "on disk (adapter default: 1440)")
    public SalesforceConfig setDescribeCacheTtlMinutes(Integer describeCacheTtlMinutes)
    {
        this.describeCacheTtlMinutes = describeCacheTtlMinutes;
        return this;
    }
}
