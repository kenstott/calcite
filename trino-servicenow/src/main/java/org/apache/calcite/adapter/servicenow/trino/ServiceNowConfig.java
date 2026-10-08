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
package org.apache.calcite.adapter.servicenow.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Catalog configuration for the ServiceNow Trino connector. These friendly properties are mapped
 * onto a {@code jdbc:servicenow:} URL for {@code ServiceNowDriver}, so the user never writes a raw
 * JDBC URL.
 *
 * <p>Authentication is HTTP Basic: {@code username} + {@code password} of an integration user.
 * OAuth client credentials is not implemented yet.
 */
public class ServiceNowConfig
{
    private String instanceUrl;
    private String authType = "basic";
    private String username;
    private String password;
    private String tables;
    private String excludeColumnTypes;
    private String schema;
    private String pushdownVerification;
    private String trustPushdown;
    private Integer pageSize;
    private Integer maxConcurrentRequests;
    private Integer maxRetries;
    private Integer maxRetryWaitSeconds;
    private String catalogCacheDirectory;
    private Integer catalogCacheTtlMinutes;

    @NotNull
    public String getInstanceUrl()
    {
        return instanceUrl;
    }

    @Config("instance-url")
    @ConfigDescription("ServiceNow instance URL, e.g. https://dev12345.service-now.com")
    public ServiceNowConfig setInstanceUrl(String instanceUrl)
    {
        this.instanceUrl = instanceUrl;
        return this;
    }

    @NotNull
    public String getAuthType()
    {
        return authType;
    }

    @Config("auth-type")
    @ConfigDescription("Authentication method; only 'basic' is implemented (default: basic)")
    public ServiceNowConfig setAuthType(String authType)
    {
        this.authType = authType;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("username")
    @ConfigDescription("Integration user name (basic authentication)")
    public ServiceNowConfig setUsername(String username)
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
    @ConfigDescription("Integration user password (basic authentication)")
    public ServiceNowConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getTables()
    {
        return tables;
    }

    @Config("tables")
    @ConfigDescription("Comma-separated names of the only tables to expose "
            + "(default: every table in sys_db_object)")
    public ServiceNowConfig setTables(String tables)
    {
        this.tables = tables;
        return this;
    }

    public String getExcludeColumnTypes()
    {
        return excludeColumnTypes;
    }

    @Config("exclude-column-types")
    @ConfigDescription("Comma-separated ServiceNow field types whose columns are left out; "
            + "a column of a type the adapter cannot map is otherwise an error")
    public ServiceNowConfig setExcludeColumnTypes(String excludeColumnTypes)
    {
        this.excludeColumnTypes = excludeColumnTypes;
        return this;
    }

    public String getSchema()
    {
        return schema;
    }

    @Config("schema")
    @ConfigDescription("The schema name to register the instance's tables under "
            + "(default: \"servicenow\")")
    public ServiceNowConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    @Min(1)
    public Integer getPageSize()
    {
        return pageSize;
    }

    @Config("page-size")
    @ConfigDescription("Rows per request when reading a table (adapter default: 1000)")
    public ServiceNowConfig setPageSize(Integer pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @Min(1)
    public Integer getMaxConcurrentRequests()
    {
        return maxConcurrentRequests;
    }

    @Config("max-concurrent-requests")
    @ConfigDescription("Concurrent requests to the instance (adapter default: 2)")
    public ServiceNowConfig setMaxConcurrentRequests(Integer maxConcurrentRequests)
    {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Min(0)
    public Integer getMaxRetries()
    {
        return maxRetries;
    }

    @Config("max-retries")
    @ConfigDescription("Retries of a rate-limited (429) request (adapter default: 3)")
    public ServiceNowConfig setMaxRetries(Integer maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Min(0)
    public Integer getMaxRetryWaitSeconds()
    {
        return maxRetryWaitSeconds;
    }

    @Config("max-retry-wait-seconds")
    @ConfigDescription("Total seconds to wait across retries of a rate-limited request "
            + "(adapter default: 60)")
    public ServiceNowConfig setMaxRetryWaitSeconds(Integer maxRetryWaitSeconds)
    {
        this.maxRetryWaitSeconds = maxRetryWaitSeconds;
        return this;
    }

    public String getCatalogCacheDirectory()
    {
        return catalogCacheDirectory;
    }

    @Config("catalog-cache-directory")
    @ConfigDescription("Directory where the table and column catalog is kept between restarts "
            + "(adapter default: ~/.calcite/servicenow/catalog-cache)")
    public ServiceNowConfig setCatalogCacheDirectory(String catalogCacheDirectory)
    {
        this.catalogCacheDirectory = catalogCacheDirectory;
        return this;
    }

    @Min(0)
    public Integer getCatalogCacheTtlMinutes()
    {
        return catalogCacheTtlMinutes;
    }

    @Config("catalog-cache-ttl-minutes")
    @ConfigDescription("How long a catalog on disk is used for; 0 keeps nothing on disk "
            + "(adapter default: 1440)")
    public ServiceNowConfig setCatalogCacheTtlMinutes(Integer catalogCacheTtlMinutes)
    {
        this.catalogCacheTtlMinutes = catalogCacheTtlMinutes;
        return this;
    }

    public String getPushdownVerification()
    {
        return pushdownVerification;
    }

    @Config("pushdown-verification")
    @ConfigDescription("Path of a pushdown verification record written by the live harness "
            + "(adapter default: the bundled record, which verifies nothing, so no filter is pushed)")
    public ServiceNowConfig setPushdownVerification(String pushdownVerification)
    {
        this.pushdownVerification = pushdownVerification;
        return this;
    }

    public String getTrustPushdown()
    {
        return trustPushdown;
    }

    @Config("trust-pushdown")
    @ConfigDescription("Comma-separated pushdown entries to trust without a verification record "
            + "(default: none). An entry nobody has proven on this instance can return wrong rows.")
    public ServiceNowConfig setTrustPushdown(String trustPushdown)
    {
        this.trustPushdown = trustPushdown;
        return this;
    }
}
