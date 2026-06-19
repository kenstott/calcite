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
package org.apache.calcite.adapter.sharepoint.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import jakarta.validation.constraints.NotNull;

/**
 * Catalog configuration for the SharePoint Trino connector. These friendly properties are mapped
 * onto a {@code jdbc:sharepoint:} URL for {@code SharePointListDriver}, so the user never writes a
 * raw JDBC URL. Mirrors the parameters parsed by the SharePoint JDBC driver.
 */
public class SharePointConfig
{
    private String siteUrl;
    private String authType;
    private String clientId;
    private String clientSecret;
    private String tenantId;
    private String user;
    private String password;
    private String certificatePath;
    private String certificatePassword;
    private String thumbprint;
    private String schema;
    private String metadataCacheTtl;

    @NotNull
    public String getSiteUrl()
    {
        return siteUrl;
    }

    @Config("site-url")
    @ConfigDescription("Full SharePoint site URL, e.g. https://contoso.sharepoint.com/sites/mysite")
    public SharePointConfig setSiteUrl(String siteUrl)
    {
        this.siteUrl = siteUrl;
        return this;
    }

    @NotNull
    public String getAuthType()
    {
        return authType;
    }

    @Config("auth-type")
    @ConfigDescription("CLIENT_CREDENTIALS, USERNAME_PASSWORD, DEVICE_CODE, MANAGED_IDENTITY or CERTIFICATE")
    public SharePointConfig setAuthType(String authType)
    {
        this.authType = authType;
        return this;
    }

    public String getClientId()
    {
        return clientId;
    }

    @Config("client-id")
    @ConfigDescription("Azure AD app registration client ID")
    public SharePointConfig setClientId(String clientId)
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
    @ConfigDescription("Client secret (CLIENT_CREDENTIALS auth)")
    public SharePointConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    public String getTenantId()
    {
        return tenantId;
    }

    @Config("tenant-id")
    @ConfigDescription("Azure AD tenant ID")
    public SharePointConfig setTenantId(String tenantId)
    {
        this.tenantId = tenantId;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("user")
    @ConfigDescription("User principal name (USERNAME_PASSWORD auth)")
    public SharePointConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password (USERNAME_PASSWORD auth)")
    public SharePointConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getCertificatePath()
    {
        return certificatePath;
    }

    @Config("certificate-path")
    @ConfigDescription("Path to the PKCS#12 (.pfx) certificate file (CERTIFICATE auth)")
    public SharePointConfig setCertificatePath(String certificatePath)
    {
        this.certificatePath = certificatePath;
        return this;
    }

    public String getCertificatePassword()
    {
        return certificatePassword;
    }

    @Config("certificate-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the PKCS#12 certificate (CERTIFICATE auth)")
    public SharePointConfig setCertificatePassword(String certificatePassword)
    {
        this.certificatePassword = certificatePassword;
        return this;
    }

    public String getThumbprint()
    {
        return thumbprint;
    }

    @Config("thumbprint")
    @ConfigDescription("Optional certificate SHA-1 thumbprint; omit to auto-compute from the .pfx")
    public SharePointConfig setThumbprint(String thumbprint)
    {
        this.thumbprint = thumbprint;
        return this;
    }

    public String getSchema()
    {
        return schema;
    }

    @Config("schema")
    @ConfigDescription("Schema name exposed for the SharePoint lists (default: sharepoint)")
    public SharePointConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    public String getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("metadata-cache-ttl")
    @ConfigDescription("List-discovery cache TTL in seconds (default 300; -1 = never expire, 0 = disabled)")
    public SharePointConfig setMetadataCacheTtl(String metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }
}
