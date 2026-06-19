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
package org.apache.calcite.adapter.splunk.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import jakarta.validation.constraints.NotNull;

/**
 * Catalog configuration for the Splunk Trino connector. These friendly properties are mapped onto a
 * {@code jdbc:splunk:} URL for {@code SplunkDriver}, so the user never writes a raw JDBC URL.
 * Authenticate with either {@code token} or {@code user} + {@code password}.
 */
public class SplunkConfig
{
    private String url;
    private String token;
    private String user;
    private String password;
    private String app;
    private String datamodelFilter;
    private boolean disableSslValidation;

    @NotNull
    public String getUrl()
    {
        return url;
    }

    @Config("url")
    @ConfigDescription("Splunk management URL, e.g. https://localhost:8089")
    public SplunkConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public String getToken()
    {
        return token;
    }

    @Config("token")
    @ConfigSecuritySensitive
    @ConfigDescription("Splunk authentication token (preferred over user/password)")
    public SplunkConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("user")
    @ConfigDescription("Splunk username (when not using a token)")
    public SplunkConfig setUser(String user)
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
    @ConfigDescription("Splunk password (when not using a token)")
    public SplunkConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getApp()
    {
        return app;
    }

    @Config("app")
    @ConfigDescription("Splunk app context for data model discovery, e.g. Splunk_SA_CIM")
    public SplunkConfig setApp(String app)
    {
        this.app = app;
        return this;
    }

    public String getDatamodelFilter()
    {
        return datamodelFilter;
    }

    @Config("datamodel-filter")
    @ConfigDescription("Filter for data model discovery (name, glob, or /regex/)")
    public SplunkConfig setDatamodelFilter(String datamodelFilter)
    {
        this.datamodelFilter = datamodelFilter;
        return this;
    }

    public boolean isDisableSslValidation()
    {
        return disableSslValidation;
    }

    @Config("disable-ssl-validation")
    @ConfigDescription("Disable TLS certificate validation (test/self-signed servers only)")
    public SplunkConfig setDisableSslValidation(boolean disableSslValidation)
    {
        this.disableSslValidation = disableSslValidation;
        return this;
    }
}
