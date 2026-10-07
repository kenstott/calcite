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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.mapping.MappingConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import org.apache.calcite.adapter.salesforce.SalesforceDriver;
import org.apache.calcite.adapter.trino.AutoCommitConnectionFactory;
import org.apache.calcite.adapter.trino.CalciteClient;
import org.apache.calcite.adapter.trino.CalciteConnectorConfig;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the Salesforce Trino connector. Reuses {@link CalciteClient} (sObjects are
 * exposed over Calcite via standard JDBC types) and supplies a {@link ConnectionFactory} backed by
 * {@code SalesforceDriver}.
 *
 * <p>The friendly catalog properties ({@code login-url}, {@code client-id}, ...) from
 * {@link SalesforceConfig} are assembled into a {@code jdbc:salesforce:} URL and installed as the
 * {@link BaseJdbcConfig} {@code connection-url} default, so the user never supplies a raw URL.
 */
public class SalesforceClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        SalesforceConfig config = buildConfigObject(SalesforceConfig.class);
        validate(config);
        // sObject names are mixed case (Account, OpportunityLineItem); Trino lower-cases
        // identifiers, so case-insensitive name matching is mandatory (see CalciteConnectorConfig).
        CalciteConnectorConfig.requireCaseInsensitiveNameMatching(
                buildConfigObject(MappingConfig.class).isCaseInsensitiveNameMatching(), "Salesforce");
        String connectionUrl = buildConnectionUrl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly Salesforce properties.
        configBinder(binder).bindConfigDefaults(
                BaseJdbcConfig.class, jdbcConfig -> jdbcConfig.setConnectionUrl(connectionUrl));
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        return new AutoCommitConnectionFactory(
                DriverConnectionFactory.builder(
                        new SalesforceDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build());
    }

    static void validate(SalesforceConfig config)
    {
        boolean accessToken = isSet(config.getAccessToken());
        boolean usernamePassword = isSet(config.getUsername()) || isSet(config.getPassword());
        boolean clientCredentials = isSet(config.getClientId()) || isSet(config.getClientSecret());

        List<String> missing = new ArrayList<>();
        if (accessToken) {
            requireField(missing, "instance-url", config.getInstanceUrl());
        }
        else if (usernamePassword) {
            requireField(missing, "username", config.getUsername());
            requireField(missing, "password", config.getPassword());
            requireField(missing, "client-id", config.getClientId());
            requireField(missing, "client-secret", config.getClientSecret());
        }
        else if (clientCredentials) {
            requireField(missing, "client-id", config.getClientId());
            requireField(missing, "client-secret", config.getClientSecret());
        }
        else {
            throw new IllegalArgumentException(
                    "Configure Salesforce credentials: client-id + client-secret, "
                            + "username + password (+ security-token) with client-id + client-secret, "
                            + "or access-token + instance-url");
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing Salesforce catalog properties: " + String.join(", ", missing));
        }
    }

    /**
     * Assembles a {@code jdbc:salesforce:key=value;...} URL. Values are URL-encoded because
     * {@code SalesforceDriver} URL-decodes each parameter value (and splits on {@code ;}).
     */
    static String buildConnectionUrl(SalesforceConfig config)
    {
        List<String> params = new ArrayList<>();
        addParam(params, "loginUrl", config.getLoginUrl());
        addParam(params, "clientId", config.getClientId());
        addParam(params, "clientSecret", config.getClientSecret());
        addParam(params, "username", config.getUsername());
        addParam(params, "password", config.getPassword());
        addParam(params, "securityToken", config.getSecurityToken());
        addParam(params, "accessToken", config.getAccessToken());
        addParam(params, "instanceUrl", config.getInstanceUrl());
        addParam(params, "apiVersion", config.getApiVersion());
        addParam(params, "schema", config.getSchema());
        if (config.getCacheMaxSize() != null) {
            addParam(params, "cacheMaxSize", config.getCacheMaxSize().toString());
        }
        addParam(params, "describeCacheDirectory", config.getDescribeCacheDirectory());
        if (config.getDescribeCacheTtlMinutes() != null) {
            addParam(params, "describeCacheTtlMinutes", config.getDescribeCacheTtlMinutes().toString());
        }
        // Trino matches names case-insensitively, so the adapter's lower-case duplicate of each
        // sObject name (Account + account) would be ambiguous.
        params.add("lowercaseAliases=false");
        return "jdbc:salesforce:" + String.join(";", params);
    }

    private static boolean isSet(String value)
    {
        return value != null && !value.isEmpty();
    }

    private static void requireField(List<String> missing, String name, String value)
    {
        if (!isSet(value)) {
            missing.add(name);
        }
    }

    private static void addParam(List<String> params, String key, String value)
    {
        if (isSet(value)) {
            params.add(key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
