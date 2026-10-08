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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import org.apache.calcite.adapter.servicenow.ServiceNowDriver;
import org.apache.calcite.adapter.trino.AutoCommitConnectionFactory;
import org.apache.calcite.adapter.trino.CalciteClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the ServiceNow Trino connector. Reuses {@link CalciteClient} (tables are exposed
 * over Calcite via standard JDBC types) and supplies a {@link ConnectionFactory} backed by
 * {@code ServiceNowDriver}.
 *
 * <p>The friendly catalog properties ({@code instance-url}, {@code username}, ...) from
 * {@link ServiceNowConfig} are assembled into a {@code jdbc:servicenow:} URL and installed as the
 * {@link BaseJdbcConfig} {@code connection-url} default, so the user never supplies a raw URL.
 *
 * <p>Unlike the Salesforce connector this does not require case-insensitive name matching:
 * ServiceNow table and column names are already lower case, which is what Trino looks up.
 */
public class ServiceNowClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServiceNowConfig config = buildConfigObject(ServiceNowConfig.class);
        validate(config);
        String connectionUrl = buildConnectionUrl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly ServiceNow properties.
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
                        new ServiceNowDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build());
    }

    static void validate(ServiceNowConfig config)
    {
        List<String> missing = new ArrayList<>();
        requireField(missing, "instance-url", config.getInstanceUrl());
        if (!"basic".equalsIgnoreCase(config.getAuthType())) {
            throw new IllegalArgumentException("Unsupported ServiceNow auth-type '"
                    + config.getAuthType() + "'; only 'basic' is implemented");
        }
        requireField(missing, "username", config.getUsername());
        requireField(missing, "password", config.getPassword());
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing ServiceNow catalog properties: " + String.join(", ", missing));
        }
    }

    /**
     * Assembles a {@code jdbc:servicenow:key=value;...} URL. Values are URL-encoded because
     * {@code ServiceNowDriver} URL-decodes each parameter value (and splits on {@code ;}).
     */
    static String buildConnectionUrl(ServiceNowConfig config)
    {
        List<String> params = new ArrayList<>();
        addParam(params, "instanceUrl", config.getInstanceUrl());
        addParam(params, "authType", config.getAuthType());
        addParam(params, "username", config.getUsername());
        addParam(params, "password", config.getPassword());
        addParam(params, "tables", config.getTables());
        addParam(params, "excludeColumnTypes", config.getExcludeColumnTypes());
        addParam(params, "schema", config.getSchema());
        addParam(params, "pushdownVerification", config.getPushdownVerification());
        addParam(params, "trustPushdown", config.getTrustPushdown());
        addParam(params, "pageSize", config.getPageSize());
        addParam(params, "maxConcurrentRequests", config.getMaxConcurrentRequests());
        addParam(params, "maxRetries", config.getMaxRetries());
        addParam(params, "maxRetryWaitSeconds", config.getMaxRetryWaitSeconds());
        addParam(params, "catalogCacheDirectory", config.getCatalogCacheDirectory());
        addParam(params, "catalogCacheTtlMinutes", config.getCatalogCacheTtlMinutes());
        return "jdbc:servicenow:" + String.join(";", params);
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

    private static void addParam(List<String> params, String key, Integer value)
    {
        if (value != null) {
            addParam(params, key, value.toString());
        }
    }

    private static void addParam(List<String> params, String key, String value)
    {
        if (isSet(value)) {
            params.add(key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
