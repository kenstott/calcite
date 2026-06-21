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

import org.apache.calcite.adapter.splunk.SplunkDriver;
import org.apache.calcite.adapter.trino.CalciteClient;
import org.apache.calcite.adapter.trino.CalciteConnectorConfig;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the Splunk Trino connector. Reuses {@link CalciteClient} (Splunk is exposed over
 * Calcite via standard JDBC types) and supplies a {@link ConnectionFactory} backed by
 * {@code SplunkDriver}.
 *
 * <p>The friendly catalog properties ({@code url}, {@code token}, ...) from {@link SplunkConfig} are
 * assembled into a {@code jdbc:splunk:} URL and installed as the {@link BaseJdbcConfig}
 * {@code connection-url} default, so the user never supplies a raw URL.
 */
public class SplunkClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        SplunkConfig config = buildConfigObject(SplunkConfig.class);
        String connectionUrl = buildConnectionUrl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly Splunk properties.
        configBinder(binder).bindConfigDefaults(
                BaseJdbcConfig.class, jdbcConfig -> jdbcConfig.setConnectionUrl(connectionUrl));
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
        // Splunk schema/table names are generated lower-case, so case-insensitive name matching is
        // mandatory; fail fast with an actionable message otherwise (see CalciteConnectorConfig).
        CalciteConnectorConfig.requireCaseInsensitiveNameMatching(
                buildConfigObject(MappingConfig.class).isCaseInsensitiveNameMatching(), "Splunk");
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        return DriverConnectionFactory.builder(
                        new SplunkDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    /**
     * Assembles a {@code jdbc:splunk:key=value;...} URL. Values are URL-encoded because
     * {@code SplunkDriver} URL-decodes each parameter value (and splits on {@code ;}).
     */
    private static String buildConnectionUrl(SplunkConfig config)
    {
        List<String> params = new ArrayList<>();
        addParam(params, "url", config.getUrl());
        addParam(params, "token", config.getToken());
        addParam(params, "user", config.getUser());
        addParam(params, "password", config.getPassword());
        addParam(params, "app", config.getApp());
        addParam(params, "datamodelFilter", config.getDatamodelFilter());
        if (config.isDisableSslValidation()) {
            params.add("disableSslValidation=true");
        }
        return "jdbc:splunk:" + String.join(";", params);
    }

    private static void addParam(List<String> params, String key, String value)
    {
        if (value != null && !value.isEmpty()) {
            params.add(key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
