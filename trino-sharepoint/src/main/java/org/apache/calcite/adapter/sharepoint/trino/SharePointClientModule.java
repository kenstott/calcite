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

import org.apache.calcite.adapter.sharepoint.SharePointListDriver;
import org.apache.calcite.adapter.trino.CalciteClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the SharePoint Trino connector. Reuses {@link CalciteClient} (the SharePoint
 * lists are exposed over Calcite via standard JDBC types) and supplies a {@link ConnectionFactory}
 * backed by {@code SharePointListDriver}.
 *
 * <p>The friendly catalog properties ({@code site-url}, {@code auth-type}, ...) from
 * {@link SharePointConfig} are assembled into a {@code jdbc:sharepoint:} URL and installed as the
 * {@link BaseJdbcConfig} {@code connection-url} default, so the user never supplies a raw URL.
 */
public class SharePointClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        SharePointConfig config = buildConfigObject(SharePointConfig.class);
        String connectionUrl = buildConnectionUrl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly SharePoint properties.
        configBinder(binder).bindConfigDefaults(
                BaseJdbcConfig.class, jdbcConfig -> jdbcConfig.setConnectionUrl(connectionUrl));
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
        // SharePoint list/column names are matched case-sensitively by Calcite, so the catalog
        // should set case-insensitive-name-matching=true (see CalciteClientModule for details).
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
                        new SharePointListDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    /**
     * Assembles a {@code jdbc:sharepoint:key=value;...} URL. Values are URL-encoded because
     * {@code SharePointListDriver} URL-decodes each parameter value (and splits on {@code ;}).
     */
    private static String buildConnectionUrl(SharePointConfig config)
    {
        List<String> params = new ArrayList<>();
        addParam(params, "siteUrl", config.getSiteUrl());
        addParam(params, "authType", config.getAuthType());
        addParam(params, "clientId", config.getClientId());
        addParam(params, "clientSecret", config.getClientSecret());
        addParam(params, "tenantId", config.getTenantId());
        addParam(params, "user", config.getUser());
        addParam(params, "password", config.getPassword());
        return "jdbc:sharepoint:" + String.join(";", params);
    }

    private static void addParam(List<String> params, String key, String value)
    {
        if (value != null && !value.isEmpty()) {
            params.add(key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
