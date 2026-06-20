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

import org.apache.calcite.adapter.ops.CloudOpsDriver;
import org.apache.calcite.adapter.trino.AutoCommitConnectionFactory;
import org.apache.calcite.adapter.trino.CalciteClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the Cloud Ops Trino connector. Reuses {@link CalciteClient} (the CloudOps tables
 * are exposed over Calcite via standard JDBC types) and supplies a {@link ConnectionFactory} backed
 * by {@code CloudOpsDriver}.
 *
 * <p>The friendly catalog properties ({@code azure.tenant-id}, {@code aws.access-key-id}, ...) from
 * {@link CloudOpsConfig} are assembled into a {@code jdbc:cloudops:} URL and installed as the
 * {@link BaseJdbcConfig} {@code connection-url} default, so the user never supplies a raw URL.
 */
public class CloudOpsClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        CloudOpsConfig config = buildConfigObject(CloudOpsConfig.class);
        validate(config);
        String connectionUrl = buildConnectionUrl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly CloudOps properties.
        configBinder(binder).bindConfigDefaults(
                BaseJdbcConfig.class, jdbcConfig -> jdbcConfig.setConnectionUrl(connectionUrl));
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
        // CloudOps table/column names are matched case-sensitively by Calcite, so the catalog should
        // set case-insensitive-name-matching=true (see CalciteClientModule for details).
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
                        new CloudOpsDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build());
    }

    /**
     * Fails fast at startup on credential combinations the CloudOps adapter would otherwise silently
     * ignore (dropping a partially-configured provider, then erroring with "at least one provider").
     * A provider is "enabled" by its gating property: {@code azure.tenant-id}, {@code aws.access-key-id}
     * or {@code gcp.credentials-path}.
     */
    static void validate(CloudOpsConfig config)
    {
        boolean azure = isSet(config.getAzureTenantId());
        boolean gcp = isSet(config.getGcpCredentialsPath());
        boolean aws = isSet(config.getAwsAccessKeyId());

        if (!azure && !gcp && !aws) {
            throw new IllegalArgumentException(
                    "Configure at least one cloud provider: set azure.tenant-id, aws.access-key-id, "
                            + "or gcp.credentials-path");
        }

        List<String> missing = new ArrayList<>();
        if (azure) {
            requireField(missing, "azure.client-id", config.getAzureClientId());
            requireField(missing, "azure.client-secret", config.getAzureClientSecret());
            requireField(missing, "azure.subscription-ids", config.getAzureSubscriptionIds());
        }
        if (gcp) {
            requireField(missing, "gcp.project-ids", config.getGcpProjectIds());
        }
        if (aws) {
            requireField(missing, "aws.account-ids", config.getAwsAccountIds());
            requireField(missing, "aws.region", config.getAwsRegion());
            requireField(missing, "aws.secret-access-key", config.getAwsSecretAccessKey());
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "CloudOps connector is missing required properties: " + String.join(", ", missing));
        }
    }

    /**
     * Assembles a {@code jdbc:cloudops:key=value;...} URL whose keys are the flat dotted operand keys
     * the CloudOps adapter reads. Values are URL-encoded because {@code CloudOpsDriver} splits the
     * remainder on {@code ;} and URL-decodes each value (so commas in list values round-trip).
     */
    static String buildConnectionUrl(CloudOpsConfig config)
    {
        List<String> params = new ArrayList<>();
        addParam(params, "providers", config.getProviders());

        addParam(params, "azure.tenantId", config.getAzureTenantId());
        addParam(params, "azure.clientId", config.getAzureClientId());
        addParam(params, "azure.clientSecret", config.getAzureClientSecret());
        addParam(params, "azure.subscriptionIds", config.getAzureSubscriptionIds());

        addParam(params, "gcp.projectIds", config.getGcpProjectIds());
        addParam(params, "gcp.credentialsPath", config.getGcpCredentialsPath());

        addParam(params, "aws.accountIds", config.getAwsAccountIds());
        addParam(params, "aws.region", config.getAwsRegion());
        addParam(params, "aws.accessKeyId", config.getAwsAccessKeyId());
        addParam(params, "aws.secretAccessKey", config.getAwsSecretAccessKey());
        addParam(params, "aws.roleArn", config.getAwsRoleArn());

        addParam(params, "cache.enabled", asString(config.getCacheEnabled()));
        addParam(params, "cache.ttlMinutes", asString(config.getCacheTtlMinutes()));
        addParam(params, "cache.debugMode", asString(config.getCacheDebugMode()));

        return "jdbc:cloudops:" + String.join(";", params);
    }

    private static void requireField(List<String> missing, String name, String value)
    {
        if (!isSet(value)) {
            missing.add(name);
        }
    }

    private static boolean isSet(String value)
    {
        return value != null && !value.trim().isEmpty();
    }

    private static String asString(Object value)
    {
        return value == null ? null : value.toString();
    }

    private static void addParam(List<String> params, String key, String value)
    {
        if (value != null && !value.isEmpty()) {
            params.add(key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
