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
package org.apache.calcite.adapter.file.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

import org.apache.calcite.adapter.trino.AutoCommitConnectionFactory;
import org.apache.calcite.adapter.trino.CalciteClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the file Trino connector. Reuses {@link CalciteClient} (the file adapter exposes
 * tables over Calcite via standard JDBC types) and supplies a {@link ConnectionFactory} backed by
 * Calcite's JDBC driver pointed at an inline model.
 *
 * <p>The friendly catalog properties from {@link FileConfig} are assembled into an inline Calcite
 * model ({@code jdbc:calcite:model=inline:{...}}) that instantiates a single
 * {@code FileSchemaFactory} schema, and installed as the {@link BaseJdbcConfig}
 * {@code connection-url} default, so the user never supplies a raw URL.
 *
 * <p>S3 credentials are resolved here at the connector layer (catalog override, then the standard
 * AWS environment variables) and passed <em>explicitly</em> into the adapter's {@code storageConfig};
 * the file adapter itself never reads the environment.
 */
public class FileClientModule
        extends AbstractConfigurationAwareModule
{
    static final String FILE_SCHEMA_FACTORY =
            "org.apache.calcite.adapter.file.FileSchemaFactory";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    protected void setup(Binder binder)
    {
        FileConfig config = buildConfigObject(FileConfig.class);
        String connectionUrl = buildConnectionUrl(config, System::getenv);
        io.airlift.units.Duration catalogTtl = catalogCacheTtl(config);
        // Satisfy BaseJdbcConfig's mandatory connection-url (bound by the framework's JdbcModule)
        // from the friendly file properties, and set the metadata cache TTL that governs how often
        // Trino re-scans the mount for new tables. An explicit metadata.cache-ttl catalog property
        // still wins over this default.
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, jdbcConfig -> {
            jdbcConfig.setConnectionUrl(connectionUrl);
            if (catalogTtl != null) {
                jdbcConfig.setMetadataCacheTtl(catalogTtl);
            }
        });
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
        // File table/column names are matched case-sensitively by Calcite, so the catalog should
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
                        new org.apache.calcite.jdbc.Driver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build());
    }

    /**
     * Builds {@code jdbc:calcite:model=inline:<json>} for the configured glob. {@code env} supplies
     * environment-variable values (production passes {@code System::getenv}; tests pass a fake).
     */
    static String buildConnectionUrl(FileConfig config, Function<String, String> env)
    {
        return "jdbc:calcite:model=inline:" + buildModelJson(config, env);
    }

    /** Builds the inline Calcite model JSON wiring one {@code FileSchemaFactory} schema. */
    static String buildModelJson(FileConfig config, Function<String, String> env)
    {
        String glob = config.getGlob();
        String[] split = splitGlob(glob);
        String directory = split[0];
        String pattern = split[1];

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("directory", directory);
        if (pattern != null) {
            operand.put("glob", pattern);
        }
        operand.put("recursive", config.isRecursive());
        operand.put("executionEngine", config.getExecutionEngine());
        if (config.getRefreshInterval() != null) {
            operand.put("refreshInterval", config.getRefreshInterval());
        }

        // Derive the storage backend from the URI scheme. The file adapter only auto-detects
        // s3/http/hdfs from the directory, so ftp/ftps/sftp must be set explicitly here.
        String scheme = schemeOf(glob);
        if (scheme != null) {
            operand.put("storageType", scheme);
            if ("s3".equals(scheme)) {
                operand.put("storageConfig", buildS3StorageConfig(config, env));
            }
            else if ("sftp".equals(scheme)) {
                Map<String, Object> sftp = buildSftpStorageConfig(config);
                if (sftp != null) {
                    operand.put("storageConfig", sftp);
                }
            }
            // ftp/ftps carry credentials in the URI; hdfs uses the ambient Hadoop config;
            // http/https need no credentials — no storageConfig for those.
        }

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("name", config.getSchemaName());
        schema.put("type", "custom");
        schema.put("factory", FILE_SCHEMA_FACTORY);
        schema.put("operand", operand);

        Map<String, Object> model = new LinkedHashMap<>();
        model.put("version", "1.0");
        model.put("defaultSchema", config.getSchemaName());
        model.put("schemas", java.util.Collections.singletonList(schema));

        try {
            return MAPPER.writeValueAsString(model);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize inline Calcite model", e);
        }
    }

    /**
     * Resolves S3 credentials/region/endpoint, preferring the catalog override over the standard
     * AWS environment variable. Fails fast when access key or secret key resolve to nothing — the
     * file adapter requires explicit credentials and does not read the environment itself.
     */
    static Map<String, Object> buildS3StorageConfig(FileConfig config, Function<String, String> env)
    {
        String accessKey = resolve(config.getAwsAccessKey(), env, "AWS_ACCESS_KEY_ID");
        String secretKey = resolve(config.getAwsSecretKey(), env, "AWS_SECRET_ACCESS_KEY");
        if (accessKey == null || secretKey == null) {
            throw new IllegalArgumentException(
                    "S3 glob '" + config.getGlob() + "' requires AWS credentials. Set the "
                    + "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables or the "
                    + "aws.access-key / aws.secret-key catalog properties.");
        }
        String region = resolve(config.getAwsRegion(), env, "AWS_REGION");
        if (region == null) {
            region = resolve(null, env, "AWS_DEFAULT_REGION");
        }
        String endpoint = resolve(config.getAwsEndpoint(), env, "AWS_ENDPOINT_URL");

        Map<String, Object> storageConfig = new LinkedHashMap<>();
        storageConfig.put("accessKeyId", accessKey);
        storageConfig.put("secretAccessKey", secretKey);
        if (region != null) {
            storageConfig.put("region", region);
        }
        if (endpoint != null) {
            storageConfig.put("endpoint", endpoint);
        }
        return storageConfig;
    }

    /**
     * Resolves SFTP credentials from the catalog properties. Returns {@code null} when no explicit
     * credential is configured, so the file adapter falls back to credentials carried in the
     * {@code sftp://} URI. {@code strictHostKeyChecking} is only meaningful alongside a connection,
     * so it is included only when explicit credentials are present.
     */
    static Map<String, Object> buildSftpStorageConfig(FileConfig config)
    {
        boolean hasCredential = config.getSftpUsername() != null
                || config.getSftpPassword() != null
                || config.getSftpPrivateKeyPath() != null;
        if (!hasCredential) {
            return null;
        }
        Map<String, Object> storageConfig = new LinkedHashMap<>();
        if (config.getSftpUsername() != null) {
            storageConfig.put("username", config.getSftpUsername());
        }
        if (config.getSftpPassword() != null) {
            storageConfig.put("password", config.getSftpPassword());
        }
        if (config.getSftpPrivateKeyPath() != null) {
            storageConfig.put("privateKeyPath", config.getSftpPrivateKeyPath());
        }
        storageConfig.put("strictHostKeyChecking", config.isSftpStrictHostKeyChecking());
        return storageConfig;
    }

    /**
     * Resolves the Trino metadata-cache TTL — how long the catalog caches the table/column list
     * before a fresh Calcite connection re-scans the mount for new files/sheets. Prefers the
     * explicit {@code catalog-refresh-interval}; otherwise derives it from {@code refresh-interval}
     * so the table list and the data stay in step. Returns {@code null} when neither is set, leaving
     * the framework default.
     */
    static io.airlift.units.Duration catalogCacheTtl(FileConfig config)
    {
        if (config.getCatalogRefreshInterval() != null) {
            return config.getCatalogRefreshInterval();
        }
        if (config.getRefreshInterval() != null) {
            java.time.Duration d =
                    org.apache.calcite.adapter.file.refresh.RefreshInterval.parse(config.getRefreshInterval());
            if (d != null) {
                return new io.airlift.units.Duration(d.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        }
        return null;
    }

    /**
     * Returns the lower-cased URI scheme of {@code spec} (the text before {@code ://}), or
     * {@code null} when there is none — a plain local path, which the file adapter treats as
     * {@code local} storage.
     */
    static String schemeOf(String spec)
    {
        int idx = spec.indexOf("://");
        return idx > 0 ? spec.substring(0, idx).toLowerCase(java.util.Locale.ROOT) : null;
    }

    private static String resolve(String override, Function<String, String> env, String envName)
    {
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String value = env.apply(envName);
        return (value != null && !value.isEmpty()) ? value : null;
    }

    /**
     * Splits a glob spec into a base directory and an optional glob pattern. The base is everything
     * up to (and excluding) the last {@code /} before the first glob metacharacter
     * ({@code * ? [ &#123;}); the pattern is the remainder. A spec with no metacharacters is treated
     * as a plain directory (pattern {@code null}). Preserves a leading {@code scheme://}.
     *
     * <p>Examples: {@code s3://b/data/**}{@code /*.parquet} → {@code ["s3://b/data", "**}{@code /*.parquet"]};
     * {@code /mnt/files/*.csv} → {@code ["/mnt/files", "*.csv"]}; {@code /mnt/files} →
     * {@code ["/mnt/files", null]}.
     */
    static String[] splitGlob(String spec)
    {
        int meta = -1;
        for (int i = 0; i < spec.length(); i++) {
            char c = spec.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                meta = i;
                break;
            }
        }
        if (meta < 0) {
            return new String[] {spec, null};
        }
        int slash = spec.lastIndexOf('/', meta);
        if (slash < 0) {
            return new String[] {spec, null};
        }
        return new String[] {spec.substring(0, slash), spec.substring(slash + 1)};
    }
}
