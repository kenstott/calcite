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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;

import jakarta.validation.constraints.NotNull;

/**
 * Catalog configuration for the file Trino connector. The single mandatory {@code glob} points the
 * Calcite file adapter at one mount; need more mounts → configure additional catalogs. The mount
 * may be a local directory/path or a remote URI whose scheme selects the storage backend:
 * {@code s3://}, {@code hdfs://}, {@code ftp://} / {@code ftps://}, {@code sftp://},
 * {@code http://} / {@code https://}.
 *
 * <p>S3 credentials default to the standard AWS environment variables (resolved in
 * {@link FileClientModule}); the {@code aws.*} properties here override them per catalog. FTP/FTPS
 * carry their credentials in the URI ({@code ftp://user:pass@host/path}); HDFS uses the ambient
 * Hadoop configuration. SFTP can take explicit credentials via the {@code sftp.*} properties (a
 * password is rarely expressible in an {@code sftp://} URI) or fall back to the URI.
 */
public class FileConfig
{
    private String glob;
    private String schemaName = "files";
    private String executionEngine = "DUCKDB";
    private boolean recursive = true;
    private String awsAccessKey;
    private String awsSecretKey;
    private String awsRegion;
    private String awsEndpoint;
    private String sftpUsername;
    private String sftpPassword;
    private String sftpPrivateKeyPath;
    private boolean sftpStrictHostKeyChecking;
    private String refreshInterval;
    private Duration catalogRefreshInterval;

    @NotNull
    public String getGlob()
    {
        return glob;
    }

    @Config("glob")
    @ConfigDescription("Local directory/path or an s3://bucket/prefix glob to expose as tables")
    public FileConfig setGlob(String glob)
    {
        this.glob = glob;
        return this;
    }

    @NotNull
    public String getSchemaName()
    {
        return schemaName;
    }

    @Config("schema-name")
    @ConfigDescription("Schema name exposed for the files (default: files)")
    public FileConfig setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    @NotNull
    public String getExecutionEngine()
    {
        return executionEngine;
    }

    @Config("execution-engine")
    @ConfigDescription("File adapter execution engine: DUCKDB (default), PARQUET, LINQ4J or ARROW. "
            + "DUCKDB reads CSV/Parquet natively without Hadoop; PARQUET converts via Hadoop, which "
            + "is incompatible with the JDK 25 that Trino requires.")
    public FileConfig setExecutionEngine(String executionEngine)
    {
        this.executionEngine = executionEngine;
        return this;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    @Config("recursive")
    @ConfigDescription("Scan subdirectories recursively (default: true)")
    public FileConfig setRecursive(boolean recursive)
    {
        this.recursive = recursive;
        return this;
    }

    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("aws.access-key")
    @ConfigDescription("Overrides AWS_ACCESS_KEY_ID for s3:// globs")
    public FileConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("aws.secret-key")
    @ConfigSecuritySensitive
    @ConfigDescription("Overrides AWS_SECRET_ACCESS_KEY for s3:// globs")
    public FileConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("aws.region")
    @ConfigDescription("Overrides AWS_REGION / AWS_DEFAULT_REGION for s3:// globs")
    public FileConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public String getAwsEndpoint()
    {
        return awsEndpoint;
    }

    @Config("aws.endpoint")
    @ConfigDescription("Overrides AWS_ENDPOINT_URL for S3-compatible stores (MinIO, R2)")
    public FileConfig setAwsEndpoint(String awsEndpoint)
    {
        this.awsEndpoint = awsEndpoint;
        return this;
    }

    public String getSftpUsername()
    {
        return sftpUsername;
    }

    @Config("sftp.username")
    @ConfigDescription("Username for an sftp:// mount (overrides any user in the URI)")
    public FileConfig setSftpUsername(String sftpUsername)
    {
        this.sftpUsername = sftpUsername;
        return this;
    }

    public String getSftpPassword()
    {
        return sftpPassword;
    }

    @Config("sftp.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for an sftp:// mount")
    public FileConfig setSftpPassword(String sftpPassword)
    {
        this.sftpPassword = sftpPassword;
        return this;
    }

    public String getSftpPrivateKeyPath()
    {
        return sftpPrivateKeyPath;
    }

    @Config("sftp.private-key-path")
    @ConfigDescription("Path to a private key file for sftp:// key-based auth")
    public FileConfig setSftpPrivateKeyPath(String sftpPrivateKeyPath)
    {
        this.sftpPrivateKeyPath = sftpPrivateKeyPath;
        return this;
    }

    public boolean isSftpStrictHostKeyChecking()
    {
        return sftpStrictHostKeyChecking;
    }

    @Config("sftp.strict-host-key-checking")
    @ConfigDescription("Enforce SSH known-hosts verification for sftp:// mounts (default: false)")
    public FileConfig setSftpStrictHostKeyChecking(boolean sftpStrictHostKeyChecking)
    {
        this.sftpStrictHostKeyChecking = sftpStrictHostKeyChecking;
        return this;
    }

    public String getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("refresh-interval")
    @ConfigDescription("How often the file adapter re-reads data, e.g. \"5 minutes\" or \"PT5M\". "
            + "Also the default cadence at which Trino re-scans for new tables (see "
            + "catalog-refresh-interval).")
    public FileConfig setRefreshInterval(String refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    public Duration getCatalogRefreshInterval()
    {
        return catalogRefreshInterval;
    }

    @Config("catalog-refresh-interval")
    @ConfigDescription("How long Trino caches the table/column list before re-scanning the mount "
            + "for new files/sheets (maps to metadata.cache-ttl). Defaults to refresh-interval.")
    public FileConfig setCatalogRefreshInterval(Duration catalogRefreshInterval)
    {
        this.catalogRefreshInterval = catalogRefreshInterval;
        return this;
    }
}
