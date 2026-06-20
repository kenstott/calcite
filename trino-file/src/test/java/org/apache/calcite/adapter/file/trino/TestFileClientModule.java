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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the inline-model and S3 credential-resolution logic in {@link FileClientModule}.
 * Deterministic and dependency-free: the AWS environment is supplied as a fake lookup function.
 */
class TestFileClientModule
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Function<String, String> NO_ENV = name -> null;

    private static Function<String, String> env(Map<String, String> vars)
    {
        return vars::get;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> firstSchemaOperand(String modelJson)
            throws Exception
    {
        Map<String, Object> model = MAPPER.readValue(modelJson, Map.class);
        java.util.List<Map<String, Object>> schemas =
                (java.util.List<Map<String, Object>>) model.get("schemas");
        return (Map<String, Object>) schemas.get(0).get("operand");
    }

    @Test
    void testLocalGlobModel()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("/mnt/data/*.csv");
        String json = FileClientModule.buildModelJson(config, NO_ENV);

        Map<String, Object> model = MAPPER.readValue(json, Map.class);
        assertEquals("files", model.get("defaultSchema"));

        Map<String, Object> operand = firstSchemaOperand(json);
        assertEquals("/mnt/data", operand.get("directory"));
        assertEquals("*.csv", operand.get("glob"));
        assertEquals("DUCKDB", operand.get("executionEngine"));
        assertEquals(Boolean.TRUE, operand.get("recursive"));
        // No S3 wiring for a local glob.
        assertFalse(operand.containsKey("storageType"));
        assertFalse(operand.containsKey("storageConfig"));
    }

    @Test
    void testLocalDirectoryWithoutPattern()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("/mnt/data").setRecursive(false);
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("/mnt/data", operand.get("directory"));
        assertFalse(operand.containsKey("glob"));
        assertEquals(Boolean.FALSE, operand.get("recursive"));
    }

    @Test
    void testS3GlobWithEnvCredentials()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("s3://my-bucket/data/**/*.parquet");
        Function<String, String> e = env(Map.of(
                "AWS_ACCESS_KEY_ID", "AKIA-env",
                "AWS_SECRET_ACCESS_KEY", "secret-env",
                "AWS_REGION", "us-west-2"));

        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, e));
        assertEquals("s3://my-bucket/data", operand.get("directory"));
        assertEquals("**/*.parquet", operand.get("glob"));
        assertEquals("s3", operand.get("storageType"));

        @SuppressWarnings("unchecked")
        Map<String, Object> sc = (Map<String, Object>) operand.get("storageConfig");
        assertEquals("AKIA-env", sc.get("accessKeyId"));
        assertEquals("secret-env", sc.get("secretAccessKey"));
        assertEquals("us-west-2", sc.get("region"));
        assertFalse(sc.containsKey("endpoint"));
    }

    @Test
    void testCatalogOverrideBeatsEnv()
    {
        FileConfig config = new FileConfig()
                .setGlob("s3://b/p/*.parquet")
                .setAwsAccessKey("AKIA-override")
                .setAwsSecretKey("secret-override")
                .setAwsRegion("eu-central-1")
                .setAwsEndpoint("https://minio.local");
        Function<String, String> e = env(Map.of(
                "AWS_ACCESS_KEY_ID", "AKIA-env",
                "AWS_SECRET_ACCESS_KEY", "secret-env",
                "AWS_REGION", "us-west-2"));

        Map<String, Object> sc = FileClientModule.buildS3StorageConfig(config, e);
        assertEquals("AKIA-override", sc.get("accessKeyId"));
        assertEquals("secret-override", sc.get("secretAccessKey"));
        assertEquals("eu-central-1", sc.get("region"));
        assertEquals("https://minio.local", sc.get("endpoint"));
    }

    @Test
    void testRegionFallsBackToDefaultRegionEnv()
    {
        FileConfig config = new FileConfig().setGlob("s3://b/p/*.parquet");
        Function<String, String> e = env(Map.of(
                "AWS_ACCESS_KEY_ID", "k",
                "AWS_SECRET_ACCESS_KEY", "s",
                "AWS_DEFAULT_REGION", "ap-south-1"));
        assertEquals("ap-south-1", FileClientModule.buildS3StorageConfig(config, e).get("region"));
    }

    @Test
    void testS3WithoutCredentialsThrows()
    {
        FileConfig config = new FileConfig().setGlob("s3://b/p/*.parquet");
        assertThrows(IllegalArgumentException.class,
                () -> FileClientModule.buildModelJson(config, NO_ENV));
    }

    @Test
    void testHdfsGlobSetsStorageTypeWithoutConfig()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("hdfs://namenode/data/*.parquet");
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("hdfs", operand.get("storageType"));
        assertEquals("hdfs://namenode/data", operand.get("directory"));
        assertEquals("*.parquet", operand.get("glob"));
        // HDFS uses the ambient Hadoop config — no storageConfig.
        assertFalse(operand.containsKey("storageConfig"));
    }

    @Test
    void testFtpGlobSetsStorageTypeWithoutConfig()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("ftp://user:pass@host/pub/*.csv");
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("ftp", operand.get("storageType"));
        // FTP credentials ride in the URI — no storageConfig.
        assertFalse(operand.containsKey("storageConfig"));
    }

    @Test
    void testSftpWithoutExplicitCredentialsOmitsConfig()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("sftp://user@host/data/*.json");
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("sftp", operand.get("storageType"));
        // No explicit sftp.* props → fall back to URI auth, no storageConfig.
        assertFalse(operand.containsKey("storageConfig"));
    }

    @Test
    void testSftpWithExplicitCredentials()
            throws Exception
    {
        FileConfig config = new FileConfig()
                .setGlob("sftp://host/data/*.json")
                .setSftpUsername("svc")
                .setSftpPassword("pw")
                .setSftpStrictHostKeyChecking(true);
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("sftp", operand.get("storageType"));

        @SuppressWarnings("unchecked")
        Map<String, Object> sc = (Map<String, Object>) operand.get("storageConfig");
        assertEquals("svc", sc.get("username"));
        assertEquals("pw", sc.get("password"));
        assertEquals(Boolean.TRUE, sc.get("strictHostKeyChecking"));
        assertFalse(sc.containsKey("privateKeyPath"));
    }

    @Test
    void testSchemeOf()
    {
        assertEquals("s3", FileClientModule.schemeOf("s3://b/k"));
        assertEquals("sftp", FileClientModule.schemeOf("sftp://h/p"));
        assertEquals("https", FileClientModule.schemeOf("https://h/p"));
        assertNull(FileClientModule.schemeOf("/mnt/local/path"));
    }

    @Test
    void testSplitGlob()
    {
        assertArrayEquals(new String[] {"s3://b/data", "**/*.parquet"},
                FileClientModule.splitGlob("s3://b/data/**/*.parquet"));
        assertArrayEquals(new String[] {"/mnt/files", "*.csv"},
                FileClientModule.splitGlob("/mnt/files/*.csv"));
        assertArrayEquals(new String[] {"s3://bucket", "*.parquet"},
                FileClientModule.splitGlob("s3://bucket/*.parquet"));
        // No metacharacters → plain directory, null pattern.
        String[] plain = FileClientModule.splitGlob("/mnt/files");
        assertEquals("/mnt/files", plain[0]);
        assertNull(plain[1]);
    }

    @Test
    void testRefreshIntervalFlowsIntoOperand()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("/mnt/data").setRefreshInterval("5 minutes");
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertEquals("5 minutes", operand.get("refreshInterval"));
    }

    @Test
    void testNoRefreshIntervalOmitsOperand()
            throws Exception
    {
        FileConfig config = new FileConfig().setGlob("/mnt/data");
        Map<String, Object> operand = firstSchemaOperand(FileClientModule.buildModelJson(config, NO_ENV));
        assertFalse(operand.containsKey("refreshInterval"));
    }

    @Test
    void testCatalogTtlDerivedFromRefreshInterval()
    {
        FileConfig config = new FileConfig().setGlob("/mnt/data").setRefreshInterval("PT5M");
        io.airlift.units.Duration ttl = FileClientModule.catalogCacheTtl(config);
        assertEquals(java.time.Duration.ofMinutes(5).toMillis(),
                (long) ttl.getValue(java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void testExplicitCatalogTtlBeatsRefreshInterval()
    {
        FileConfig config = new FileConfig()
                .setGlob("/mnt/data")
                .setRefreshInterval("PT5M")
                .setCatalogRefreshInterval(
                        new io.airlift.units.Duration(30, java.util.concurrent.TimeUnit.SECONDS));
        io.airlift.units.Duration ttl = FileClientModule.catalogCacheTtl(config);
        assertEquals(30_000L, (long) ttl.getValue(java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void testCatalogTtlNullWhenUnset()
    {
        assertNull(FileClientModule.catalogCacheTtl(new FileConfig().setGlob("/mnt/data")));
    }

    @Test
    void testConnectionUrlPrefix()
    {
        FileConfig config = new FileConfig().setGlob("/tmp/data");
        String url = FileClientModule.buildConnectionUrl(config, NO_ENV);
        assertTrue(url.startsWith("jdbc:calcite:model=inline:"),
                "unexpected url: " + url);
    }
}
