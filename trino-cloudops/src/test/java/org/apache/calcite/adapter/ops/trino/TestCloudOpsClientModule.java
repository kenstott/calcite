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

import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@code jdbc:cloudops:} URL building and fail-fast validation in
 * {@link CloudOpsClientModule}. Deterministic and dependency-free: no Trino server, no cloud calls.
 */
class TestCloudOpsClientModule
{
    /**
     * Parses a {@code jdbc:cloudops:k=v;...} URL the same way {@code CloudOpsDriver} does: split the
     * remainder on {@code ;}, split each pair on the first {@code =}, then URL-decode the value.
     */
    private static Map<String, String> parse(String url)
    {
        assertTrue(url.startsWith("jdbc:cloudops:"), "wrong prefix: " + url);
        Map<String, String> out = new HashMap<>();
        String remainder = url.substring("jdbc:cloudops:".length());
        if (remainder.isEmpty()) {
            return out;
        }
        for (String pair : remainder.split(";")) {
            int idx = pair.indexOf('=');
            assertTrue(idx > 0, "malformed pair: " + pair);
            String key = pair.substring(0, idx);
            String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
            out.put(key, value);
        }
        return out;
    }

    private static CloudOpsConfig awsConfig()
    {
        return new CloudOpsConfig()
                .setAwsAccessKeyId("AKIA123")
                .setAwsSecretAccessKey("secret/with+special=chars")
                .setAwsAccountIds("111111111111,222222222222")
                .setAwsRegion("us-west-2");
    }

    @Test
    void testAwsUrlRoundTrips()
    {
        Map<String, String> params = parse(CloudOpsClientModule.buildConnectionUrl(awsConfig()));
        assertEquals("AKIA123", params.get("aws.accessKeyId"));
        // Secret with =, + and / survives encode/decode unchanged.
        assertEquals("secret/with+special=chars", params.get("aws.secretAccessKey"));
        // Comma-separated list survives so the adapter's parseList sees both account IDs.
        assertEquals("111111111111,222222222222", params.get("aws.accountIds"));
        assertEquals("us-west-2", params.get("aws.region"));
        // No azure/gcp keys for an AWS-only config.
        assertFalse(params.containsKey("azure.tenantId"));
        assertFalse(params.containsKey("gcp.credentialsPath"));
    }

    @Test
    void testAzureSubscriptionListAndCacheParams()
    {
        CloudOpsConfig config = new CloudOpsConfig()
                .setAzureTenantId("tenant")
                .setAzureClientId("client")
                .setAzureClientSecret("shh")
                .setAzureSubscriptionIds("subA,subB,subC")
                .setProviders("azure")
                .setCacheEnabled(Boolean.FALSE)
                .setCacheTtlMinutes(15)
                .setCacheDebugMode(Boolean.TRUE);
        Map<String, String> params = parse(CloudOpsClientModule.buildConnectionUrl(config));
        assertEquals("subA,subB,subC", params.get("azure.subscriptionIds"));
        assertEquals("azure", params.get("providers"));
        assertEquals("false", params.get("cache.enabled"));
        assertEquals("15", params.get("cache.ttlMinutes"));
        assertEquals("true", params.get("cache.debugMode"));
    }

    @Test
    void testUnsetCacheParamsOmitted()
    {
        // No cache.* properties set -> not emitted, so the adapter applies its own defaults.
        Map<String, String> params = parse(CloudOpsClientModule.buildConnectionUrl(awsConfig()));
        assertFalse(params.containsKey("cache.enabled"));
        assertFalse(params.containsKey("cache.ttlMinutes"));
        assertFalse(params.containsKey("cache.debugMode"));
    }

    @Test
    void testValidateFailsWithNoProvider()
    {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CloudOpsClientModule.validate(new CloudOpsConfig()));
        assertTrue(e.getMessage().contains("at least one cloud provider"), e.getMessage());
    }

    @Test
    void testValidateFailsOnPartialAzure()
    {
        // Azure enabled (tenant-id present) but credential set incomplete -> precise startup error,
        // instead of the adapter silently dropping the provider.
        CloudOpsConfig config = new CloudOpsConfig().setAzureTenantId("tenant");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CloudOpsClientModule.validate(config));
        assertTrue(e.getMessage().contains("azure.client-id"), e.getMessage());
        assertTrue(e.getMessage().contains("azure.client-secret"), e.getMessage());
        assertTrue(e.getMessage().contains("azure.subscription-ids"), e.getMessage());
    }

    @Test
    void testValidateFailsOnPartialAws()
    {
        CloudOpsConfig config = new CloudOpsConfig().setAwsAccessKeyId("AKIA123");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CloudOpsClientModule.validate(config));
        assertTrue(e.getMessage().contains("aws.account-ids"), e.getMessage());
        assertTrue(e.getMessage().contains("aws.region"), e.getMessage());
        assertTrue(e.getMessage().contains("aws.secret-access-key"), e.getMessage());
    }

    @Test
    void testValidatePassesForCompleteAws()
    {
        // Should not throw.
        CloudOpsClientModule.validate(awsConfig());
    }
}
