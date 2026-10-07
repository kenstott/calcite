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

import com.google.common.collect.ImmutableMap;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the connector in an in-process Trino server against a live Salesforce org. Credentials come
 * from {@code govdata/.env.prod}: SF_LOGIN_URL, SF_CONSUMER_KEY and SF_CONSUMER_SECRET (OAuth client
 * credentials flow).
 */
@Tag("integration")
class TestSalesforceConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> env = loadEnv();
        String host = env.get("SF_LOGIN_URL");
        String loginUrl = host.startsWith("https://") ? host : "https://" + host;

        Session session = testSessionBuilder()
                .setCatalog("salesforce")
                .setSchema("salesforce")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new SalesforcePlugin());
        queryRunner.createCatalog("salesforce", "salesforce", ImmutableMap.of(
                "login-url", loginUrl,
                "client-id", env.get("SF_CONSUMER_KEY"),
                "client-secret", env.get("SF_CONSUMER_SECRET"),
                "api-version", "v61.0",
                "case-insensitive-name-matching", "true"));
        return queryRunner;
    }

    private static Map<String, String> loadEnv()
            throws IOException
    {
        String rootDir = System.getProperty("gradle.rootDir");
        if (rootDir == null) {
            throw new IllegalStateException("gradle.rootDir system property is not set");
        }
        Path envFile = Paths.get(rootDir, "govdata", ".env.prod");
        if (!Files.exists(envFile)) {
            throw new IllegalStateException("Salesforce credentials file not found: " + envFile);
        }
        Map<String, String> env = new HashMap<>();
        for (String raw : Files.readAllLines(envFile, StandardCharsets.UTF_8)) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("export ")) {
                line = line.substring("export ".length()).trim();
            }
            int eq = line.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String value = line.substring(eq + 1).trim();
            if (value.length() >= 2
                    && ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'")))) {
                value = value.substring(1, value.length() - 1);
            }
            env.put(line.substring(0, eq).trim(), value);
        }
        for (String key : new String[] {"SF_LOGIN_URL", "SF_CONSUMER_KEY", "SF_CONSUMER_SECRET"}) {
            if (!env.containsKey(key)) {
                throw new IllegalStateException(key + " is missing from " + envFile);
            }
        }
        return env;
    }

    @Test
    void testShowTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES FROM salesforce.salesforce");
        assertTrue(tables.getOnlyColumnAsSet().contains("account"),
                "expected 'account' table, got: " + tables.getOnlyColumnAsSet());
    }

    @Test
    void testDescribe()
    {
        MaterializedResult columns = computeActual("DESCRIBE account");
        Map<Object, Object> types = new HashMap<>();
        for (var row : columns.getMaterializedRows()) {
            types.put(row.getField(0), row.getField(1));
        }
        assertEquals("varchar(18)", types.get("id"));
        assertEquals("boolean", types.get("isdeleted"));
        assertEquals("integer", types.get("numberofemployees"));
    }

    @Test
    void testFilterAndAggregate()
    {
        long total = (Long) computeActual("SELECT count(*) FROM account").getOnlyValue();
        assertTrue(total > 0, "expected accounts in the org");

        MaterializedResult named = computeActual(
                "SELECT name FROM account WHERE name LIKE '%a%' ORDER BY name LIMIT 3");
        assertTrue(named.getRowCount() > 0 && named.getRowCount() <= 3);
    }

    @Test
    void testJoin()
    {
        long rows = (Long) computeActual(
                "SELECT count(*) FROM contact c JOIN account a ON c.accountid = a.id").getOnlyValue();
        assertTrue(rows > 0, "expected contacts joined to accounts");
    }
}
