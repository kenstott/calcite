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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the connector in an in-process Trino server against a live ServiceNow instance. NEVER RUN so
 * far: no instance was available when it was written. Credentials come from
 * {@code govdata/.env.prod}: SN_INSTANCE_URL, SN_USERNAME and SN_PASSWORD (HTTP Basic). The class
 * skips itself when they are not configured.
 */
@Tag("integration")
class TestServiceNowConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> env = loadEnv();
        assumeTrue(env != null,
                "no ServiceNow instance configured: SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD");
        String host = env.get("SN_INSTANCE_URL");
        String instanceUrl = host.startsWith("https://") ? host : "https://" + host;

        Session session = testSessionBuilder()
                .setCatalog("servicenow")
                .setSchema("servicenow")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new ServiceNowPlugin());
        queryRunner.createCatalog("servicenow", "servicenow", ImmutableMap.of(
                "instance-url", instanceUrl,
                "username", env.get("SN_USERNAME"),
                "password", env.get("SN_PASSWORD"),
                "tables", "incident,task,sys_user"));
        return queryRunner;
    }

    /** Returns the credentials, or null if none are configured. */
    private static Map<String, String> loadEnv()
            throws IOException
    {
        String rootDir = System.getProperty("gradle.rootDir");
        if (rootDir == null) {
            throw new IllegalStateException("gradle.rootDir system property is not set");
        }
        Path envFile = Paths.get(rootDir, "govdata", ".env.prod");
        if (!Files.exists(envFile)) {
            return null;
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
        boolean any = false;
        for (String key : new String[] {"SN_INSTANCE_URL", "SN_USERNAME", "SN_PASSWORD"}) {
            any |= env.containsKey(key);
        }
        if (!any) {
            return null;
        }
        for (String key : new String[] {"SN_INSTANCE_URL", "SN_USERNAME", "SN_PASSWORD"}) {
            if (!env.containsKey(key)) {
                throw new IllegalStateException(key + " is missing from " + envFile);
            }
        }
        return env;
    }

    @Test
    void testShowTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES FROM servicenow.servicenow");
        assertTrue(tables.getOnlyColumnAsSet().contains("incident"),
                "expected 'incident' table, got: " + tables.getOnlyColumnAsSet());
    }

    @Test
    void testDescribe()
    {
        MaterializedResult columns = computeActual("DESCRIBE incident");
        Map<Object, Object> types = new HashMap<>();
        for (var row : columns.getMaterializedRows()) {
            types.put(row.getField(0), row.getField(1));
        }
        assertEquals("varchar(32)", types.get("sys_id"));
        assertEquals("varchar(32)", types.get("caller_id"));
        assertTrue(types.containsKey("caller_id__display"));
    }

    @Test
    void testCountAndFilter()
    {
        long total = (Long) computeActual("SELECT count(*) FROM incident").getOnlyValue();
        assertTrue(total > 0, "expected incidents in the instance");

        MaterializedResult numbered = computeActual(
                "SELECT number FROM incident WHERE number LIKE 'INC%' ORDER BY number LIMIT 3");
        assertTrue(numbered.getRowCount() > 0 && numbered.getRowCount() <= 3);
    }
}
