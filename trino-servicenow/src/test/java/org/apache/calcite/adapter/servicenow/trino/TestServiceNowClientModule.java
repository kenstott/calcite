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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestServiceNowClientModule
{
    @Test
    void basicAuthUrl()
    {
        ServiceNowConfig config = new ServiceNowConfig()
                .setInstanceUrl("https://dev12345.service-now.com")
                .setUsername("svc")
                .setPassword("p;w=d")
                .setPageSize(500);
        ServiceNowClientModule.validate(config);
        assertEquals(
                "jdbc:servicenow:instanceUrl=https%3A%2F%2Fdev12345.service-now.com;authType=basic;"
                        + "username=svc;password=p%3Bw%3Dd;pageSize=500",
                ServiceNowClientModule.buildConnectionUrl(config));
    }

    @Test
    void tablesAndCacheSettingsReachTheDriver()
    {
        ServiceNowConfig config = new ServiceNowConfig()
                .setInstanceUrl("https://dev12345.service-now.com")
                .setUsername("svc")
                .setPassword("pw")
                .setTables("incident,sys_user")
                .setCatalogCacheDirectory("/var/cache/sn catalog")
                .setCatalogCacheTtlMinutes(0);
        assertEquals(
                "jdbc:servicenow:instanceUrl=https%3A%2F%2Fdev12345.service-now.com;authType=basic;"
                        + "username=svc;password=pw;tables=incident%2Csys_user;"
                        + "catalogCacheDirectory=%2Fvar%2Fcache%2Fsn+catalog;"
                        + "catalogCacheTtlMinutes=0",
                ServiceNowClientModule.buildConnectionUrl(config));
    }

    @Test
    void pushdownSettingsReachTheDriver()
    {
        ServiceNowConfig config = new ServiceNowConfig()
                .setInstanceUrl("https://dev12345.service-now.com")
                .setUsername("svc")
                .setPassword("pw")
                .setPushdownVerification("/etc/trino/pushdown.json")
                .setTrustPushdown("SHAPE:AND,EQ:TEXT:AND");
        assertEquals(
                "jdbc:servicenow:instanceUrl=https%3A%2F%2Fdev12345.service-now.com;authType=basic;"
                        + "username=svc;password=pw;pushdownVerification=%2Fetc%2Ftrino%2Fpushdown.json;"
                        + "trustPushdown=SHAPE%3AAND%2CEQ%3ATEXT%3AAND",
                ServiceNowClientModule.buildConnectionUrl(config));
    }

    @Test
    void missingCredentialsAreNamed()
    {
        ServiceNowConfig config = new ServiceNowConfig()
                .setInstanceUrl("https://dev12345.service-now.com");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ServiceNowClientModule.validate(config));
        assertTrue(e.getMessage().contains("username"), e.getMessage());
        assertTrue(e.getMessage().contains("password"), e.getMessage());
    }

    @Test
    void missingInstanceUrlIsNamed()
    {
        ServiceNowConfig config = new ServiceNowConfig().setUsername("u").setPassword("p");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ServiceNowClientModule.validate(config));
        assertTrue(e.getMessage().contains("instance-url"), e.getMessage());
    }

    @Test
    void oauthIsNotImplemented()
    {
        ServiceNowConfig config = new ServiceNowConfig()
                .setInstanceUrl("https://dev12345.service-now.com")
                .setAuthType("oauth_client_credentials")
                .setUsername("u")
                .setPassword("p");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ServiceNowClientModule.validate(config));
        assertTrue(e.getMessage().contains("only 'basic'"), e.getMessage());
    }
}
