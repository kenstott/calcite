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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSalesforceClientModule
{
    @Test
    void clientCredentialsUrl()
    {
        SalesforceConfig config = new SalesforceConfig()
                .setLoginUrl("https://acme.my.salesforce.com")
                .setClientId("key")
                .setClientSecret("s;e=c")
                .setCacheMaxSize(50);
        SalesforceClientModule.validate(config);
        assertEquals(
                "jdbc:salesforce:loginUrl=https%3A%2F%2Facme.my.salesforce.com;clientId=key;"
                        + "clientSecret=s%3Be%3Dc;cacheMaxSize=50;lowercaseAliases=false",
                SalesforceClientModule.buildConnectionUrl(config));
    }

    @Test
    void describeCacheSettingsReachTheDriver()
    {
        SalesforceConfig config = new SalesforceConfig()
                .setLoginUrl("https://acme.my.salesforce.com")
                .setClientId("key")
                .setClientSecret("secret")
                .setDescribeCacheDirectory("/var/cache/sf describe")
                .setDescribeCacheTtlMinutes(0);
        assertEquals(
                "jdbc:salesforce:loginUrl=https%3A%2F%2Facme.my.salesforce.com;clientId=key;"
                        + "clientSecret=secret;describeCacheDirectory=%2Fvar%2Fcache%2Fsf+describe;"
                        + "describeCacheTtlMinutes=0;lowercaseAliases=false",
                SalesforceClientModule.buildConnectionUrl(config));
    }

    @Test
    void usernamePasswordNeedsConnectedApp()
    {
        SalesforceConfig config = new SalesforceConfig()
                .setLoginUrl("https://login.salesforce.com")
                .setUsername("user@example.com")
                .setPassword("pw");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SalesforceClientModule.validate(config));
        assertTrue(e.getMessage().contains("client-id"), e.getMessage());
        assertTrue(e.getMessage().contains("client-secret"), e.getMessage());
    }

    @Test
    void accessTokenNeedsInstanceUrl()
    {
        SalesforceConfig config = new SalesforceConfig()
                .setLoginUrl("https://login.salesforce.com")
                .setAccessToken("token");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SalesforceClientModule.validate(config));
        assertTrue(e.getMessage().contains("instance-url"), e.getMessage());
    }

    @Test
    void noCredentials()
    {
        SalesforceConfig config = new SalesforceConfig().setLoginUrl("https://login.salesforce.com");
        assertThrows(IllegalArgumentException.class, () -> SalesforceClientModule.validate(config));
    }
}
