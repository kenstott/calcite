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
package org.apache.calcite.test;

import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Base class for Splunk adapter tests that require connection to a Splunk instance.
 * The connection comes from {@link SplunkTestSettings}; without one the tests fail.
 */
public abstract class SplunkTestBase {
  protected static String SPLUNK_URL = null;
  protected static String SPLUNK_USER = null;
  protected static String SPLUNK_PASSWORD = null;
  protected static boolean DISABLE_SSL_VALIDATION = false;
  protected static boolean splunkAvailable = false;

  static {
    // Register the Splunk driver
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Splunk driver", e);
    }
  }

  @BeforeAll
  public static void loadConnectionProperties() {
    SPLUNK_URL = SplunkTestSettings.url();
    SPLUNK_USER = SplunkTestSettings.user();
    SPLUNK_PASSWORD = SplunkTestSettings.password();
    DISABLE_SSL_VALIDATION = SplunkTestSettings.sslInsecure();
    splunkAvailable = true;
  }

  protected Connection getConnection() throws SQLException {
    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured");
    }

    Properties props = new Properties();
    props.setProperty("url", SPLUNK_URL);
    props.setProperty("user", SPLUNK_USER);
    props.setProperty("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      props.setProperty("disableSslValidation", "true");
    }
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    return DriverManager.getConnection("jdbc:splunk:", props);
  }
}
