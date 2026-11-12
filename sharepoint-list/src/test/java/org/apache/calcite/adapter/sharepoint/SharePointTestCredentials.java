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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;
import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The credentials of the SharePoint site the live tests run against: an app registration
 * that authenticates with a certificate.
 *
 * <p>They are read from {@code file/local-test.properties}, or from
 * {@code sharepoint-list/local-test.properties}; an environment variable of the same name
 * takes the place of a key the file lacks. A key that is in neither is an error naming it.
 */
final class SharePointTestCredentials {
  static final String TENANT_ID = "SHAREPOINT_TENANT_ID";
  static final String CLIENT_ID = "SHAREPOINT_CLIENT_ID";
  static final String SITE_URL = "SHAREPOINT_SITE_URL";
  static final String CERT_PATH = "SHAREPOINT_CERT_PATH";
  static final String CERT_PASSWORD = "SHAREPOINT_CERT_PASSWORD";

  private static final String[] KEYS = {TENANT_ID, CLIENT_ID, SITE_URL, CERT_PATH, CERT_PASSWORD};

  /** Where the file may be, relative to the directory Gradle or an IDE runs a test in. */
  private static final String[] LOCATIONS = {
      "../file/local-test.properties", "file/local-test.properties",
      "local-test.properties", "sharepoint-list/local-test.properties"};

  private final Properties properties;

  private SharePointTestCredentials(Properties properties) {
    this.properties = properties;
  }

  /** Reads the credentials, or throws naming the keys that are missing. */
  static SharePointTestCredentials load() {
    Properties file = new Properties();
    Path found = null;
    for (String location : LOCATIONS) {
      Path path = Paths.get(location);
      if (Files.isRegularFile(path)) {
        found = path;
        try (InputStream in = Files.newInputStream(path)) {
          file.load(in);
        } catch (IOException e) {
          throw new IllegalStateException("Reading " + path.toAbsolutePath() + " failed", e);
        }
        break;
      }
    }
    Properties properties = new Properties();
    List<String> missing = new ArrayList<>();
    for (String key : KEYS) {
      String value = file.getProperty(key);
      if (value == null || value.isEmpty()) {
        value = System.getenv(key);
      }
      if (value == null || value.isEmpty()) {
        missing.add(key);
      } else {
        properties.setProperty(key, value);
      }
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException("The SharePoint live tests authenticate with a certificate"
          + " and need " + missing + ", from "
          + (found == null ? "file/local-test.properties (not found)" : found.toAbsolutePath())
          + " or the environment. See file/local-test.properties.sample.");
    }
    if (!Files.isRegularFile(Paths.get(properties.getProperty(CERT_PATH)))) {
      throw new IllegalStateException(CERT_PATH + " names a file that does not exist: "
          + properties.getProperty(CERT_PATH));
    }
    return new SharePointTestCredentials(properties);
  }

  /** The five keys, for tests that read them by name. */
  Properties properties() {
    Properties copy = new Properties();
    copy.putAll(properties);
    return copy;
  }

  String tenantId() {
    return properties.getProperty(TENANT_ID);
  }

  String clientId() {
    return properties.getProperty(CLIENT_ID);
  }

  String siteUrl() {
    return properties.getProperty(SITE_URL);
  }

  String certificatePath() {
    return properties.getProperty(CERT_PATH);
  }

  String certificatePassword() {
    return properties.getProperty(CERT_PASSWORD);
  }

  /** The adapter's authentication settings: {@code authType} and what it needs. */
  Map<String, Object> authConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("authType", "CERTIFICATE");
    config.put("tenantId", tenantId());
    config.put("clientId", clientId());
    config.put("certificatePath", certificatePath());
    config.put("certificatePassword", certificatePassword());
    return config;
  }

  SharePointAuth auth() {
    return SharePointAuthFactory.createAuth(authConfig());
  }
}
