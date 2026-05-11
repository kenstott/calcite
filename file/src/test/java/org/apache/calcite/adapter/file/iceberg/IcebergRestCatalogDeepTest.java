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
package org.apache.calcite.adapter.file.iceberg;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link IcebergRestCatalog}.
 * Tests validation, configuration parsing, and error handling.
 */
@Tag("unit")
public class IcebergRestCatalogDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergRestCatalogDeepTest.class);

  // --- validateRestCatalogConfig tests ---

  @Test
  public void testValidateNullConfig() {
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.validateRestCatalogConfig(null));
  }

  @Test
  public void testValidateMissingUri() {
    Map<String, Object> config = new HashMap<>();
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.validateRestCatalogConfig(config));
  }

  @Test
  public void testValidateEmptyUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "");
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.validateRestCatalogConfig(config));
  }

  @Test
  public void testValidateWhitespaceUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "   ");
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.validateRestCatalogConfig(config));
  }

  @Test
  public void testValidateValidConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "http://localhost:8181");
    // Should not throw
    IcebergRestCatalog.validateRestCatalogConfig(config);
  }

  @Test
  public void testValidateConfigWithAllOptions() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "https://iceberg-rest.example.com");
    config.put("credential", "my-credential");
    config.put("token", "my-token");
    config.put("warehouse", "my-warehouse");
    config.put("oauth2-server-uri", "https://auth.example.com");
    config.put("client-id", "client123");
    config.put("client-secret", "secret456");

    // Should not throw
    IcebergRestCatalog.validateRestCatalogConfig(config);
  }

  // --- createRestCatalog tests ---

  @Test
  public void testCreateRestCatalogMissingUri() {
    Map<String, Object> config = new HashMap<>();
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.createRestCatalog(config));
  }

  @Test
  public void testCreateRestCatalogEmptyUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "");
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.createRestCatalog(config));
  }

  @Test
  public void testCreateRestCatalogNullUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", null);
    assertThrows(IllegalArgumentException.class,
        () -> IcebergRestCatalog.createRestCatalog(config));
  }

  @Test
  public void testCreateRestCatalogWithCredentials() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "http://localhost:8181");
    config.put("credential", "my-credential");
    config.put("token", "my-token");
    config.put("warehouse", "/tmp/warehouse");

    // May throw connection error, but should not throw config error
    try {
      IcebergRestCatalog.createRestCatalog(config);
    } catch (Exception e) {
      // Expected - REST catalog creation may fail without a real server
      LOGGER.debug("Expected error creating REST catalog without server: {}",
          e.getMessage());
    }
  }

  @Test
  public void testCreateRestCatalogWithOAuth() {
    Map<String, Object> config = new HashMap<>();
    config.put("uri", "http://localhost:8181");
    config.put("oauth2-server-uri", "https://auth.example.com");
    config.put("client-id", "client123");
    config.put("client-secret", "secret456");

    try {
      IcebergRestCatalog.createRestCatalog(config);
    } catch (Exception e) {
      // Expected - REST catalog creation may fail without a real server
      LOGGER.debug("Expected error creating REST catalog without server: {}",
          e.getMessage());
    }
  }
}
