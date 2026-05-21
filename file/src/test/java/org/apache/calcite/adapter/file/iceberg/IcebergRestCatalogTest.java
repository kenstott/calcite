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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IcebergRestCatalog}.
 */
@Tag("unit")
public class IcebergRestCatalogTest {

  @Test public void testValidateRestCatalogConfigNullConfig() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.validateRestCatalogConfig(null);
      }
    });
  }

  @Test public void testValidateRestCatalogConfigMissingUri() {
    Map<String, Object> config = new HashMap<String, Object>();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            IcebergRestCatalog.validateRestCatalogConfig(config);
          }
        });
    assertTrue(ex.getMessage().contains("uri"));
  }

  @Test public void testValidateRestCatalogConfigEmptyUri() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "  ");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            IcebergRestCatalog.validateRestCatalogConfig(config);
          }
        });
    assertTrue(ex.getMessage().contains("uri"));
  }

  @Test public void testValidateRestCatalogConfigNullUri() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", null);
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.validateRestCatalogConfig(config);
      }
    });
  }

  @Test public void testValidateRestCatalogConfigValidUri() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "http://localhost:8181");
    // Should not throw
    IcebergRestCatalog.validateRestCatalogConfig(config);
  }

  @Test public void testValidateRestCatalogConfigWithWarehouse() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "http://localhost:8181");
    config.put("warehouse", "s3://my-bucket/warehouse");
    // Should not throw
    IcebergRestCatalog.validateRestCatalogConfig(config);
  }

  @Test public void testCreateRestCatalogMissingUri() {
    final Map<String, Object> config = new HashMap<String, Object>();
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.createRestCatalog(config);
      }
    });
  }

  @Test public void testCreateRestCatalogEmptyUri() {
    final Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "");
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.createRestCatalog(config);
      }
    });
  }

  @Test public void testCreateRestCatalogNullUri() {
    final Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", null);
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.createRestCatalog(config);
      }
    });
  }

  @Test public void testValidateRestCatalogConfigInvalidUri() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "://invalid-uri");
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        IcebergRestCatalog.validateRestCatalogConfig(config);
      }
    });
  }

  @Test public void testValidateRestCatalogConfigWithAllOptions() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("uri", "http://localhost:8181");
    config.put("credential", "client-id:client-secret");
    config.put("token", "bearer-token");
    config.put("warehouse", "s3://bucket/warehouse");
    config.put("oauth2-server-uri", "http://auth.example.com/token");
    config.put("client-id", "my-client");
    config.put("client-secret", "my-secret");
    // Should not throw
    IcebergRestCatalog.validateRestCatalogConfig(config);
  }
}
