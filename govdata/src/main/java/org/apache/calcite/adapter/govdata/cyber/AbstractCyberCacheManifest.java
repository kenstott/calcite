/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.cyber;

import org.apache.calcite.adapter.govdata.YamlUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;


/**
 * Base class for cyber schema cache manifests.
 *
 * <p>Reads {@code ttl_days} from per-table YAML config and exposes it for
 * use by downloaders and TTL-based partition eviction. This completes the
 * wiring that {@code econ-schema.yaml}'s {@code current_year_ttl_days} field
 * intended but never implemented.
 *
 * <p>Subclasses specify the schema YAML resource via {@link #getSchemaResourceName()}.
 */
public abstract class AbstractCyberCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCyberCacheManifest.class);

  private final Map<String, Integer> tableTtlDays = new HashMap<>();
  private boolean loaded = false;

  /**
   * Returns the schema YAML resource path (e.g., "/cyber/cyber-vuln-schema.yaml").
   */
  protected abstract String getSchemaResourceName();

  /**
   * Returns the TTL in days for a given table, or the default if not configured.
   *
   * @param tableName table name as defined in the YAML schema
   * @param defaultTtlDays fallback value if the table has no ttl_days config
   * @return TTL in days
   */
  public int getTtlDays(String tableName, int defaultTtlDays) {
    ensureLoaded();
    Integer ttl = tableTtlDays.get(tableName);
    return ttl != null ? ttl : defaultTtlDays;
  }

  /**
   * Returns the TTL in days for a given table, or -1 if not configured (no eviction).
   */
  public int getTtlDays(String tableName) {
    return getTtlDays(tableName, -1);
  }

  /**
   * Returns true if the table has an explicit TTL configured.
   */
  public boolean hasTtl(String tableName) {
    ensureLoaded();
    return tableTtlDays.containsKey(tableName);
  }

  private void ensureLoaded() {
    if (!loaded) {
      loadTtlConfig();
      loaded = true;
    }
  }

  private void loadTtlConfig() {
    String resource = getSchemaResourceName();
    try (InputStream stream = AbstractCyberCacheManifest.class.getResourceAsStream(resource)) {
      if (stream == null) {
        LOGGER.debug("Schema resource {} not found, no TTL config loaded", resource);
        return;
      }

      JsonNode root = YamlUtils.parseYamlOrJson(stream, resource);
      JsonNode tables = root.path("partitionedTables");

      if (tables.isArray()) {
        for (JsonNode tableNode : tables) {
          String tableName = tableNode.path("name").asText(null);
          if (tableName == null) {
            continue;
          }
          JsonNode ttlNode = tableNode.path("ttl_days");
          if (!ttlNode.isMissingNode() && ttlNode.isInt()) {
            tableTtlDays.put(tableName, ttlNode.asInt());
            LOGGER.debug("Loaded ttl_days={} for table {}", ttlNode.asInt(), tableName);
          }
        }
      }

      LOGGER.info("Loaded TTL config for {} tables from {}", tableTtlDays.size(), resource);

    } catch (IOException e) {
      LOGGER.error("Error loading TTL config from {}: {}", resource, e.getMessage());
    }
  }
}
