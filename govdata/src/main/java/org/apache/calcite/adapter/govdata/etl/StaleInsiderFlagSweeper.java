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
package org.apache.calcite.adapter.govdata.etl;
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.partition.PipelineTrackerFactory;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.sec.EdgarFullIndexCache;
import org.apache.calcite.adapter.govdata.sec.SecFilingCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Lightweight one-shot sweeper that clears stale {@code _no_xbrl} tracker flags
 * for insider forms (3/4/5) without running the full ETL pipeline.
 *
 * <p>The bug: {@link org.apache.calcite.adapter.file.etl.DocumentETLProcessor} previously
 * set {@code _no_xbrl} on insider forms that don't use XBRL. This prevented the next ETL
 * run from reprocessing them.
 *
 * <p>This sweeper reads the EDGAR full-index cache (quarterly static files, no per-filing HTTP)
 * and calls {@link SecFilingCache#clearStaleInsiderNoXbrl} for each year. Completes in minutes
 * rather than the days required by the full ETL approach.
 *
 * <p>Usage: {@code java ... StaleInsiderFlagSweeper --model <path> [--start-year N] [--end-year N]}
 */
public class StaleInsiderFlagSweeper {
  private static final Logger LOGGER = LoggerFactory.getLogger(StaleInsiderFlagSweeper.class);

  private static final List<String> INSIDER_FILING_TYPES =
      Arrays.asList("3", "3/A", "4", "4/A", "5", "5/A");

  public static void main(String[] args) {
    int exitCode = 2;
    try {
      exitCode = new StaleInsiderFlagSweeper().run(args);
    } catch (Exception e) {
      LOGGER.error("Fatal error: {}", e.getMessage(), e);
    }
    System.exit(exitCode);
  }

  public int run(String[] args) throws IOException {
    String modelPath = null;
    int startYear = 2010;
    int endYear = 2019;

    for (int i = 0; i < args.length; i++) {
      if ("--model".equals(args[i]) && i + 1 < args.length) {
        modelPath = args[++i];
      } else if ("--start-year".equals(args[i]) && i + 1 < args.length) {
        startYear = Integer.parseInt(args[++i]);
      } else if ("--end-year".equals(args[i]) && i + 1 < args.length) {
        endYear = Integer.parseInt(args[++i]);
      }
    }

    if (modelPath == null) {
      LOGGER.error("Usage: StaleInsiderFlagSweeper --model <path> [--start-year N] [--end-year N]");
      return 2;
    }

    Map<String, Object> operand = loadOperand(modelPath);
    Map<String, Object> resolved = resolveEnvVars(operand);

    String directory = (String) resolved.get("directory");
    String cacheDirectory = (String) resolved.get("cacheDirectory");

    StorageProvider storageProvider = buildStorageProvider(resolved, directory);
    PipelineTracker tracker = PipelineTrackerFactory.createFromOperand(resolved, cacheDirectory);
    SecFilingCache filingCache = new SecFilingCache(tracker, storageProvider, directory);

    int totalCleared = 0;
    int totalCandidates = 0;

    try {
      for (int year = startYear; year <= endYear; year++) {
        LOGGER.info("Processing year {}", year);
        String secCacheDirectory = storageProvider.resolvePath(cacheDirectory, "sec");
        EdgarFullIndexCache indexCache =
            new EdgarFullIndexCache(storageProvider, secCacheDirectory, year, year);

        List<EdgarFullIndexCache.IndexEntry> candidates =
            indexCache.getActiveAccessions(year, INSIDER_FILING_TYPES, null);
        LOGGER.info("Year {}: {} insider candidates in index", year, candidates.size());
        totalCandidates += candidates.size();

        // Bulk-load tracker state into memory so isComplete calls are O(1) not per-S3-file
        List<String> accessions = new ArrayList<>(candidates.size());
        for (EdgarFullIndexCache.IndexEntry ie : candidates) {
          accessions.add(ie.accession);
        }
        filingCache.preload(accessions);

        int cleared = filingCache.clearStaleInsiderNoXbrl(candidates);
        LOGGER.info("Year {}: cleared {} stale no_xbrl flags", year, cleared);
        totalCleared += cleared;
      }
    } finally {
      try {
        filingCache.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing filing cache: {}", e.getMessage());
      }
      if (tracker instanceof AutoCloseable) {
        try {
          ((AutoCloseable) tracker).close();
        } catch (Exception e) {
          LOGGER.warn("Error closing tracker: {}", e.getMessage());
        }
      }
    }

    LOGGER.info("Sweep complete: {} candidates examined, {} stale flags cleared (years {}-{})",
        totalCandidates, totalCleared, startYear, endYear);
    return 0;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> loadOperand(String modelPath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(new File(modelPath));
    JsonNode schemas = root.get("schemas");
    if (schemas == null || !schemas.isArray() || schemas.size() == 0) {
      throw new IllegalArgumentException("Model must contain a 'schemas' array");
    }
    JsonNode operandNode = schemas.get(0).get("operand");
    if (operandNode == null) {
      throw new IllegalArgumentException("First schema must have an 'operand' object");
    }
    Map<String, Object> operand = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = operandNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      operand.put(field.getKey(), jsonNodeToObject(field.getValue()));
    }
    return operand;
  }

  private Object jsonNodeToObject(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isDouble()) {
      return node.doubleValue();
    } else if (node.isTextual()) {
      return node.textValue();
    } else if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      for (JsonNode element : node) {
        list.add(jsonNodeToObject(element));
      }
      return list;
    } else if (node.isObject()) {
      Map<String, Object> map = new HashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        map.put(field.getKey(), jsonNodeToObject(field.getValue()));
      }
      return map;
    }
    return node.asText();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> resolveEnvVars(Map<String, Object> operand) {
    Map<String, Object> resolved = new HashMap<>(operand);
    for (String configKey : new String[]{"s3Config", "trackerConfig"}) {
      Object configObj = resolved.get(configKey);
      if (configObj instanceof Map) {
        Map<String, Object> config = new HashMap<>((Map<String, Object>) configObj);
        for (Map.Entry<String, Object> entry : config.entrySet()) {
          Object val = entry.getValue();
          if (val instanceof String) {
            String str = (String) val;
            if (str.contains("${")) {
              entry.setValue(VariableResolver.resolveEnvVars(str));
            }
          }
        }
        resolved.put(configKey, config);
      }
    }
    // Resolve top-level string values too (directory, cacheDirectory)
    for (Map.Entry<String, Object> entry : new HashMap<>(resolved).entrySet()) {
      Object val = entry.getValue();
      if (val instanceof String) {
        String str = (String) val;
        if (str.contains("${")) {
          resolved.put(entry.getKey(), VariableResolver.resolveEnvVars(str));
        }
      }
    }
    return resolved;
  }

  @SuppressWarnings("unchecked")
  private StorageProvider buildStorageProvider(Map<String, Object> operand, String directory) {
    Map<String, Object> s3Config = (Map<String, Object>) operand.get("s3Config");
    if (directory != null && directory.startsWith("s3://")) {
      Map<String, Object> storageConfig = new HashMap<>(s3Config);
      storageConfig.put("directory", directory);
      return StorageProviderFactory.createFromType("s3", storageConfig);
    }
    return StorageProviderFactory.createFromUrl(directory);
  }
}
