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
package org.apache.calcite.adapter.file.etl.cache;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory index of raw cache bundle entries.
 *
 * <p>Maps source keys to {@link BundleEntry} values. Supports merging
 * multiple index files from successive ETL runs — later entries override
 * earlier ones for the same source key.
 */
public class BundleIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(BundleIndex.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, BundleEntry> entries;

  public BundleIndex() {
    this.entries = new LinkedHashMap<String, BundleEntry>();
  }

  /** Returns the entry for the given source key, or null. */
  public BundleEntry get(String sourceKey) {
    return entries.get(sourceKey);
  }

  /** Returns true if the index contains the given source key. */
  public boolean contains(String sourceKey) {
    return entries.containsKey(sourceKey);
  }

  /** Returns the number of entries. */
  public int size() {
    return entries.size();
  }

  /** Returns an unmodifiable view of all entries. */
  public Map<String, BundleEntry> getEntries() {
    return Collections.unmodifiableMap(entries);
  }

  /**
   * Merges entries from a JSONL index file into this index.
   * Later entries override earlier ones for the same key.
   *
   * @param jsonlContent Contents of an .idx.jsonl file
   * @param defaultBundleFile Bundle filename for entries without explicit bundleFile
   */
  public void mergeFromJsonl(String jsonlContent, String defaultBundleFile) {
    try (BufferedReader reader = new BufferedReader(new StringReader(jsonlContent))) {
      String line;
      int lineNum = 0;
      while ((line = reader.readLine()) != null) {
        lineNum++;
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        try {
          @SuppressWarnings("unchecked")
          Map<String, Object> json = MAPPER.readValue(line, Map.class);
          String key = (String) json.get("key");
          if (key == null) {
            LOGGER.warn("Index line {} missing 'key', skipping", lineNum);
            continue;
          }
          long length = ((Number) json.get("length")).longValue();
          long ts = json.containsKey("ts") ? ((Number) json.get("ts")).longValue() : 0;
          String storage = (String) json.get("storage");

          BundleEntry entry;
          if ("object".equals(storage)) {
            entry = BundleEntry.individual(length, ts);
          } else {
            long offset = ((Number) json.get("offset")).longValue();
            String bundleFile = json.containsKey("bundleFile")
                ? (String) json.get("bundleFile") : defaultBundleFile;
            entry = BundleEntry.bundled(bundleFile, offset, length, ts);
          }
          entries.put(key, entry);
        } catch (Exception e) {
          LOGGER.warn("Failed to parse index line {}: {}", lineNum, e.getMessage());
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to read index content", e);
    }
  }

  /**
   * Serializes all entries to JSONL format.
   *
   * @return JSONL string suitable for writing to an .idx.jsonl file
   */
  public String toJsonl() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, BundleEntry> entry : entries.entrySet()) {
      String key = entry.getKey();
      BundleEntry be = entry.getValue();
      sb.append("{\"key\":\"").append(escapeJson(key)).append("\"");
      if (be.isBundled()) {
        sb.append(",\"offset\":").append(be.getOffset());
      } else {
        sb.append(",\"storage\":\"object\"");
      }
      sb.append(",\"length\":").append(be.getLength());
      sb.append(",\"ts\":").append(be.getTimestamp());
      sb.append("}\n");
    }
    return sb.toString();
  }

  /** Puts a single entry into the index. */
  public void put(String sourceKey, BundleEntry entry) {
    entries.put(sourceKey, entry);
  }

  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
