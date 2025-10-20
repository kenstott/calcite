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
package org.apache.calcite.mcp.metadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metadata parser for vector column comments.
 *
 * <p>Parses [VECTOR ...] metadata from JDBC column REMARKS to extract:
 * <ul>
 *   <li>dimension: Vector dimension (e.g., 384)</li>
 *   <li>provider: Provider type ("local", "openai", "cohere")</li>
 *   <li>model: Model name (e.g., "tf-idf", "text-embedding-ada-002")</li>
 *   <li>source_table: Logical FK target table (Pattern 2b)</li>
 *   <li>source_id_col: Logical FK column (Pattern 2b)</li>
 *   <li>source_table_col: Column containing source table name (Pattern 3)</li>
 *   <li>source_id_col: Column containing source ID (Pattern 3)</li>
 * </ul>
 *
 * <p>Example metadata formats:
 * <pre>
 * Pattern 1 (Co-located):
 *   [VECTOR dimension=384 provider=local model=tf-idf]
 *
 * Pattern 2b (Logical FK):
 *   [VECTOR dimension=384 provider=local model=tf-idf source_table=mda_sections source_id_col=section_id]
 *
 * Pattern 3 (Multi-source):
 *   [VECTOR dimension=384 provider=local model=tf-idf source_table_col=source_table source_id_col=source_id]
 * </pre>
 */
public class VectorMetadata {
  private final String columnName;
  private final int dimension;
  private final String provider;
  private final String model;
  private final String description;

  // Pattern 2b: Logical FK
  private String sourceTable;
  private String sourceIdColumn;

  // Pattern 3: Multi-source
  private String sourceTableColumn;
  private String sourceIdColumnName;

  // Pattern 2a: Discovered FK
  private VectorPattern pattern;
  private String discoveredFkColumn;
  private String discoveredTargetTable;
  private String discoveredTargetColumn;

  private VectorMetadata(String columnName, int dimension, String provider,
                         String model, String description) {
    this.columnName = columnName;
    this.dimension = dimension;
    this.provider = provider;
    this.model = model;
    this.description = description;
  }

  /**
   * Parse vector metadata from JDBC column comment.
   *
   * @param columnName Column name
   * @param comment JDBC column REMARKS containing [VECTOR ...] metadata
   * @return VectorMetadata instance or null if no valid metadata found
   */
  public static VectorMetadata parseFromComment(String columnName, String comment) {
    if (comment == null || !comment.contains("[VECTOR ")) {
      return null;
    }

    // Parse: [VECTOR dimension=384 provider=local model=tf-idf ...]
    // Try Pattern 3 first (source_table_col + source_id_col)
    Pattern pattern3Regex = Pattern.compile(
        "\\[VECTOR\\s+" +
        "dimension=(\\d+)\\s+" +
        "provider=(\\w+)\\s+" +
        "model=([\\w-]+)" +
        "\\s+source_table_col=(\\w+)" +
        "\\s+source_id_col=(\\w+)" +
        "\\]");

    Matcher matcher = pattern3Regex.matcher(comment);
    if (matcher.find()) {
      VectorMetadata vm = new VectorMetadata(
          columnName,
          Integer.parseInt(matcher.group(1)),
          matcher.group(2),
          matcher.group(3),
          comment.substring(0, matcher.start()).trim()
      );
      vm.sourceTableColumn = matcher.group(4);
      vm.sourceIdColumnName = matcher.group(5);
      return vm;
    }

    // Try Pattern 2b (source_table + source_id_col)
    Pattern pattern2bRegex = Pattern.compile(
        "\\[VECTOR\\s+" +
        "dimension=(\\d+)\\s+" +
        "provider=(\\w+)\\s+" +
        "model=([\\w-]+)" +
        "\\s+source_table=([\\w.]+)" +
        "\\s+source_id_col=(\\w+)" +
        "\\]");

    matcher = pattern2bRegex.matcher(comment);
    if (matcher.find()) {
      VectorMetadata vm = new VectorMetadata(
          columnName,
          Integer.parseInt(matcher.group(1)),
          matcher.group(2),
          matcher.group(3),
          comment.substring(0, matcher.start()).trim()
      );
      vm.sourceTable = matcher.group(4);
      vm.sourceIdColumn = matcher.group(5);
      return vm;
    }

    // Try Pattern 1 (no extra params)
    Pattern pattern1Regex = Pattern.compile(
        "\\[VECTOR\\s+" +
        "dimension=(\\d+)\\s+" +
        "provider=(\\w+)\\s+" +
        "model=([\\w-]+)" +
        "\\]");

    matcher = pattern1Regex.matcher(comment);
    if (matcher.find()) {
      VectorMetadata vm = new VectorMetadata(
          columnName,
          Integer.parseInt(matcher.group(1)),
          matcher.group(2),
          matcher.group(3),
          comment.substring(0, matcher.start()).trim()
      );
      return vm;
    }

    return null;
  }

  // Getters
  public String getColumnName() {
    return columnName;
  }

  public int getDimension() {
    return dimension;
  }

  public String getProvider() {
    return provider;
  }

  public String getModel() {
    return model;
  }

  public String getDescription() {
    return description;
  }

  public VectorPattern getPattern() {
    return pattern;
  }

  public void setPattern(VectorPattern pattern) {
    this.pattern = pattern;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getSourceIdColumn() {
    return sourceIdColumn;
  }

  public String getSourceTableColumn() {
    return sourceTableColumn;
  }

  public String getSourceIdColumnName() {
    return sourceIdColumnName;
  }

  public String getDiscoveredFkColumn() {
    return discoveredFkColumn;
  }

  public void setDiscoveredFkColumn(String col) {
    this.discoveredFkColumn = col;
  }

  public String getDiscoveredTargetTable() {
    return discoveredTargetTable;
  }

  public void setDiscoveredTargetTable(String table) {
    this.discoveredTargetTable = table;
  }

  public String getDiscoveredTargetColumn() {
    return discoveredTargetColumn;
  }

  public void setDiscoveredTargetColumn(String col) {
    this.discoveredTargetColumn = col;
  }
}
