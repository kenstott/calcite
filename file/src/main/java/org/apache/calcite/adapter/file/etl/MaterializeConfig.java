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
package org.apache.calcite.adapter.file.etl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for data materialization to hive-partitioned Parquet files.
 *
 * <p>This is the main configuration class for the {@link HiveParquetWriter}.
 * It combines output, partition, column, and option settings.
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>
 * materialize:
 *   enabled: true
 *   trigger: auto  # or manual, onFirstQuery
 *   output:
 *     location: "s3://bucket/data/"
 *     pattern: "type=sales/year=STAR/region=STAR/"
 *     format: parquet
 *     compression: snappy
 *   partition:
 *     columns: [year, region]
 *     batchBy: [year]
 *   columns:
 *     - name: region_code, type: VARCHAR, source: regionCode
 *     - name: year, type: INTEGER, source: fiscalYear
 *     - name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"
 *   options:
 *     threads: 4
 *     rowGroupSize: 100000
 * </pre>
 *
 * <h3>Trigger Types</h3>
 * <ul>
 *   <li>{@code auto} - Materialize when schema loads</li>
 *   <li>{@code manual} - Only via explicit API call</li>
 *   <li>{@code onFirstQuery} - Materialize on first table access</li>
 * </ul>
 *
 * @see MaterializeOutputConfig
 * @see MaterializePartitionConfig
 * @see MaterializeOptionsConfig
 * @see ColumnConfig
 */
public class MaterializeConfig {

  /**
   * Trigger types for materialization.
   */
  public enum Trigger {
    /** Materialize when schema loads. */
    AUTO,
    /** Only materialize via explicit API call. */
    MANUAL,
    /** Materialize on first table access. */
    ON_FIRST_QUERY
  }

  private final boolean enabled;
  private final Trigger trigger;
  private final MaterializeOutputConfig output;
  private final MaterializePartitionConfig partition;
  private final List<ColumnConfig> columns;
  private final MaterializeOptionsConfig options;
  private final String name;

  private MaterializeConfig(Builder builder) {
    this.enabled = builder.enabled != null ? builder.enabled : true;
    this.trigger = builder.trigger != null ? builder.trigger : Trigger.AUTO;
    this.output = builder.output;
    this.partition = builder.partition;
    this.columns = builder.columns != null
        ? Collections.unmodifiableList(new ArrayList<ColumnConfig>(builder.columns))
        : Collections.<ColumnConfig>emptyList();
    this.options = builder.options != null ? builder.options : MaterializeOptionsConfig.defaults();
    this.name = builder.name;
  }

  /**
   * Returns whether materialization is enabled.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the trigger type for materialization.
   */
  public Trigger getTrigger() {
    return trigger;
  }

  /**
   * Returns the output configuration.
   */
  public MaterializeOutputConfig getOutput() {
    return output;
  }

  /**
   * Returns the partition configuration.
   */
  public MaterializePartitionConfig getPartition() {
    return partition;
  }

  /**
   * Returns the column configurations.
   */
  public List<ColumnConfig> getColumns() {
    return columns;
  }

  /**
   * Returns the processing options.
   */
  public MaterializeOptionsConfig getOptions() {
    return options;
  }

  /**
   * Returns the name identifier for this materialization config.
   * Used for logging and tracking.
   */
  public String getName() {
    return name;
  }

  /**
   * Creates a new builder for MaterializeConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a MaterializeConfig from a YAML/JSON map.
   *
   * @param map Configuration map
   * @return MaterializeConfig instance, or null if not enabled
   */
  @SuppressWarnings("unchecked")
  public static MaterializeConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();

    // Parse enabled flag
    Object enabledObj = map.get("enabled");
    if (enabledObj instanceof Boolean) {
      builder.enabled((Boolean) enabledObj);
    }

    // Parse trigger
    Object triggerObj = map.get("trigger");
    if (triggerObj instanceof String) {
      builder.trigger(parseTrigger((String) triggerObj));
    }

    // Parse name
    Object nameObj = map.get("name");
    if (nameObj instanceof String) {
      builder.name((String) nameObj);
    }

    // Parse nested configs
    Object outputObj = map.get("output");
    if (outputObj instanceof Map) {
      builder.output(MaterializeOutputConfig.fromMap((Map<String, Object>) outputObj));
    }

    Object partitionObj = map.get("partition");
    if (partitionObj instanceof Map) {
      builder.partition(MaterializePartitionConfig.fromMap((Map<String, Object>) partitionObj));
    }

    Object columnsObj = map.get("columns");
    if (columnsObj instanceof List) {
      builder.columns(ColumnConfig.fromList((List<?>) columnsObj));
    }

    Object optionsObj = map.get("options");
    if (optionsObj instanceof Map) {
      builder.options(MaterializeOptionsConfig.fromMap((Map<String, Object>) optionsObj));
    }

    return builder.build();
  }

  private static Trigger parseTrigger(String value) {
    if (value == null) {
      return Trigger.AUTO;
    }
    switch (value.toLowerCase()) {
      case "auto":
        return Trigger.AUTO;
      case "manual":
        return Trigger.MANUAL;
      case "onfirstquery":
      case "on_first_query":
        return Trigger.ON_FIRST_QUERY;
      default:
        return Trigger.AUTO;
    }
  }

  /**
   * Builder for MaterializeConfig.
   */
  public static class Builder {
    private Boolean enabled;
    private Trigger trigger;
    private MaterializeOutputConfig output;
    private MaterializePartitionConfig partition;
    private List<ColumnConfig> columns;
    private MaterializeOptionsConfig options;
    private String name;

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder trigger(Trigger trigger) {
      this.trigger = trigger;
      return this;
    }

    public Builder output(MaterializeOutputConfig output) {
      this.output = output;
      return this;
    }

    public Builder partition(MaterializePartitionConfig partition) {
      this.partition = partition;
      return this;
    }

    public Builder columns(List<ColumnConfig> columns) {
      this.columns = columns;
      return this;
    }

    public Builder options(MaterializeOptionsConfig options) {
      this.options = options;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public MaterializeConfig build() {
      if (output == null) {
        throw new IllegalArgumentException("Output configuration is required");
      }
      return new MaterializeConfig(this);
    }
  }
}
