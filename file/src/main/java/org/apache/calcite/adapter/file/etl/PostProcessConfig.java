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
 * Configuration for post-processing scripts at column, table, or schema level.
 *
 * <p>Post-processing scripts are executed after data materialization to perform
 * expensive operations like GPU-based embedding generation that are better done
 * in bulk rather than row-by-row.
 *
 * <p>Example YAML configuration:
 * <pre>
 * # Column-level bulk generator
 * columns:
 *   - name: embedding
 *     type: array&lt;double&gt;
 *     bulkGenerator:
 *       script: "scripts/vss-gpu-runner.sh"
 *       args: ["--years", "${year}"]
 *       onFailure: "error"
 *
 * # Table-level post-process
 * postProcess:
 *   - name: gpu_embeddings
 *     script: "scripts/vss-gpu-runner.sh"
 *     args: ["--table", "${table}"]
 *     runAfter: "all_partitions"
 *     onFailure: "warn"
 *
 * # Schema-level post-process
 * schema:
 *   postProcess:
 *     - name: vss_refresh
 *       script: "scripts/vss.sh"
 *       args: ["refresh"]
 *       dependsOn: ["vectorized_chunks"]
 * </pre>
 */
public class PostProcessConfig {

  /**
   * Failure handling mode.
   */
  public enum OnFailure {
    /** Log warning and continue processing. */
    WARN,
    /** Stop processing and propagate error. */
    ERROR
  }

  /**
   * When to run table-level post-processing.
   */
  public enum RunAfter {
    /** Run after each partition is materialized. */
    EACH_PARTITION,
    /** Run once after all partitions are materialized. */
    ALL_PARTITIONS
  }

  private final String name;
  private final String script;
  private final List<String> args;
  private final OnFailure onFailure;
  private final RunAfter runAfter;
  private final List<String> dependsOn;
  private final String condition;
  private final boolean async;

  private PostProcessConfig(Builder builder) {
    this.name = builder.name;
    this.script = builder.script;
    this.args = Collections.unmodifiableList(new ArrayList<>(builder.args));
    this.onFailure = builder.onFailure;
    this.runAfter = builder.runAfter;
    this.dependsOn = Collections.unmodifiableList(new ArrayList<>(builder.dependsOn));
    this.condition = builder.condition;
    this.async = builder.async;
  }

  public String getName() {
    return name;
  }

  public String getScript() {
    return script;
  }

  public List<String> getArgs() {
    return args;
  }

  public OnFailure getOnFailure() {
    return onFailure;
  }

  public RunAfter getRunAfter() {
    return runAfter;
  }

  public List<String> getDependsOn() {
    return dependsOn;
  }

  public String getCondition() {
    return condition;
  }

  public boolean isAsync() {
    return async;
  }

  /**
   * Creates a PostProcessConfig from a YAML map.
   *
   * @param config Map from YAML parsing
   * @return PostProcessConfig instance
   */
  @SuppressWarnings("unchecked")
  public static PostProcessConfig fromMap(Map<String, Object> config) {
    if (config == null) {
      return null;
    }

    Builder builder = builder();

    if (config.containsKey("name")) {
      builder.name((String) config.get("name"));
    }

    if (config.containsKey("script")) {
      builder.script((String) config.get("script"));
    }

    if (config.containsKey("args")) {
      Object argsObj = config.get("args");
      if (argsObj instanceof List) {
        for (Object arg : (List<?>) argsObj) {
          builder.addArg(String.valueOf(arg));
        }
      }
    }

    if (config.containsKey("onFailure")) {
      String onFailureStr = (String) config.get("onFailure");
      builder.onFailure("error".equalsIgnoreCase(onFailureStr)
          ? OnFailure.ERROR : OnFailure.WARN);
    }

    if (config.containsKey("runAfter")) {
      String runAfterStr = (String) config.get("runAfter");
      builder.runAfter("each_partition".equalsIgnoreCase(runAfterStr)
          ? RunAfter.EACH_PARTITION : RunAfter.ALL_PARTITIONS);
    }

    if (config.containsKey("dependsOn")) {
      Object depsObj = config.get("dependsOn");
      if (depsObj instanceof List) {
        for (Object dep : (List<?>) depsObj) {
          builder.addDependsOn(String.valueOf(dep));
        }
      }
    }

    if (config.containsKey("condition")) {
      builder.condition((String) config.get("condition"));
    }

    if (config.containsKey("async")) {
      Object asyncObj = config.get("async");
      builder.async(Boolean.TRUE.equals(asyncObj)
          || "true".equalsIgnoreCase(String.valueOf(asyncObj)));
    }

    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for PostProcessConfig.
   */
  public static class Builder {
    private String name = "post_process";
    private String script;
    private List<String> args = new ArrayList<>();
    private OnFailure onFailure = OnFailure.ERROR;
    private RunAfter runAfter = RunAfter.ALL_PARTITIONS;
    private List<String> dependsOn = new ArrayList<>();
    private String condition;
    private boolean async = false;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder script(String script) {
      this.script = script;
      return this;
    }

    public Builder args(List<String> args) {
      this.args = new ArrayList<>(args);
      return this;
    }

    public Builder addArg(String arg) {
      this.args.add(arg);
      return this;
    }

    public Builder onFailure(OnFailure onFailure) {
      this.onFailure = onFailure;
      return this;
    }

    public Builder runAfter(RunAfter runAfter) {
      this.runAfter = runAfter;
      return this;
    }

    public Builder dependsOn(List<String> dependsOn) {
      this.dependsOn = new ArrayList<>(dependsOn);
      return this;
    }

    public Builder addDependsOn(String dep) {
      this.dependsOn.add(dep);
      return this;
    }

    public Builder condition(String condition) {
      this.condition = condition;
      return this;
    }

    public Builder async(boolean async) {
      this.async = async;
      return this;
    }

    public PostProcessConfig build() {
      if (script == null || script.isEmpty()) {
        throw new IllegalStateException("PostProcessConfig requires a script");
      }
      return new PostProcessConfig(this);
    }
  }

  @Override
  public String toString() {
    return "PostProcessConfig{"
        + "name='" + name + '\''
        + ", script='" + script + '\''
        + ", args=" + args
        + ", onFailure=" + onFailure
        + ", runAfter=" + runAfter
        + ", async=" + async
        + '}';
  }
}
