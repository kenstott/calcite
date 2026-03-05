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
package org.apache.calcite.adapter.govdata.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Configuration for ETL runner, parsed from CLI arguments and model file.
 *
 * <p>CLI options:
 * <ul>
 *   <li>{@code --model <file>} (required) - Path to model JSON file</li>
 *   <li>{@code --compact} - Minimal output, suitable for scripting</li>
 *   <li>{@code --dry-run} - Validate model without executing ETL</li>
 *   <li>{@code --verbose} - Detailed progress output</li>
 *   <li>{@code --manifest <file>} - Custom manifest file path</li>
 * </ul>
 */
public class EtlRunConfig {

  private final File modelFile;
  private final boolean compact;
  private final boolean compactOnly;
  private final boolean dryRun;
  private final boolean verbose;
  private final File manifestFile;
  private final JsonNode modelJson;
  private final List<SchemaConfig> schemas;

  private EtlRunConfig(Builder builder) throws IOException {
    this.modelFile = builder.modelFile;
    this.compact = builder.compact;
    this.compactOnly = builder.compactOnly;
    this.dryRun = builder.dryRun;
    this.verbose = builder.verbose;
    this.manifestFile = builder.manifestFile;
    this.modelJson = parseModelFile(builder.modelFile);
    this.schemas = extractSchemas(this.modelJson);
  }

  /**
   * Parse CLI arguments and create configuration.
   *
   * @param args command line arguments
   * @return parsed configuration
   * @throws IllegalArgumentException if required arguments are missing
   * @throws IOException if model file cannot be read
   */
  public static EtlRunConfig fromArgs(String[] args) throws IOException {
    Builder builder = new Builder();

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      switch (arg) {
        case "--model":
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--model requires a file path");
          }
          builder.modelFile(new File(args[++i]));
          break;
        case "--compact":
          builder.compact(true);
          break;
        case "--compact-only":
          builder.compactOnly(true);
          break;
        case "--dry-run":
          builder.dryRun(true);
          break;
        case "--verbose":
          builder.verbose(true);
          break;
        case "--manifest":
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--manifest requires a file path");
          }
          builder.manifestFile(new File(args[++i]));
          break;
        case "--help":
        case "-h":
          printUsage();
          System.exit(0);
          break;
        default:
          if (arg.startsWith("-")) {
            throw new IllegalArgumentException("Unknown option: " + arg);
          }
          // Positional argument - treat as model file if not set
          if (builder.modelFile == null) {
            builder.modelFile(new File(arg));
          } else {
            throw new IllegalArgumentException("Unexpected argument: " + arg);
          }
      }
    }

    return builder.build();
  }

  private static void printUsage() {
    System.out.println("Usage: etl-runner [OPTIONS] --model <file>");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --model <file>    Model JSON file (required)");
    System.out.println("  --compact         Minimal output for scripting");
    System.out.println("  --compact-only    Scan and compact tracker data only (no ETL)");
    System.out.println("  --dry-run         Validate model without executing ETL");
    System.out.println("  --verbose         Detailed progress output");
    System.out.println("  --manifest <file> Custom manifest file path");
    System.out.println("  --help, -h        Show this help message");
    System.out.println();
    System.out.println("Exit codes:");
    System.out.println("  0  SUCCESS - All schemas processed successfully");
    System.out.println("  1  PARTIAL - Some schemas failed, some succeeded");
    System.out.println("  2  FAILED  - Critical error, no schemas processed");
  }

  private JsonNode parseModelFile(File file) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readTree(file);
  }

  private List<SchemaConfig> extractSchemas(JsonNode modelJson) {
    List<SchemaConfig> result = new ArrayList<>();
    JsonNode schemasNode = modelJson.get("schemas");

    if (schemasNode == null || !schemasNode.isArray()) {
      throw new IllegalArgumentException("Model file must contain 'schemas' array");
    }

    for (JsonNode schemaNode : schemasNode) {
      String name = schemaNode.has("name") ? schemaNode.get("name").asText() : null;
      String type = schemaNode.has("type") ? schemaNode.get("type").asText() : null;
      String factory = schemaNode.has("factory") ? schemaNode.get("factory").asText() : null;

      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Each schema must have a 'name'");
      }

      Map<String, Object> operand = new HashMap<>();
      JsonNode operandNode = schemaNode.get("operand");
      if (operandNode != null && operandNode.isObject()) {
        Iterator<Map.Entry<String, JsonNode>> fields = operandNode.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          operand.put(field.getKey(), jsonNodeToObject(field.getValue()));
        }
      }

      result.add(new SchemaConfig(name, type, factory, operand));
    }

    return result;
  }

  private Object jsonNodeToObject(JsonNode node) {
    if (node.isTextual()) {
      return node.asText();
    } else if (node.isInt()) {
      return node.asInt();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isDouble()) {
      return node.asDouble();
    } else if (node.isBoolean()) {
      return node.asBoolean();
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
    } else if (node.isNull()) {
      return null;
    }
    return node.toString();
  }

  // Getters

  public File getModelFile() {
    return modelFile;
  }

  public boolean isCompact() {
    return compact;
  }

  public boolean isCompactOnly() {
    return compactOnly;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public File getManifestFile() {
    return manifestFile;
  }

  public JsonNode getModelJson() {
    return modelJson;
  }

  public List<SchemaConfig> getSchemas() {
    return schemas;
  }

  public String getDefaultSchema() {
    JsonNode defaultSchema = modelJson.get("defaultSchema");
    return defaultSchema != null ? defaultSchema.asText() : null;
  }

  /**
   * Configuration for a single schema within the model.
   */
  public static class SchemaConfig {
    private final String name;
    private final String type;
    private final String factory;
    private final Map<String, Object> operand;

    public SchemaConfig(String name, String type, String factory, Map<String, Object> operand) {
      this.name = name;
      this.type = type;
      this.factory = factory;
      this.operand = operand;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public String getFactory() {
      return factory;
    }

    public Map<String, Object> getOperand() {
      return operand;
    }

    public String getDataSource() {
      Object ds = operand.get("dataSource");
      return ds != null ? ds.toString() : null;
    }
  }

  /**
   * Builder for EtlRunConfig.
   */
  public static class Builder {
    private File modelFile;
    private boolean compact = false;
    private boolean compactOnly = false;
    private boolean dryRun = false;
    private boolean verbose = false;
    private File manifestFile;

    public Builder modelFile(File modelFile) {
      this.modelFile = modelFile;
      return this;
    }

    public Builder compact(boolean compact) {
      this.compact = compact;
      return this;
    }

    public Builder compactOnly(boolean compactOnly) {
      this.compactOnly = compactOnly;
      return this;
    }

    public Builder dryRun(boolean dryRun) {
      this.dryRun = dryRun;
      return this;
    }

    public Builder verbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }

    public Builder manifestFile(File manifestFile) {
      this.manifestFile = manifestFile;
      return this;
    }

    public EtlRunConfig build() throws IOException {
      if (modelFile == null) {
        throw new IllegalArgumentException("Model file is required (--model <file>)");
      }
      if (!modelFile.exists()) {
        throw new IllegalArgumentException("Model file not found: " + modelFile);
      }
      if (!modelFile.isFile()) {
        throw new IllegalArgumentException("Model path is not a file: " + modelFile);
      }
      return new EtlRunConfig(this);
    }
  }
}
