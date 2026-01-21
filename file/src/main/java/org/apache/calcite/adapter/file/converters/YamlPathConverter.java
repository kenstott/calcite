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
package org.apache.calcite.adapter.file.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Converter that extracts data from YAML files using path expressions.
 * Since YAML is a superset of JSON, this converter simply:
 * 1. Converts YAML to JsonNode (which works because YAML is JSON-compatible)
 * 2. Uses the JsonPathConverter to do the actual path extraction
 * 3. Writes the result as YAML or JSON based on the output file extension
 */
public class YamlPathConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(YamlPathConverter.class);
  private static final YAMLMapper YAML_MAPPER = new YAMLMapper();
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /**
   * Extracts data from a YAML file using a path expression and writes it to an output file.
   * The output can be either YAML or JSON format based on the output file extension.
   *
   * @param sourceYaml The source YAML file to extract from
   * @param outputFile The output file (can be .yaml, .yml, or .json)
   * @param path The path expression to apply (uses same syntax as JsonPathConverter)
   * @throws IOException if extraction fails
   */
  public static void extract(File sourceYaml, File outputFile, String path, File baseDirectory) throws IOException {
    LOGGER.debug("Extracting from {} using path: {}", sourceYaml.getName(), path);

    // Read YAML file into JsonNode (YAML is JSON-compatible)
    JsonNode yamlData = YAML_MAPPER.readTree(sourceYaml);

    // Reuse the JsonPathConverter's path extraction logic
    JsonNode extractedData = JsonPathConverter.extractPath(yamlData, path);

    if (extractedData == null || extractedData.isNull()) {
      LOGGER.warn("Path {} not found in {}", path, sourceYaml.getName());
      extractedData = JSON_MAPPER.createObjectNode(); // Empty object
    }

    // Write output based on file extension
    String outputName = outputFile.getName().toLowerCase();
    try (FileWriter writer = new FileWriter(outputFile, StandardCharsets.UTF_8)) {
      if (outputName.endsWith(".yaml") || outputName.endsWith(".yml")) {
        // Write as YAML
        String yamlOutput = YAML_MAPPER.writeValueAsString(extractedData);
        writer.write(yamlOutput);
        LOGGER.debug("Wrote extracted YAML data to {}", outputFile.getName());
      } else if (outputName.endsWith(".json")) {
        // Write as JSON
        String jsonOutput = JSON_MAPPER.writerWithDefaultPrettyPrinter()
            .writeValueAsString(extractedData);
        writer.write(jsonOutput);
        LOGGER.debug("Wrote extracted JSON data to {}", outputFile.getName());
      } else {
        throw new IllegalArgumentException("Unsupported output format: " + outputName);
      }
    }

    // Record the conversion for refresh tracking
    ConversionRecorder.recordConversion(sourceYaml, outputFile,
        "JSONPATH_EXTRACTION[" + path + "]", baseDirectory);
  }

  /**
   * Convenience method to extract from YAML to JSON.
   */
  public static File extractToJson(File sourceYaml, String jsonPath, String outputName) throws IOException {
    File outputDir = sourceYaml.getParentFile();
    File outputFile = new File(outputDir, outputName + ".json");
    extract(sourceYaml, outputFile, jsonPath, outputDir.getParentFile());
    return outputFile;
  }

  /**
   * Convenience method to extract from YAML to YAML.
   */
  public static File extractToYaml(File sourceYaml, String jsonPath, String outputName) throws IOException {
    File outputDir = sourceYaml.getParentFile();
    File outputFile = new File(outputDir, outputName + ".yaml");
    extract(sourceYaml, outputFile, jsonPath, outputDir.getParentFile());
    return outputFile;
  }
}
