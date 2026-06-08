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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

/**
 * Simplified factory for creating CsvNextGen schemas.
 * Supports configuration of execution engine and other parameters.
 */
public class CsvNextGenSchemaFactorySimple implements SchemaFactory {

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    // Extract configuration parameters
    String directory = (String) operand.get("directory");
    String engineType = (String) operand.getOrDefault("engine", "linq4j");
    Integer batchSize = (Integer) operand.getOrDefault("batchSize", 1024);
    Boolean header = (Boolean) operand.getOrDefault("header", true);

    if (directory == null) {
      throw new IllegalArgumentException("directory parameter is required");
    }

    File baseDirectory = new File(directory);
    if (!baseDirectory.exists() || !baseDirectory.isDirectory()) {
      throw new IllegalArgumentException("Directory does not exist: " + directory);
    }

    return new CsvNextGenSchemaSimple(baseDirectory, engineType,
        batchSize, header);
  }
}
