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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.IOException;

/**
 * Interface for domain-specific converters that transform raw files (JSON, CSV, XML, etc.)
 * into Parquet format with custom logic.
 *
 * <p>This interface allows adapters to register custom conversion logic that understands
 * domain-specific data structures, API response formats, and transformation requirements
 * that go beyond simple format-to-format conversion.
 *
 * <h3>Use Cases</h3>
 * <ul>
 *   <li><b>API Response Unwrapping</b> - Extract data from API metadata wrappers</li>
 *   <li><b>Nested Structure Flattening</b> - Convert complex JSON hierarchies to flat tables</li>
 *   <li><b>Time Series Pivoting</b> - Reshape data optimized for time series queries</li>
 *   <li><b>Multi-Table Splitting</b> - Split single API responses into multiple tables</li>
 *   <li><b>Domain Partitioning</b> - Apply semantic partitioning (by year, region, etc.)</li>
 * </ul>
 *
 * <h3>Integration with FileSchema</h3>
 * <p>FileSchema checks registered custom converters before applying its default conversion:
 * <pre>
 * 1. FileSchema detects a raw file needs conversion to Parquet
 * 2. Iterates through registered RawToParquetConverters
 * 3. If converter.canConvert() returns true:
 *    - Calls converter.convertToParquet()
 *    - If returns true, conversion complete
 *    - If returns false, tries next converter
 * 4. If no custom converter handles it, falls back to default ParquetConversionUtil
 * 5. Records conversion in .conversions.json regardless of which converter was used
 * </pre>
 *
 * <h3>Example: ECON Adapter</h3>
 * <pre>
 * public class EconRawToParquetConverter implements RawToParquetConverter {
 *   private final FredDataDownloader fredDownloader;
 *   // ... other downloaders ...
 *
 *   public boolean canConvert(String rawFilePath, ConversionMetadata metadata) {
 *     return rawFilePath.contains("govdata-production-cache/econ/");
 *   }
 *
 *   public boolean convertToParquet(String rawFilePath, String targetPath,
 *                                   StorageProvider provider) throws IOException {
 *     if (rawFilePath.contains("/fred/")) {
 *       fredDownloader.convertToParquet(sourceDir, targetPath);
 *       return true;
 *     }
 *     return false; // Let another converter or default handle it
 *   }
 * }
 * </pre>
 *
 * <h3>Registration</h3>
 * <pre>
 * FileSchema fileSchema = new FileSchema(...);
 * fileSchema.registerRawToParquetConverter(new EconRawToParquetConverter(...));
 * </pre>
 *
 * @see FileSchema#registerRawToParquetConverter(RawToParquetConverter)
 * @see org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil
 */
public interface RawToParquetConverter {

  /**
   * Checks if this converter can handle conversion for the given raw file.
   *
   * <p>Implementations should use file path patterns, metadata, or other heuristics
   * to determine if they understand the structure of this specific file.
   *
   * @param rawFilePath The absolute path to the raw source file
   * @param metadata Conversion metadata that may provide hints about file origin
   * @return true if this converter should handle this file, false otherwise
   */
  boolean canConvert(String rawFilePath, ConversionMetadata metadata);

  /**
   * Converts a raw file to Parquet format using domain-specific logic.
   *
   * <p>Implementations should:
   * <ul>
   *   <li>Read the raw file via StorageProvider (supports local, S3, HTTP)</li>
   *   <li>Apply domain-specific transformations (flatten, pivot, partition, etc.)</li>
   *   <li>Write Parquet file(s) to targetParquetPath via StorageProvider</li>
   *   <li>Return true if conversion succeeded, false to let another converter try</li>
   *   <li>Throw IOException if conversion fails unrecoverably</li>
   * </ul>
   *
   * <p><b>Important</b>: Do NOT manually update FileSchema's .conversions.json -
   * FileSchema handles that automatically after successful conversion.
   *
   * @param rawFilePath The absolute path to the raw source file to convert
   * @param targetParquetPath The absolute path where Parquet file should be written
   * @param storageProvider Provider for reading source and writing target files
   * @return true if conversion succeeded, false to try next converter or fall back to default
   * @throws IOException if conversion fails and should not be retried
   */
  boolean convertToParquet(String rawFilePath, String targetParquetPath,
      StorageProvider storageProvider) throws IOException;
}