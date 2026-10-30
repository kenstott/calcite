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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.converters.SourceFileConversion;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Refreshable JSON table that re-reads the source file when modified.
 * Also monitors original source files (HTML, Excel, XML) and re-converts them when changed.
 */
public class RefreshableJsonTable extends AbstractRefreshableTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshableJsonTable.class);

  private final Source source;
  private final String columnNameCasing;
  private final @Nullable File operatingCacheDirectory;
  // The conversion the schema ran on the table's original source, re-run on its change.
  private final SourceFileConversion conversion;
  private @Nullable RelDataType rowType;
  private @Nullable List<Object> dataList;
  private @Nullable ConversionMetadata conversionMetadata;

  /**
   * @param conversion How the schema converts its source files; the table's JSON is read with
   *     its column casing, and its original source is re-converted with it when it changes
   */
  public RefreshableJsonTable(Source source, String tableName, @Nullable Duration refreshInterval,
      SourceFileConversion conversion) {
    super(tableName, refreshInterval);
    this.source = source;
    this.conversion = conversion;
    this.columnNameCasing = conversion.columnNameCasing();
    this.operatingCacheDirectory = conversion.baseDirectory();
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = JsonEnumerator.deduceRowType(typeFactory, source, (Map<String, Object>) null, columnNameCasing).getRelDataType();
    }
    return rowType;
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    // Check and refresh if needed before scanning
    refresh();

    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        return new JsonEnumerator(getDataList(typeFactory));
      }
    };
  }

  private List<Object> getDataList(RelDataTypeFactory typeFactory) {
    if (dataList == null) {
      JsonEnumerator.JsonDataConverter jsonDataConverter =
          JsonEnumerator.deduceRowType(typeFactory, source, (Map<String, Object>) null, columnNameCasing);
      dataList = jsonDataConverter.getDataList();
    }
    return dataList;
  }

  @Override protected void doRefresh() {
    File jsonFile = source.file();
    if (jsonFile == null) {
      return;
    }

    // Initialize conversion metadata if not already done
    if (conversionMetadata == null && operatingCacheDirectory != null) {
      try {
        conversionMetadata = new ConversionMetadata(operatingCacheDirectory);
      } catch (Exception e) {
        LOGGER.debug("Could not initialize conversion metadata: {}", e.getMessage());
      }
    }

    // Check if original source file needs re-conversion
    boolean needsReconversion = false;
    File originalSource = null;

    if (conversionMetadata != null) {
      try {
        originalSource = conversionMetadata.findOriginalSource(jsonFile);
        if (originalSource != null && originalSource.exists()) {
          // Check if original source has been modified
          if (isFileModified(originalSource)) {
            needsReconversion = true;
            LOGGER.debug("Original source {} has been modified, triggering re-conversion",
                originalSource.getName());
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error checking original source: {}", e.getMessage());
      }
    }

    // If original source changed, re-convert it
    if (needsReconversion && originalSource != null) {
      try {
        File outputDir = jsonFile.getParentFile();

        // Check if this is a JSONPath extraction
        ConversionMetadata.ConversionRecord record = conversionMetadata.getConversionRecord(jsonFile);
        if (record != null && record.getConversionType() != null
            && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
          // Extract the JSONPath from the conversion type
          String jsonPath = record.getConversionType().substring("JSONPATH_EXTRACTION:".length());

          // Re-run the JSONPath extraction
          org.apache.calcite.adapter.file.converters.JsonPathConverter.extract(
              originalSource, jsonFile, jsonPath, outputDir.getParentFile());
          LOGGER.debug("Re-extracted JSONPath {} from {} to {}",
              jsonPath, originalSource.getName(), jsonFile.getName());
        } else {
          // Re-run the conversion the schema first ran, so it writes what that one wrote.
          // Into the directory of the JSON this table reads: where the first conversion wrote it.
          conversion.reconvert(originalSource, outputDir);
          LOGGER.debug("Re-converted {}", originalSource.getName());
        }

        // Update the last modified time of the original source
        updateLastModified(originalSource);

        // Clear cached data to force re-read of new JSON
        dataList = null;
        rowType = null;
      } catch (Exception e) {
        LOGGER.warn("Failed to re-convert original source {}: {}",
            originalSource.getName(), e.getMessage());
      }
    }

    // Also check if JSON file itself has been modified directly
    if (isFileModified(jsonFile)) {
      // Clear cached data to force re-read
      dataList = null;
      rowType = null;
      updateLastModified(jsonFile);
    }
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  @Override public String toString() {
    return "RefreshableJsonTable(" + tableName + ")";
  }
}
