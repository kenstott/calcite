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
package org.apache.calcite.adapter.file.format.json;
// storage-provider-guard:ignore-file - audited: writes a local newline-delimited JSON cache file for
// DuckDB to read; genuinely local filesystem I/O, not object-store.

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Materializes a Calcite {@link Table} to a newline-delimited JSON (NDJSON) file so the DuckDB
 * engine can read it via {@code read_json_auto()} — the Hadoop-free counterpart to
 * {@link org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil}.
 *
 * <p>Used for the DuckDB engine when CSV/Parquet conversion is undesirable (it requires the Hadoop
 * ParquetWriter, which cannot run on the JDK 25 that Trino requires). Because it serializes the rows
 * the supplied table already produces, the materialized JSON has exactly the columns the in-scan
 * transform would have produced — JSON/YAML flattening and JSONPath extraction included — with no
 * column-name divergence.
 */
public final class JsonConversionUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonConversionUtil() {
  }

  /**
   * Scans {@code table} and writes one JSON object per row to {@code targetFile} (NDJSON). Returns
   * {@code false} when the table is not scannable (the caller should fall back). The write is atomic
   * (temp file + rename) so a concurrent DuckDB read never sees a partial file.
   */
  public static boolean writeTableAsJson(Table table, File targetFile) throws Exception {
    if (!(table instanceof ScannableTable)) {
      return false;
    }
    ScannableTable scannable = (ScannableTable) table;

    final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    DataContext dataContext = new DataContext() {
      @Override public SchemaPlus getRootSchema() {
        return null;
      }
      @Override public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
      }
      @Override public QueryProvider getQueryProvider() {
        return null;
      }
      @Override public Object get(String name) {
        if (DataContext.Variable.CANCEL_FLAG.camelName.equals(name)) {
          return cancelFlag;
        }
        return null;
      }
    };

    JavaTypeFactory typeFactory = (JavaTypeFactory) dataContext.getTypeFactory();
    RelDataType rowType = table.getRowType(typeFactory);
    List<String> fieldNames = rowType.getFieldNames();

    File parent = targetFile.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    }
    File tempFile = new File(targetFile.getAbsolutePath() + ".tmp." + java.util.UUID.randomUUID());

    Enumerable<Object[]> rows = scannable.scan(dataContext);
    try (Writer writer = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
         Enumerator<Object[]> enumerator = rows.enumerator()) {
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        Map<String, Object> obj = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
          obj.put(fieldNames.get(i), i < row.length ? row[i] : null);
        }
        writer.write(MAPPER.writeValueAsString(obj));
        writer.write("\n");
      }
    }

    Files.move(tempFile.toPath(), targetFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    return true;
  }
}
