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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Table based on a JSON file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class JsonScannableTable extends JsonTable
    implements ScannableTable {
  /**
   * Creates a JsonScannableTable.
   */
  public JsonScannableTable(Source source) {
    super(source);
  }

  /**
   * Creates a JsonScannableTable with options.
   */
  public JsonScannableTable(Source source, Map<String, Object> options) {
    super(source, options);
  }

  /**
   * Creates a JsonScannableTable with options and column casing.
   */
  public JsonScannableTable(Source source, Map<String, Object> options, String columnNameCasing) {
    super(source, options, columnNameCasing);
  }

  @Override public String toString() {
    return "JsonScannableTable";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<Object> dataList = getDataList(typeFactory);
        return new JsonEnumerator(dataList);
      }
    };
  }
}
