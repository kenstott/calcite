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
import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scannable table based on a CSV file.
 * Used for Parquet conversion where we need to scan the table data.
 */
public class CsvScannableTable extends CsvTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvScannableTable.class);

  /** Creates a CsvScannableTable. */
  public CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType) {
    super(source, protoRowType);
  }

  /** Creates a CsvScannableTable with column casing. */
  public CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType, String columnCasing) {
    super(source, protoRowType, columnCasing);
  }

  /** Creates a CsvScannableTable with column casing and type inference. */
  public CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType, String columnCasing,
      CsvTypeInferrer.TypeInferenceConfig typeInferenceConfig) {
    super(source, protoRowType, columnCasing, typeInferenceConfig);
  }

  @Override public String toString() {
    return "CsvScannableTable";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    JavaTypeFactory typeFactory = root.getTypeFactory();
    final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
    final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        return new CsvEnumerator<>(source, cancelFlag, false, null,
            CsvEnumerator.arrayConverter(fieldTypes, fields, false));
      }
    };
  }
}
