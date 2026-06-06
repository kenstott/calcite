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
package org.apache.calcite.adapter.file.format.csv;

import org.apache.calcite.adapter.file.table.CsvTable;
import org.apache.calcite.adapter.file.table.CsvTranslatableTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.Map;

/**
 * Factory that creates a {@link CsvTranslatableTable}.
 *
 * <p>Allows a file-based table to be included in a model.json file, even in a
 * schema that is not based upon {@link FileSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvTableFactory implements TableFactory<CsvTable> {
  // public constructor, per factory contract
  public CsvTableFactory() {
  }

  @Override public CsvTable create(SchemaPlus schema, String name,
      Map<String, Object> operand, @Nullable RelDataType rowType) {
    String fileName = (String) operand.get("file");
    final File base =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    final Source source = Sources.file(base, fileName);
    final RelProtoDataType protoRowType =
        rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return new CsvTranslatableTable(source, protoRowType);
  }
}
