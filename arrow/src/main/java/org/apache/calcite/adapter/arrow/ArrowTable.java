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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Arrow Table.
 */
public class ArrowTable extends AbstractTable
    implements TranslatableTable, QueryableTable {
  private final @Nullable RelProtoDataType protoRowType;
  /** Arrow schema. (In Calcite terminology, more like a row type than a Schema.) */
  private final Schema schema;
  private final ArrowFileReader arrowFileReader;
  private final java.io.@Nullable File sourceFile;

  public ArrowTable(@Nullable RelProtoDataType protoRowType, ArrowFileReader arrowFileReader) {
    this(protoRowType, arrowFileReader, null);
  }

  public ArrowTable(@Nullable RelProtoDataType protoRowType, ArrowFileReader arrowFileReader,
      java.io.@Nullable File sourceFile) {
    try {
      this.schema = arrowFileReader.getVectorSchemaRoot().getSchema();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
    this.protoRowType = protoRowType;
    this.arrowFileReader = arrowFileReader;
    this.sourceFile = sourceFile;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /** Called via code generation; see uses of
   * {@link org.apache.calcite.adapter.arrow.ArrowMethod#ARROW_QUERY}. */
  @SuppressWarnings("unused")
  public Enumerable<Object> query(DataContext root, ImmutableIntList fields,
      List<String> conditions) {
    requireNonNull(fields, "fields");

    // Gandiva is optional (see GandivaAvailability). Without it, serve the scan straight off the
    // Arrow vectors and let Calcite's Enumerable convention apply filters and projections. Every
    // reference to the Gandiva API lives in GandivaEvaluators so that this class still loads when
    // those classes are absent — it is reached via Class.forName, which initialises it.
    if (!GandivaAvailability.isAvailable()) {
      if (!conditions.isEmpty()) {
        throw new IllegalStateException(
            "Arrow filter pushdown requires Gandiva, which is not available; "
                + "ArrowRules should not have pushed " + conditions);
      }
      return new ArrowEnumerable(openReader(), fields, null, null);
    }

    final Object projector;
    final Object filter;
    if (conditions.isEmpty()) {
      filter = null;
      projector = GandivaEvaluators.makeProjector(schema, fields);
    } else {
      projector = null;
      filter = GandivaEvaluators.makeFilter(schema, conditions);
    }

    return new ArrowEnumerable(openReader(), fields, projector, filter);
  }

  /** A reader positioned at the start of the file, so each query reads from the beginning. */
  private ArrowFileReader openReader() {
    if (sourceFile == null) {
      return arrowFileReader;
    }
    try {
      java.io.FileInputStream fileInputStream = new java.io.FileInputStream(sourceFile);
      SeekableReadChannel seekableReadChannel =
          new SeekableReadChannel(fileInputStream.getChannel());
      BufferAllocator allocator = new RootAllocator();
      return new ArrowFileReader(seekableReadChannel, allocator);
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final ImmutableIntList fields =
        ImmutableIntList.copyOf(Util.range(fieldCount));
    final RelOptCluster cluster = context.getCluster();
    return new ArrowTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION),
        relOptTable, this, fields);
  }

  private static RelDataType deduceRowType(Schema schema,
      JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(),
          ArrowFieldTypeFactory.toType(field.getType(), typeFactory));
    }
    return builder.build();
  }

}
