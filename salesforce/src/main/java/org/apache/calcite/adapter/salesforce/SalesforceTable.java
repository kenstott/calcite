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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Table based on a Salesforce sObject.
 */
public class SalesforceTable extends AbstractQueryableTable
    implements TranslatableTable, ModifiableTable {

  private final SalesforceSchema schema;
  private final String sObjectType;
  private RelDataType rowType;
  /** Describe metadata for each column, parallel to {@link #rowType}. */
  private List<SalesforceConnection.FieldDescription> columns;
  /** Ids of the records created since {@link #takeInsertedKeys} was last called. */
  private final List<String> insertedKeys = new ArrayList<>();

  public SalesforceTable(SalesforceSchema schema, String sObjectType) {
    super(Object[].class);
    this.schema = schema;
    this.sObjectType = sObjectType;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = createRowType(typeFactory);
    }
    return rowType;
  }

  private RelDataType createRowType(RelDataTypeFactory typeFactory) {
    SalesforceConnection.SObjectDescription description = schema.getDescription(sObjectType);
    List<RelDataTypeField> fields = new ArrayList<>();
    List<SalesforceConnection.FieldDescription> columnList = new ArrayList<>();

    // Always include Id field first
    SalesforceConnection.FieldDescription idField = null;
    for (SalesforceConnection.FieldDescription field : description.fields) {
      if ("Id".equals(field.name)) {
        idField = field;
      }
    }
    if (idField == null) {
      throw new IllegalStateException("Describe of " + sObjectType + " has no Id field");
    }
    fields.add(
        new RelDataTypeFieldImpl("Id", 0,
            withWriteNullability(typeFactory,
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 18), idField)));
    columnList.add(idField);

    int index = 1;
    for (SalesforceConnection.FieldDescription field : description.fields) {
      if ("Id".equals(field.name)) {
        continue; // Already added
      }

      RelDataType fieldType = convertFieldType(typeFactory, field);
      fields.add(new RelDataTypeFieldImpl(field.name, index++, fieldType));
      columnList.add(field);
    }

    columns = columnList;
    return typeFactory.createStructType(fields);
  }

  private RelDataType convertFieldType(RelDataTypeFactory typeFactory,
      SalesforceConnection.FieldDescription field) {
    SqlTypeName typeName;
    Integer precision = null;

    switch (field.type.toLowerCase(Locale.ROOT)) {
    case "id":
    case "reference":
    case "string":
    case "picklist":
    case "multipicklist":
    case "textarea":
    case "phone":
    case "email":
    case "url":
    case "combobox":
      typeName = SqlTypeName.VARCHAR;
      precision = field.length > 0 ? field.length : null;
      break;

    case "boolean":
      typeName = SqlTypeName.BOOLEAN;
      break;

    case "int":
    case "integer":
      typeName = SqlTypeName.INTEGER;
      break;

    case "double":
    case "percent":
      typeName = SqlTypeName.DOUBLE;
      break;

    case "currency":
    case "decimal":
      typeName = SqlTypeName.DECIMAL;
      precision = 19; // Salesforce currency precision
      break;

    case "date":
      typeName = SqlTypeName.DATE;
      break;

    case "datetime":
      typeName = SqlTypeName.TIMESTAMP;
      break;

    case "time":
      typeName = SqlTypeName.TIME;
      break;

    default:
      // Default to VARCHAR for unknown types
      typeName = SqlTypeName.VARCHAR;
    }

    RelDataType baseType;
    if (precision != null && typeName == SqlTypeName.VARCHAR) {
      baseType = typeFactory.createSqlType(typeName, precision);
    } else if (typeName == SqlTypeName.DECIMAL) {
      baseType = typeFactory.createSqlType(typeName, precision, 2);
    } else {
      baseType = typeFactory.createSqlType(typeName);
    }

    return withWriteNullability(typeFactory, baseType, field);
  }

  /**
   * Nullable if the field allows nulls, or if an INSERT may omit it because
   * Salesforce populates it (system fields, fields defaulted on create).
   */
  private static RelDataType withWriteNullability(RelDataTypeFactory typeFactory,
      RelDataType baseType, SalesforceConnection.FieldDescription field) {
    if (field.nillable || !field.createable || field.defaultedOnCreate) {
      return typeFactory.createTypeWithNullability(baseType, true);
    }
    return baseType;
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      @Override public Enumerator<T> enumerator() {
        throw new UnsupportedOperationException(
            "Salesforce tables are read through SalesforceToEnumerableConverter");
      }
    };
  }

  @Override public <C> @Nullable C unwrap(Class<C> aClass) {
    if (aClass == InitializerExpressionFactory.class) {
      return aClass.cast(new SalesforceInitializerExpressionFactory());
    }
    return super.unwrap(aClass);
  }

  @Override public @Nullable Collection getModifiableCollection() {
    // Writes are executed by SalesforceTableModify, not through a collection
    return null;
  }

  @Override public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input,
      TableModify.Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    // INSERT ... VALUES has no table scan to register the adapter's rules
    for (RelOptRule rule : SalesforceRules.RULES) {
      cluster.getPlanner().addRule(rule);
    }
    return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE),
        table, catalogReader, input, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  /**
   * Name of the column that identifies a row. With {@link #takeInsertedKeys} this is what a
   * server needs to answer {@code INSERT/UPDATE/DELETE ... RETURNING}: Calcite has no RETURNING,
   * so the rows are read back by key. pgwire-calcite looks these two methods up by name.
   */
  public String getKeyColumn() {
    return "Id";
  }

  /**
   * Returns the Ids of the records created through this table since the last call, in creation
   * order, and forgets them. Salesforce assigns the Id, so a caller that needs the rows it just
   * inserted calls this before the INSERT (to discard earlier Ids) and again after it.
   */
  public List<String> takeInsertedKeys() {
    synchronized (insertedKeys) {
      List<String> keys = new ArrayList<>(insertedKeys);
      insertedKeys.clear();
      return keys;
    }
  }

  /**
   * Executes an INSERT, UPDATE or DELETE. Called from code generated by
   * {@link SalesforceTableModify}.
   *
   * <p>Rows are written in batches of
   * {@link SalesforceConnection#COLLECTION_BATCH_SIZE}; each batch is atomic,
   * but a statement spanning several batches is not.
   *
   * @param operation     INSERT, UPDATE or DELETE
   * @param rows          for INSERT, one value per table column; for DELETE,
   *                      the table row; for UPDATE, the table row followed by
   *                      one new value per update column
   * @param updateColumns comma-separated update column names (UPDATE only)
   * @return single-element enumerable holding the affected row count
   */
  public Enumerable<Long> modify(String operation, Enumerable<Object[]> rows,
      String updateColumns) {
    final SalesforceConnection connection = schema.getConnection();
    final List<Object[]> rowList = rows.toList();
    long count = 0;
    try {
      for (int from = 0; from < rowList.size();
           from += SalesforceConnection.COLLECTION_BATCH_SIZE) {
        List<Object[]> batch =
            rowList.subList(from,
                Math.min(rowList.size(), from + SalesforceConnection.COLLECTION_BATCH_SIZE));
        switch (TableModify.Operation.valueOf(operation)) {
        case INSERT:
          List<String> ids = connection.createRecords(sObjectType, insertRecords(batch));
          synchronized (insertedKeys) {
            insertedKeys.addAll(ids);
          }
          count += ids.size();
          break;
        case UPDATE:
          count += connection.updateRecords(sObjectType,
              updateRecords(batch, Arrays.asList(updateColumns.split(","))));
          break;
        case DELETE:
          List<String> deleted = new ArrayList<>();
          for (Object[] row : batch) {
            deleted.add((String) row[0]);
          }
          count += connection.deleteRecords(deleted);
          break;
        default:
          throw new UnsupportedOperationException(
              operation + " is not supported on Salesforce tables");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(operation + " on " + sObjectType + " failed after "
          + count + " rows were written", e);
    }
    return Linq4j.singletonEnumerable(count);
  }

  private List<Map<String, Object>> insertRecords(List<Object[]> batch) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<Map<String, Object>> records = new ArrayList<>();
    for (Object[] row : batch) {
      Map<String, Object> record = new LinkedHashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        if (row[i] == null) {
          // Omitted columns arrive as NULL; Salesforce applies its own default
          continue;
        }
        SalesforceConnection.FieldDescription column = columns.get(i);
        if (!column.createable) {
          throw new IllegalArgumentException(
              sObjectType + "." + column.name + " is not createable");
        }
        record.put(column.name, toSalesforceValue(row[i], fields.get(i).getType()));
      }
      records.add(record);
    }
    return records;
  }

  private List<Map<String, Object>> updateRecords(List<Object[]> batch,
      List<String> updateColumns) {
    int tableFieldCount = rowType.getFieldCount();
    List<Map<String, Object>> records = new ArrayList<>();
    for (Object[] row : batch) {
      Map<String, Object> record = new LinkedHashMap<>();
      record.put("Id", row[0]);
      for (int j = 0; j < updateColumns.size(); j++) {
        RelDataTypeField field = rowType.getField(updateColumns.get(j), true, false);
        if (field == null) {
          throw new IllegalStateException(
              "Update column " + updateColumns.get(j) + " is not a column of " + sObjectType);
        }
        SalesforceConnection.FieldDescription column = columns.get(field.getIndex());
        if (!column.updateable) {
          throw new IllegalArgumentException(
              sObjectType + "." + column.name + " is not updateable");
        }
        // A null here is an explicit SET col = NULL, which clears the field
        record.put(column.name,
            toSalesforceValue(row[tableFieldCount + j], field.getType()));
      }
      records.add(record);
    }
    return records;
  }

  /** Converts a value from Calcite's internal representation to Salesforce JSON. */
  private static @Nullable Object toSalesforceValue(@Nullable Object value, RelDataType type) {
    if (value == null) {
      return null;
    }
    switch (type.getSqlTypeName()) {
    case DATE:
      return LocalDate.ofEpochDay(((Number) value).longValue()).toString();
    case TIMESTAMP:
      return Instant.ofEpochMilli(((Number) value).longValue()).toString();
    case TIME:
      return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L) + "Z";
    case DECIMAL:
      return value instanceof BigDecimal ? value : new BigDecimal(value.toString());
    default:
      return value;
    }
  }

  /**
   * Column strategies for INSERT: fields Salesforce populates itself (Id,
   * audit fields, formulas) cannot be targeted; everything else follows its
   * nullability.
   */
  private class SalesforceInitializerExpressionFactory
      extends NullInitializerExpressionFactory {
    @Override public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
      if (!columns.get(iColumn).createable) {
        return ColumnStrategy.STORED;
      }
      return super.generationStrategy(table, iColumn);
    }
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();
    return new SalesforceTableScan(cluster, relOptTable, this, sObjectType);
  }

  /**
   * Get the Salesforce schema.
   */
  public SalesforceSchema getSalesforceSchema() {
    return schema;
  }

  /**
   * Get the sObject type name.
   */
  public String getSObjectType() {
    return sObjectType;
  }

  /**
   * Execute a SOQL query and return results as an Enumerable.
   *
   * @param root         execution context, the source of bind parameter values
   * @param soqlTemplate SOQL, with tokens for any bind parameters (see {@link SOQLBuilder#bind})
   * @param selectFields comma-separated SOQL field names, in result row order
   */
  public Enumerable<Object[]> query(DataContext root, String soqlTemplate,
      String selectFields) {
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (String name : selectFields.split(",")) {
      RelDataTypeField field = rowType.getField(name, true, false);
      if (field == null) {
        throw new IllegalStateException(
            "Field " + name + " is not a column of " + sObjectType);
      }
      fields.add(field);
    }
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new SalesforceEnumerator(schema.getConnection(),
            SOQLBuilder.bind(soqlTemplate, root), fields);
      }
    };
  }
}
