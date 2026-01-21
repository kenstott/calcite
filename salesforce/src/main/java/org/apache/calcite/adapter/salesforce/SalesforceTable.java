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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Table based on a Salesforce sObject.
 */
public class SalesforceTable extends AbstractTable implements TranslatableTable {

  private final SalesforceSchema schema;
  private final String sObjectType;
  private RelDataType rowType;

  public SalesforceTable(SalesforceSchema schema, String sObjectType) {
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

    // Always include Id field first
    fields.add(
        new RelDataTypeFieldImpl("Id", 0,
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 18)));

    int index = 1;
    for (SalesforceConnection.FieldDescription field : description.fields) {
      if ("Id".equals(field.name)) {
        continue; // Already added
      }

      RelDataType fieldType = convertFieldType(typeFactory, field);
      fields.add(new RelDataTypeFieldImpl(field.name, index++, fieldType));
    }

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

    // Make nullable if field allows nulls
    if (field.nillable) {
      return typeFactory.createTypeWithNullability(baseType, true);
    }
    return baseType;
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
   */
  public Enumerable<Object[]> query(String soql) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new SalesforceEnumerator(schema.getConnection(), soql, getRowType(null));
      }
    };
  }
}
