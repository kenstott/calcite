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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableDef;
import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableRow;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The tables that describe the instance without exposing its tables: what a steward picks from
 * when registering a table.
 *
 * <ul>
 *   <li>{@value #TABLES}{@code (name, label, theme)}: every table the instance lists, from the one
 *   read of {@code sys_db_object}; it reads no column metadata.
 *   <li>{@value #COLUMNS}{@code (table_name, column_name, data_type, ordinal_position)}: the
 *   columns one table would have once registered, read from that table's own and its ancestors'
 *   {@code sys_dictionary} rows. A scan must name the table with {@code table_name = '<name>'};
 *   without it the scan is refused, because answering it would read the dictionary of every
 *   table.
 *   <li>{@value #TRANSFORM_MAPS}{@code (name, source_table, target_table, active)}: the instance's
 *   transform maps, from {@code sys_transform_map}, for binding a registered table's INSERTs to
 *   an import set (the map's {@code source_table} is the staging table). A condition
 *   {@code target_table = '<table>'} is sent to the instance.
 * </ul>
 *
 * <p>{@code data_type} is the PostgreSQL name the pgwire server reports for the column in
 * {@code information_schema.columns} once the table is registered.
 */
final class ListingTables {

  static final String TABLES = "_servicenow_tables";
  static final String COLUMNS = "_servicenow_columns";
  static final String TRANSFORM_MAPS = "_servicenow_transform_maps";

  private ListingTables() {}

  /** {@value #TABLES}: one row per table, ordered by theme and name. */
  static final class Tables extends AbstractTable implements ScannableTable {
    private final ServiceNowSchema schema;

    Tables(ServiceNowSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("name", SqlTypeName.VARCHAR)
          .add("label", SqlTypeName.VARCHAR)
          .add("theme", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      final List<TableRow> tables = schema.catalog().tables();
      tables.sort(Comparator.comparing((TableRow t) -> t.theme).thenComparing(t -> t.name));
      final List<Object[]> rows = new ArrayList<>(tables.size());
      for (TableRow t : tables) {
        rows.add(new Object[] {t.name, t.label, t.theme});
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /** {@value #COLUMNS}: the columns of the one table a scan names. */
  static final class Columns extends AbstractTable implements FilterableTable {
    private final ServiceNowSchema schema;

    Columns(ServiceNowSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("table_name", SqlTypeName.VARCHAR)
          .add("column_name", SqlTypeName.VARCHAR)
          .add("data_type", SqlTypeName.VARCHAR)
          .add("ordinal_position", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      final String table = literal(filters, 0, false);
      if (table == null) {
        throw new ServiceNowException(COLUMNS + " lists the columns of one table and must be "
            + "queried with table_name = '<table>'; without it, it would read the column "
            + "metadata of every table on the instance");
      }
      if (!schema.catalog().hasTable(table)) {
        throw new ServiceNowException("Table '" + table + "' is not in sys_db_object (or not "
            + "readable by the integration user)");
      }
      final TableDef definition = schema.catalog().table(table);
      final List<Object[]> rows = new ArrayList<>(definition.columns.size());
      int ordinal = 0;
      for (ServiceNowColumn column : definition.columns) {
        rows.add(new Object[] {table, column.name, postgresName(column.kind.sqlType), ++ordinal});
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /** {@value #TRANSFORM_MAPS}: the instance's transform maps, optionally of one target table. */
  static final class TransformMaps extends AbstractTable implements FilterableTable {
    private final ServiceNowSchema schema;

    TransformMaps(ServiceNowSchema schema) {
      this.schema = schema;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("name", SqlTypeName.VARCHAR)
          .add("source_table", SqlTypeName.VARCHAR)
          .add("target_table", SqlTypeName.VARCHAR)
          .add("active", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      // A target table that is a valid name is sent as a query term (a metadata condition, like
      // the dictionary's per-table read); any other filter stays for Calcite
      final String target = literal(filters, 2, true);
      final List<Object[]> rows = new ArrayList<>();
      for (JsonNode map : ServiceNowCatalog.readAll(schema.connection(), "sys_transform_map",
          schema.metadataPageSize(), false, target == null ? "" : "target_table=" + target,
          "sys_id", "name", "source_table", "target_table", "active")) {
        final String mapTarget = Rows.text(map, "target_table", "sys_transform_map", false);
        if (target != null && !target.equals(mapTarget)) {
          throw new ServiceNowException("sys_transform_map returned a map of '" + mapTarget
              + "' when asked for the maps of '" + target + "' only");
        }
        rows.add(new Object[] {Rows.text(map, "name", "sys_transform_map", false),
            Rows.text(map, "source_table", "sys_transform_map", false), mapTarget,
            "true".equals(Rows.text(map, "active", "sys_transform_map", true))});
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * The literal of a {@code <column> = '<literal>'} condition among the filters, or null if there
   * is none. Calcite may pass several conditions as one AND, so each filter's conjuncts are
   * searched. The filters are left as they are for Calcite to evaluate: every row these tables
   * return for the literal satisfies the condition. With {@code nameOnly}, a literal that is not
   * a ServiceNow name is not used.
   */
  private static String literal(List<RexNode> filters, int columnIndex, boolean nameOnly) {
    for (RexNode filter : filters) {
      for (RexNode condition : RelOptUtil.conjunctions(filter)) {
        if (condition.getKind() != SqlKind.EQUALS) {
          continue;
        }
        final List<RexNode> operands = ((RexCall) condition).getOperands();
        for (int side = 0; side < 2; side++) {
          final RexNode column = operands.get(side);
          final RexNode value = operands.get(1 - side);
          if (column instanceof RexInputRef
              && ((RexInputRef) column).getIndex() == columnIndex
              && value instanceof RexLiteral) {
            final String text = ((RexLiteral) value).getValueAs(String.class);
            if (text != null
                && (!nameOnly || ServiceNowCatalog.NAME.matcher(text).matches())) {
              return text;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * The PostgreSQL type name the pgwire server reports for a column of this SQL type: its
   * {@code normalize._TYPE_TABLE} type, shown under {@code catalog._PG_TYPE_DISPLAY_NAMES}.
   */
  static String postgresName(SqlTypeName type) {
    switch (type) {
    case VARCHAR:
      return "character varying";
    case INTEGER:
      return "integer";
    case BIGINT:
      return "bigint";
    case DECIMAL:
      return "numeric";
    case DOUBLE:
      return "double precision";
    case BOOLEAN:
      return "boolean";
    case TIMESTAMP:
      return "timestamp";
    case DATE:
      return "date";
    case TIME:
      return "time";
    default:
      throw new ServiceNowException("No PostgreSQL type name for SQL type " + type);
    }
  }
}
