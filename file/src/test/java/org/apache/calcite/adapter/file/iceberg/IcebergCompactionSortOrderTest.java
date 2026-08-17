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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.etl.MaterializeConfig;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the {@code sortOrder} compaction option.
 *
 * <p>Regression context: {@code sortOrder:} was declared ten times across sec-schema.yaml and
 * read by no code at all — compaction bin-packed by size only. Files therefore carried min/max
 * ranges spanning the whole column domain, so the Parquet reader could prune nothing and an
 * equality lookup scanned every row group (measured at 177s against a 1.07M-row table). These
 * tests pin both halves: that the option is parsed off the iceberg block, and that the
 * comparator it drives actually orders rows.
 */
@Tag("unit")
public class IcebergCompactionSortOrderTest {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "name", Types.StringType.get()),
      Types.NestedField.optional(2, "year", Types.IntegerType.get()));

  private static Record row(String name, Integer year) {
    GenericRecord r = GenericRecord.create(SCHEMA);
    r.setField("name", name);
    r.setField("year", year);
    return r;
  }

  @SuppressWarnings("unchecked")
  private static Comparator<Record> comparator(List<String> sortOrder) throws Exception {
    Method m = IcebergTableWriter.class.getDeclaredMethod(
        "recordComparator", Schema.class, List.class);
    m.setAccessible(true);
    return (Comparator<Record>) m.invoke(null, SCHEMA, sortOrder);
  }

  @Test void parsesSortOrderFromTheIcebergBlock() {
    Map<String, Object> iceberg = new HashMap<>();
    iceberg.put("tableName", "filing_metadata");
    iceberg.put("sortOrder", Arrays.asList("name", "year"));

    MaterializeConfig.IcebergConfig config = MaterializeConfig.IcebergConfig.fromMap(iceberg);

    assertEquals(Arrays.asList("name", "year"), config.getSortOrder(),
        "sortOrder declared in YAML must reach the config that compaction reads");
  }

  @Test void absentSortOrderIsEmptyNotNull() {
    Map<String, Object> iceberg = new HashMap<>();
    iceberg.put("tableName", "t");

    MaterializeConfig.IcebergConfig config = MaterializeConfig.IcebergConfig.fromMap(iceberg);

    assertNotNull(config.getSortOrder());
    assertTrue(config.getSortOrder().isEmpty(),
        "no sortOrder means preserve read order, which callers test with isEmpty()");
  }

  @Test void ordersRowsByTheNamedColumns() throws Exception {
    List<Record> rows = new ArrayList<>(Arrays.asList(
        row("zebra", 2020), row("alpha", 2021), row("alpha", 2019), row("mid", 2020)));

    rows.sort(comparator(Arrays.asList("name", "year")));

    assertEquals(Arrays.asList("alpha", "alpha", "mid", "zebra"),
        namesOf(rows));
    assertEquals(2019, rows.get(0).getField("year"),
        "the second sort column must break ties within the first");
  }

  @Test void sortsNullsLast() throws Exception {
    List<Record> rows = new ArrayList<>(Arrays.asList(
        row(null, 2020), row("alpha", 2021), row("zebra", 2019)));

    rows.sort(comparator(Arrays.asList("name")));

    // Nulls last so a null key never sits at the head of a file, widening its min/max for a
    // value that cannot be predicated on anyway.
    assertEquals("alpha", rows.get(0).getField("name"));
    assertEquals("zebra", rows.get(1).getField("name"));
    assertNull(rows.get(2).getField("name"));
  }

  @Test void refusesToSortOnAnUnknownColumn() throws Exception {
    // Sorting on the subset that happens to exist would make pruning behave differently than
    // the config reads, with nothing to explain it — so the comparator is refused outright and
    // compaction falls back to an unsorted (still correct) rewrite.
    assertNull(comparator(Arrays.asList("name", "no_such_column")),
        "an unknown sortOrder column must disable sorting, not silently sort on a subset");
  }

  private static List<String> namesOf(List<Record> rows) {
    List<String> out = new ArrayList<>();
    for (Record r : rows) {
      out.add((String) r.getField("name"));
    }
    return out;
  }
}
