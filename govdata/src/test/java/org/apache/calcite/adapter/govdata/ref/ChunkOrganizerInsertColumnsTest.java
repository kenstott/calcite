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
package org.apache.calcite.adapter.govdata.ref;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FK columns {@code ensureVcSchema} adds to {@code vc_staging} for every registered source are
 * only useful if {@code insertParentRows} also writes them. Loading the class also runs the
 * Postgres identifier-length check over every generated FK column name.
 */
@Tag("unit")
public class ChunkOrganizerInsertColumnsTest {

  @SuppressWarnings("unchecked")
  private static List<String> insertColumns() throws Exception {
    Field f = ChunkOrganizer.class.getDeclaredField("VC_STAGING_INSERT_COLUMNS");
    f.setAccessible(true);
    return (List<String>) f.get(null);
  }

  @Test void insertWritesTheGeneratedForeignKeyColumns() throws Exception {
    List<String> cols = insertColumns();
    assertTrue(cols.contains("ref_naics_vintage_naics_code"),
        "a source with no legacy wide column must still get its generated FK column written: "
        + cols);
    assertTrue(cols.contains("ref_naics_code"), "legacy wide FK column must remain: " + cols);
  }

  /** A key component containing ':' must not shift later components into the wrong FK column. */
  @Test void fkColumnsComeFromTheSourceRowNotFromSplittingTheStringifiedKey() {
    Map<String, Object> sourceRow = new LinkedHashMap<String, Object>();
    sourceRow.put("doc_id", "urn:a:b");
    sourceRow.put("part", 7);
    Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
    ChunkOrganizer.putFkColumns(chunkRow, Arrays.asList("s_t_doc_id", "s_t_part"),
        Arrays.asList("doc_id", "part"), sourceRow);
    assertEquals("urn:a:b", chunkRow.get("s_t_doc_id"));
    assertEquals("7", chunkRow.get("s_t_part"));
  }

  @Test void aNullKeyComponentStaysNullInItsForeignKeyColumn() {
    Map<String, Object> sourceRow = new LinkedHashMap<String, Object>();
    sourceRow.put("doc_id", "d1");
    sourceRow.put("part", null);
    Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
    ChunkOrganizer.putFkColumns(chunkRow, Arrays.asList("s_t_doc_id", "s_t_part"),
        Arrays.asList("doc_id", "part"), sourceRow);
    assertEquals("d1", chunkRow.get("s_t_doc_id"));
    assertTrue(chunkRow.containsKey("s_t_part") && chunkRow.get("s_t_part") == null);
  }
}
