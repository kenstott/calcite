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

import java.sql.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link ChunkOrganizer}'s sec.vectorized_chunks -> ref.vectorized_chunks backfill
 * row reshaping (the pure transform, independent of any live Iceberg/DuckDB state).
 */
@Tag("unit")
public class ChunkOrganizerTest {

  @Test public void testTransformSecChunkRowMapsAllFields() {
    Map<String, Object> sourceRow = new LinkedHashMap<String, Object>();
    sourceRow.put("cik", "0000320193");
    sourceRow.put("accession_number", "0000320193-24-000123");
    sourceRow.put("year", 2024);
    sourceRow.put("chunk_id", "sec:0000320193:0000320193-24-000123:mda_paragraph:3");
    sourceRow.put("source_type", "mda_paragraph");
    sourceRow.put("section", "Item 7");
    sourceRow.put("subsection", "Results of Operations");
    sourceRow.put("section_path", "Item 7 > Results of Operations");
    sourceRow.put("paragraph_continuation", false);
    sourceRow.put("sequence", 3);
    sourceRow.put("filing_date", Date.valueOf("2024-11-01"));
    sourceRow.put("chunk_text", "Revenue increased 5%.");
    sourceRow.put("enriched_text", "Revenue increased 5% [FY2024].");
    sourceRow.put("content_type", "paragraph");
    sourceRow.put("financial_concepts", "Revenue");
    sourceRow.put("exhibit_number", null);
    sourceRow.put("speaker_name", null);
    sourceRow.put("speaker_role", null);
    sourceRow.put("paragraph_number", 12);

    Map<String, Object> out = ChunkOrganizer.transformSecChunkRow(sourceRow);

    assertEquals("sec:0000320193:0000320193-24-000123:mda_paragraph:3", out.get("chunk_id"));
    assertEquals("sec", out.get("source_schema"));
    assertEquals("vectorized_chunks", out.get("source_table"));
    assertEquals("0000320193:0000320193-24-000123", out.get("stringified_fk"));
    assertEquals(Long.valueOf(3), out.get("sequence"));
    assertEquals("mda_paragraph", out.get("source_type"));
    assertEquals("Revenue increased 5%.", out.get("chunk_text"));
    assertEquals("Revenue increased 5% [FY2024].", out.get("enriched_text"));
    assertEquals("Item 7", out.get("section"));
    assertEquals("Results of Operations", out.get("subsection"));
    assertEquals("Item 7 > Results of Operations", out.get("section_path"));
    assertEquals(false, out.get("paragraph_continuation"));
    assertEquals(Long.valueOf(12), out.get("paragraph_number"));
    assertEquals("paragraph", out.get("content_type"));
    assertEquals("Revenue", out.get("financial_concepts"));
    assertNull(out.get("exhibit_number"));
    assertNull(out.get("speaker_name"));
    assertNull(out.get("speaker_role"));
    assertEquals("0000320193", out.get("cik"));
    assertEquals("0000320193-24-000123", out.get("accession_number"));
    assertEquals("2024-11-01", out.get("filing_date"));
    // year is source-only (used for iceberg_scan partition pruning); not a ref.vectorized_chunks
    // column, must not leak into the transformed row.
    assertNull(out.get("year"));
  }

  @Test public void testTransformSecChunkRowNullFilingDate() {
    Map<String, Object> sourceRow = new LinkedHashMap<String, Object>();
    sourceRow.put("cik", "123");
    sourceRow.put("accession_number", "abc");
    sourceRow.put("chunk_id", "x");
    sourceRow.put("filing_date", null);

    Map<String, Object> out = ChunkOrganizer.transformSecChunkRow(sourceRow);

    assertNull(out.get("filing_date"));
    assertEquals("123:abc", out.get("stringified_fk"));
  }

  /**
   * {@code ResultSet#getObject} doesn't guarantee a consistent {@code Number} subtype across
   * calls for a given column, and {@code sequence}/{@code paragraph_number} are declared
   * {@code bigint} in both ref-schema.yaml and sec-schema.yaml. Simulates a narrower subtype
   * (e.g. {@code Integer}) coming back from the driver and asserts the transform still
   * normalizes to {@code Long}.
   */
  @Test public void testTransformSecChunkRowNormalizesIntSequenceAndParagraphNumberToLong() {
    Map<String, Object> sourceRow = new LinkedHashMap<String, Object>();
    sourceRow.put("cik", "0000320193");
    sourceRow.put("accession_number", "0000320193-24-000123");
    sourceRow.put("chunk_id", "sec:0000320193:0000320193-24-000123:mda_paragraph:3");
    sourceRow.put("sequence", 3);
    sourceRow.put("paragraph_number", 12);

    Map<String, Object> out = ChunkOrganizer.transformSecChunkRow(sourceRow);

    assertEquals(Long.valueOf(3), out.get("sequence"));
    assertEquals(Long.valueOf(12), out.get("paragraph_number"));
  }
}
