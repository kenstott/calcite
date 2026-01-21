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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test to verify SEC schema loading from YAML file.
 * Tests that YAML anchors/aliases are properly resolved.
 */
@Tag("unit")
public class SecSchemaYamlTest extends AbstractSecDataDownloader {

  @Test
  void testLoadFinancialLineItemsColumns() {
    List<TableColumn> columns = loadTableColumns("financial_line_items");

    assertNotNull(columns, "Columns should not be null");
    assertFalse(columns.isEmpty(), "Columns should not be empty");

    // Verify cik column from template *cik_column is resolved
    TableColumn cikColumn = findColumn(columns, "cik");
    assertNotNull(cikColumn, "cik column should exist (from template)");
    assertEquals("string", cikColumn.getType());
    assertFalse(cikColumn.isNullable());

    // Verify accession_number column from template *accession_number is resolved
    TableColumn accessionColumn = findColumn(columns, "accession_number");
    assertNotNull(accessionColumn, "accession_number column should exist (from template)");

    // Verify concept column from template *concept_column is resolved
    TableColumn conceptColumn = findColumn(columns, "concept");
    assertNotNull(conceptColumn, "concept column should exist (from template)");

    // Print columns for debugging
    System.out.println("financial_line_items columns (" + columns.size() + "):");
    for (TableColumn col : columns) {
      System.out.println("  - " + col.getName() + " (" + col.getType()
          + ", nullable=" + col.isNullable() + ")");
    }
  }

  @Test
  void testLoadVectorizedChunksColumns() {
    List<TableColumn> columns = loadTableColumns("vectorized_chunks");

    assertNotNull(columns, "Columns should not be null");
    assertFalse(columns.isEmpty(), "Columns should not be empty");

    // Verify cik column exists (from YAML template)
    TableColumn cikColumn = findColumn(columns, "cik");
    assertNotNull(cikColumn, "cik column should exist in vectorized_chunks");

    // Verify accession_number column exists (from YAML template)
    TableColumn accessionColumn = findColumn(columns, "accession_number");
    assertNotNull(accessionColumn, "accession_number column should exist in vectorized_chunks");

    // Verify chunk_id column exists
    TableColumn chunkIdColumn = findColumn(columns, "chunk_id");
    assertNotNull(chunkIdColumn, "chunk_id column should exist");

    // Verify embedding column exists and is computed
    TableColumn embeddingColumn = findColumn(columns, "embedding");
    assertNotNull(embeddingColumn, "embedding column should exist");
    assertEquals("array<double>", embeddingColumn.getType(), "embedding should be array<double>");
    assertTrue(embeddingColumn.isComputed(), "embedding should be a computed column");

    // Print columns for debugging
    System.out.println("vectorized_chunks columns (" + columns.size() + "):");
    for (TableColumn col : columns) {
      System.out.println("  - " + col.getName() + " (" + col.getType()
          + ", nullable=" + col.isNullable()
          + (col.isComputed() ? ", computed" : "") + ")");
    }
  }

  @Test
  void testLoadAllSecTables() {
    String[] tableNames = {
        "filing_metadata",
        "financial_line_items",
        "filing_contexts",
        "mda_sections",
        "xbrl_relationships",
        "insider_transactions",
        "earnings_transcripts",
        "vectorized_chunks"
    };

    for (String tableName : tableNames) {
      List<TableColumn> columns = loadTableColumns(tableName);
      assertNotNull(columns, tableName + " columns should not be null");
      assertFalse(columns.isEmpty(), tableName + " should have columns");
      System.out.println(tableName + ": " + columns.size() + " columns");
    }
  }

  @Test
  void testColumnTemplatesResolved() {
    // Test that column templates like *cik_column are properly resolved
    List<TableColumn> filingMetadata = loadTableColumns("filing_metadata");

    // cik should have the comment from the template
    TableColumn cikColumn = findColumn(filingMetadata, "cik");
    assertNotNull(cikColumn);
    assertTrue(cikColumn.getComment().contains("Central Index Key"),
        "cik column should have template comment");
  }

  private TableColumn findColumn(List<TableColumn> columns, String name) {
    for (TableColumn col : columns) {
      if (col.getName().equals(name)) {
        return col;
      }
    }
    return null;
  }
}
