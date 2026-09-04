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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streams one FIA archive CSV through unchanged, one output row per source row.
 *
 * <p>The other Fia*Transformers in this package aggregate: they collapse the archive's rows into
 * state/year/species groups. That is the wrong shape for FIA's design tables — the stratification
 * set ({@code POP_STRATUM}, {@code POP_PLOT_STRATUM_ASSGN}, {@code POP_ESTN_UNIT}, …) exists
 * precisely to let a consumer compute their own expansion factors and population estimates, and an
 * aggregate of it is useless for that. These tables are carried at their published grain.
 *
 * <p>The archive entry is derived from the table's own {@code type} dimension rather than
 * configured separately: {@code fia_pop_stratum} reads {@code <ST>_POP_STRATUM.csv}. So adding
 * another passthrough table is a schema-YAML edit with no new Java — declare the columns you want
 * and they are populated by matching the CSV header case-insensitively. Columns declared in the
 * YAML but absent from the archive come through null rather than failing the row, since FIA adds
 * and retires columns between DB versions.
 *
 * <p>Values are emitted as strings and coerced by the materialization writer against the declared
 * column types, so the YAML is the single place a type is stated.
 */
public class FiaArchiveTableTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FiaArchiveTableTransformer.class);

  /** Table-name prefix stripped to derive the archive entry — fia_pop_stratum -> POP_STRATUM. */
  private static final String TABLE_PREFIX = "fia_";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IOException("FiaArchiveTableTransformer: no 'state' dimension in " + dims);
    }
    String table = dims.get("type");
    if (table == null || table.isEmpty()) {
      throw new IOException("FiaArchiveTableTransformer: no 'type' dimension in " + dims);
    }
    final String entry = archiveEntryFor(table);
    final String stateFips = FiaLookups.stateFipsForAbbr(state);

    final FiaStateArchive.EntryHandle handle = FiaStateArchive.openEntry(state, entry);
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(handle.stream, StandardCharsets.UTF_8));
    String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      LOGGER.warn("{}: empty {}_{} in FIA archive", table, state, entry);
      reader.close();
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    final List<String> header = CsvRecordReader.splitFields(headerLine, ',');

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean done;
      private long rows;

      private void advance() {
        if (nextRow != null || done) {
          return;
        }
        try {
          String record;
          while ((record = CsvRecordReader.readRecord(reader)) != null) {
            if (record.isEmpty()) {
              continue;
            }
            List<String> cols = CsvRecordReader.splitFields(record, ',');
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("state_fips", stateFips);
            for (int i = 0; i < header.size(); i++) {
              String name = header.get(i).trim().toLowerCase(Locale.ROOT);
              if (name.isEmpty()) {
                continue;
              }
              String value = i < cols.size() ? cols.get(i).trim() : null;
              row.put(name, value == null || value.isEmpty() ? null : value);
            }
            rows++;
            nextRow = row;
            return;
          }
          done = true;
          reader.close();
          handle.close();
          LOGGER.info("{}[{}]: {} rows from {}", table, state, rows, entry);
        } catch (IOException e) {
          throw new RuntimeException(table + "[" + state + "]: streaming failed on " + entry, e);
        }
      }

      @Override public boolean hasNext() {
        advance();
        return nextRow != null;
      }

      @Override public Map<String, Object> next() {
        advance();
        if (nextRow == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = nextRow;
        nextRow = null;
        return row;
      }
    };
  }

  /** {@code fia_pop_stratum} -> {@code POP_STRATUM.csv}. */
  static String archiveEntryFor(String tableName) {
    String base = tableName.startsWith(TABLE_PREFIX)
        ? tableName.substring(TABLE_PREFIX.length()) : tableName;
    return base.toUpperCase(Locale.ROOT) + ".csv";
  }
}
