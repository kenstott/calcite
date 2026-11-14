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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Shared streaming reader for the PatentsView pre-grant publication bulk files (ODP product
 * PVPGPUBDIS, {@code pg_*.tsv.zip}). Subclasses name the file and map one TSV record to one row;
 * returning {@code null} from {@link #toRow} skips the record (used for the per-year filter).
 */
public abstract class AbstractPgpubTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPgpubTransformer.class);

  /** ODP bulk-download base for the pre-grant publication tables (product PVPGPUBDIS). */
  protected static final String ODP_PVPGPUBDIS_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/PVPGPUBDIS/";

  /** Source file name without extension, e.g. {@code pg_published_application}. */
  protected abstract String sourceName();

  /** Maps one record to a row, or returns null to skip it. */
  protected abstract Map<String, Object> toRow(String[] parts, Map<String, Integer> hdr,
      RequestContext context);

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String name = sourceName();
    final String file = downloadAndCacheTsv(ODP_PVPGPUBDIS_BASE + name + ".tsv.zip",
        cacheFile(name + "_" + quarterToken(context) + ".tsv"));
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(file), StandardCharsets.UTF_8));
    String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      reader.close();
      return Collections.emptyIterator();
    }
    final Map<String, Integer> hdr = buildHeaderMap(splitTsv(headerLine));
    final int[] count = {0};

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      { advance(); }

      private void advance() {
        pending = null;
        try {
          String line;
          while ((line = CsvRecordReader.readRecord(reader)) != null) {
            if (line.isEmpty()) {
              continue;
            }
            Map<String, Object> row = toRow(splitTsv(line), hdr, context);
            if (row != null) {
              count[0]++;
              pending = row;
              return;
            }
          }
          reader.close();
          LOGGER.info("{}: {} records", name, count[0]);
        } catch (IOException e) {
          try {
            reader.close();
          } catch (IOException closeEx) {
            LOGGER.debug("close failed", closeEx);
          }
          throw new UncheckedIOException(name + " read failed", e);
        }
      }

      @Override public boolean hasNext() {
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }
}
