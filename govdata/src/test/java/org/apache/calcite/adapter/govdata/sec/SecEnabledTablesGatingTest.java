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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for kenstott/govdata-ops#235: before {@code enabledTables} was threaded into
 * {@link XbrlToParquetConverter}, {@code SecSchemaFactory}'s {@code isEnabled("*") -> false} hook
 * meant a caller's {@code --tables} scope was a complete no-op for sec — every one of the ~10
 * disaggregated document tables got written regardless of what was asked for.
 *
 * <p>Uses the same embedded minimal XBRL fixture as {@link FilingMetadataDqTest}, so a normal
 * (unrestricted) conversion is known to produce facts/metadata/contexts/mda/relationships output
 * files (risk_factor_sections is 10-K-only content-dependent and not asserted on either side).
 */
class SecEnabledTablesGatingTest {

  @TempDir
  File tempDir;

  private static final String CIK = "0000320193";
  private static final String ACCESSION = "0000320193-23-000106";

  private List<String> convert(Set<String> enabledTables) throws Exception {
    java.net.URL resourceUrl = getClass().getClassLoader().getResource("aapl-10k-minimal.xml");
    assertTrue(resourceUrl != null, "Test resource aapl-10k-minimal.xml must exist");
    File docFile = new File(resourceUrl.toURI());

    File outputDir = new File(tempDir, "parquet-" + (enabledTables == null ? "all" : String.join("_", enabledTables)));
    outputDir.mkdirs();

    StorageProvider storage = new LocalFileStorageProvider();
    XbrlToParquetConverter converter = new XbrlToParquetConverter(storage, false, enabledTables);

    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    metadata.setHint("cik", CIK);
    metadata.setHint("form", "10-K");
    metadata.setHint("filingDate", "2023-11-03");
    metadata.setHint("accession", ACCESSION);

    return converter.convert(docFile.getAbsolutePath(), outputDir.getAbsolutePath(), metadata);
  }

  private static boolean anyEndsWith(List<String> paths, String suffix) {
    for (String p : paths) {
      if (p.endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  @Test
  @Tag("integration")
  void unrestrictedConversionWritesEveryTable() throws Exception {
    List<String> outputFiles = convert(null);

    assertTrue(anyEndsWith(outputFiles, "_facts.parquet"), "unrestricted: facts must be written");
    assertTrue(anyEndsWith(outputFiles, "_metadata.parquet"), "unrestricted: metadata must be written");
    assertTrue(anyEndsWith(outputFiles, "_contexts.parquet"), "unrestricted: contexts must be written");
    assertTrue(anyEndsWith(outputFiles, "_mda.parquet"), "unrestricted: mda must be written");
    assertTrue(anyEndsWith(outputFiles, "_relationships.parquet"),
        "unrestricted: relationships must be written");
  }

  @Test
  @Tag("integration")
  void scopedToFilingMetadataOnlyWritesOnlyThatTable() throws Exception {
    // The exact scenario #235 was filed over: a remediation job scoped with
    // --tables filing_metadata must touch only filing_metadata.
    List<String> outputFiles = convert(new HashSet<>(Collections.singletonList("filing_metadata")));

    assertTrue(anyEndsWith(outputFiles, "_metadata.parquet"),
        "scoped to filing_metadata: metadata must still be written");
    assertFalse(anyEndsWith(outputFiles, "_facts.parquet"),
        "scoped to filing_metadata: facts (financial_line_items) must NOT be written");
    assertFalse(anyEndsWith(outputFiles, "_contexts.parquet"),
        "scoped to filing_metadata: contexts (filing_contexts) must NOT be written");
    assertFalse(anyEndsWith(outputFiles, "_mda.parquet"),
        "scoped to filing_metadata: mda (mda_sections) must NOT be written");
    assertFalse(anyEndsWith(outputFiles, "_relationships.parquet"),
        "scoped to filing_metadata: relationships (xbrl_relationships) must NOT be written");

    // No file on disk for a gated table either — not just absent from the returned list.
    for (String path : outputFiles) {
      assertTrue(new File(path).exists(), "every path this run DID report must actually exist: " + path);
    }
  }

  @Test
  @Tag("integration")
  void scopedToFinancialLineItemsOnlyExcludesMetadata() throws Exception {
    List<String> outputFiles = convert(new HashSet<>(Collections.singletonList("financial_line_items")));

    assertTrue(anyEndsWith(outputFiles, "_facts.parquet"),
        "scoped to financial_line_items: facts must be written");
    assertFalse(anyEndsWith(outputFiles, "_metadata.parquet"),
        "scoped to financial_line_items: metadata must NOT be written");
    assertFalse(anyEndsWith(outputFiles, "_contexts.parquet"),
        "scoped to financial_line_items: contexts must NOT be written");
  }

  @Test
  @Tag("integration")
  void emptyEnabledTablesIsTreatedAsUnrestricted() throws Exception {
    // An empty (not null) set must behave like unrestricted — matches SecSchemaFactory
    // #readEnabledTables treating an empty model-declared enabledTables list the same way.
    List<String> outputFiles = convert(new HashSet<String>());

    assertTrue(anyEndsWith(outputFiles, "_facts.parquet"), "empty set: facts must be written");
    assertTrue(anyEndsWith(outputFiles, "_metadata.parquet"), "empty set: metadata must be written");
  }

  /** Captures log events from a named logger while active, for asserting on messages a test
   * can't observe through outputFiles alone (e.g. that extraction was skipped, not just its
   * output left unreported). */
  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    CapturingAppender() {
      super("test-capture-" + java.util.UUID.randomUUID(), null, null, false, Property.EMPTY_ARRAY);
    }

    @Override public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    boolean anyContains(String substring) {
      return messages.stream().anyMatch(m -> m.contains(substring));
    }
  }

  @Test
  @Tag("integration")
  void scopedRunSkipsExtractionNotJustTheOutputListForDisabledTables() throws Exception {
    // Regression for kenstott/govdata-ops#243: enabledTables gating originally only withheld a
    // disabled table's output path from outputFiles — the underlying extraction (XBRL DOM walk,
    // HTML chunking, linkbase XML parsing) ran unconditionally regardless. Real work with real
    // cost (writeRelationshipsToParquet does live XSD downloads per relationship, logged here)
    // happened for every table on every run, whether or not that table was ever going to be
    // used. Proving this via log capture, not outputFiles, since a skipped extraction leaves no
    // output file either way — the distinguishing signal is whether the extraction ITSELF ran.
    Logger xbrlLogger =
        (Logger) LoggerContext.getContext(false).getLogger(XbrlToParquetConverter.class);
    CapturingAppender appender = new CapturingAppender();
    appender.start();
    xbrlLogger.addAppender(appender);
    xbrlLogger.setLevel(Level.DEBUG);
    try {
      convert(new HashSet<>(Collections.singletonList("filing_metadata")));
    } finally {
      xbrlLogger.removeAppender(appender);
      appender.stop();
    }

    assertFalse(appender.anyContains("linkbase relationships"),
        "xbrl_relationships extraction must not run at all when scoped out, not just be "
            + "unreported: " + appender.messages);
    assertTrue(appender.anyContains("Skipping financial_line_items extraction"),
        "the skip itself must be logged for the disabled table: " + appender.messages);
  }
}
