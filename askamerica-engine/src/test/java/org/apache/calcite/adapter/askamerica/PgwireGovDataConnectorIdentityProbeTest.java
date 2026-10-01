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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The identity check runs on every cold start before the first tool call can answer, so it must
 * stay a schema-list lookup: reading a govdata table makes DuckDB fetch that table's Iceberg
 * manifests first (30 s for {@code sec.filing_metadata}), and a {@code pg_catalog} or
 * {@code information_schema.tables} lookup resolves tables in the server's JVM (144 s and 4 s).
 * Measured 2026-10-01.
 */
@Tag("unit")
class PgwireGovDataConnectorIdentityProbeTest {
  private static final Pattern FROM = Pattern.compile("\\b(?:from|join)\\s+([a-z0-9_.\"]+)");
  private static final Pattern QUOTED = Pattern.compile("'([a-z_]+)'");

  @Test void identityProbeReadsOnlyTheSchemaList() {
    Matcher m = FROM.matcher(PgwireGovDataConnector.IDENTITY_PROBE_SQL.toLowerCase(Locale.ROOT));
    int relations = 0;
    while (m.find()) {
      relations++;
      assertEquals("information_schema.schemata", m.group(1));
    }
    assertEquals(1, relations, PgwireGovDataConnector.IDENTITY_PROBE_SQL);
  }

  @Test void expectedCountMatchesTheSchemasNamed() {
    Set<String> named = new HashSet<>();
    Matcher m = QUOTED.matcher(PgwireGovDataConnector.IDENTITY_PROBE_SQL);
    while (m.find()) {
      named.add(m.group(1));
    }
    assertEquals(PgwireGovDataConnector.IDENTITY_PROBE_SCHEMAS, named.size(), named.toString());
  }
}
