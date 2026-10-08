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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test void onlyTheIdentityCheckUsesTheReservedProbeConnection() {
    assertEquals(PgwireGovDataConnector.PROBE_APPLICATION_NAME,
        PgwireGovDataConnector.connectionProperties(true).getProperty("ApplicationName"));
    // The connection that runs queries must not ask for it: a user scan there would hold the
    // connection the identity check of every other process is waiting on
    assertNull(PgwireGovDataConnector.connectionProperties(false).getProperty("ApplicationName"));
  }

  @Test void bothConnectionsShareTheirOtherSettings() {
    Properties probe = PgwireGovDataConnector.connectionProperties(true);
    probe.remove("ApplicationName");
    assertEquals(PgwireGovDataConnector.connectionProperties(false), probe);
  }

  @Test void probeNameIsTheOnePgwireCalciteReserves() throws IOException {
    Path backend = Paths.get("..", "pgwire-calcite", "src", "pgwire_calcite", "backend.py");
    String source = new String(Files.readAllBytes(backend), StandardCharsets.UTF_8);
    Matcher m = Pattern.compile("PROBE_APPLICATION_NAME = \"([^\"]+)\"").matcher(source);
    assertTrue(m.find(), "PROBE_APPLICATION_NAME is assigned in " + backend);
    assertEquals(m.group(1), PgwireGovDataConnector.PROBE_APPLICATION_NAME);
  }
}
