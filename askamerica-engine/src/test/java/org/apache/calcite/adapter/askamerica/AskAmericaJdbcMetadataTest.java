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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * E2E JDBC metadata tests via the AskAmerica driver (jdbc:askamerica:).
 *
 * <p>These tests replicate the client's exact code path: load the driver,
 * connect via {@code jdbc:askamerica:}, call {@code DatabaseMetaData.getPrimaryKeys()}.
 * This is the test that catches issue #19 — unit tests on the govdata classpath
 * do NOT catch this because they use {@code jdbc:govdata:} directly.
 *
 * <p>Requires R2 network access. Run:
 * {@code ./gradlew :askamerica-engine:test -PincludeTags=integration}
 */
@Tag("integration")
public class AskAmericaJdbcMetadataTest {

  private static final Logger LOGGER =
      Logger.getLogger(AskAmericaJdbcMetadataTest.class.getName());

  private static boolean refAvailable = false;

  @BeforeAll
  static void loadDriver() throws Exception {
    Class.forName("org.apache.calcite.adapter.askamerica.AskAmericaDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c = DriverManager.getConnection("jdbc:askamerica:source=ref", props)) {
      refAvailable = c != null;
    } catch (Exception e) {
      LOGGER.warning("R2 not reachable via jdbc:askamerica:source=ref — " + e.getMessage());
    }
    assumeTrue(refAvailable, "R2 not reachable via jdbc:askamerica:source=ref — skipping");
  }

  /**
   * Issue #19 regression: getPrimaryKeys() must return non-empty results for a table
   * whose PK is declared in the schema YAML, when connecting via jdbc:askamerica:.
   *
   * <p>ref-schema.yaml declares: sec_company_tickers.primaryKey = [type, cik]
   */
  @Test void getPrimaryKeys_secCompanyTickers_returnsExpectedColumns() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:askamerica:source=ref", props)) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, "ref", "sec_company_tickers")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
        }
      }
      LOGGER.info("getPrimaryKeys(ref.sec_company_tickers) via jdbc:askamerica: => " + pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys(ref.sec_company_tickers) returned no rows via jdbc:askamerica:. "
          + "Issue #19 regression: IcebergTable PK constraint not surfaced through JDBC metadata. "
          + "primaryKey=[type,cik] is declared in ref-schema.yaml.");
      assertTrue(pks.contains("cik"),
          "ref.sec_company_tickers PK must include 'cik'; got: " + pks);
    }
  }

  /**
   * Issue #19 regression: getPrimaryKeys() for gleif_entities (IcebergTable, multi-column PK).
   *
   * <p>ref-schema.yaml declares: gleif_entities.primaryKey = [type, lei]
   */
  @Test void getPrimaryKeys_gleifEntities_returnsExpectedColumns() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:askamerica:source=ref", props)) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, "ref", "gleif_entities")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
        }
      }
      LOGGER.info("getPrimaryKeys(ref.gleif_entities) via jdbc:askamerica: => " + pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys(ref.gleif_entities) returned no rows via jdbc:askamerica:. "
          + "Issue #19 regression — primaryKey=[type,lei] declared in ref-schema.yaml.");
      assertTrue(pks.contains("lei"),
          "ref.gleif_entities PK must include 'lei'; got: " + pks);
    }
  }
}
