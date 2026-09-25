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
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The entity bridge's source registry and ref-schema.yaml's canonical tables must agree: every
 * registered source needs its column and its _confidence sibling on the canonical table, in both
 * the row-type list and the materialize list. Also exercises the name-parsing expressions the law
 * schema's mixed and person-name sources use.
 */
@Tag("unit")
class EntityBridgeLawSourcesTest {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> refSchema() throws Exception {
    try (InputStream in = EntityBridgeLawSourcesTest.class
        .getResourceAsStream("/ref/ref-schema.yaml")) {
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(100000);
      return new Yaml(new SafeConstructor(options)).load(in);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> table(Map<String, Object> schema, String name) {
    for (Object t : (List<Object>) schema.get("partitionedTables")) {
      Map<String, Object> table = (Map<String, Object>) t;
      if (name.equals(table.get("name"))) {
        return table;
      }
    }
    throw new AssertionError("no table " + name);
  }

  @SuppressWarnings("unchecked")
  private static Set<String> names(List<Object> columns) {
    Set<String> out = new HashSet<String>();
    for (Object c : columns) {
      out.add((String) ((Map<String, Object>) c).get("name"));
    }
    return out;
  }

  /** The canonicalColumn of every entry in a private registry list. */
  private static List<String> canonicalColumns(String registryField) throws Exception {
    Field registry = EntityBridgeListener.class.getDeclaredField(registryField);
    registry.setAccessible(true);
    List<String> out = new ArrayList<String>();
    for (Object source : (List<?>) registry.get(null)) {
      Field column = source.getClass().getDeclaredField("canonicalColumn");
      column.setAccessible(true);
      out.add((String) column.get(source));
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static void assertDeclared(String registryField, String tableName) throws Exception {
    Map<String, Object> table = table(refSchema(), tableName);
    Set<String> rowType = names((List<Object>) table.get("columns"));
    Set<String> materialized = names(
        (List<Object>) ((Map<String, Object>) table.get("materialize")).get("columns"));
    assertEquals(rowType, materialized, tableName + ": columns and materialize.columns differ");
    for (String column : canonicalColumns(registryField)) {
      assertTrue(rowType.contains(column), tableName + " is missing " + column);
      assertTrue(rowType.contains(column + "_confidence"),
          tableName + " is missing " + column + "_confidence");
    }
  }

  @Test void testEveryOrgSourceHasItsColumnsOnCanonicalOrgEntity() throws Exception {
    assertDeclared("ORG_SOURCES", "canonical_org_entity");
  }

  @Test void testEveryPersonSourceHasItsColumnsOnCanonicalPersonEntity() throws Exception {
    assertDeclared("PERSON_SOURCES", "canonical_person_entity");
  }

  private static String query(String select, String sourceRow) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT " + select + " FROM (" + sourceRow + ") s")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  private static String honoree(String name, String expression) throws Exception {
    return query(expression, "SELECT " + (name == null ? "NULL" : "'" + name.replace("'", "''") + "'")
        + " AS honoree_name");
  }

  private static String parsed(String name) throws Exception {
    String select = "CASE WHEN " + EntityBridgeListener.HONOREE_PERSON_FILTER + " THEN "
        + EntityBridgeListener.HONOREE_FIRST + " || '|' || " + EntityBridgeListener.HONOREE_LAST
        + " ELSE NULL END";
    return honoree(name, select);
  }

  private static boolean orgShaped(String name) throws Exception {
    return "true".equals(honoree(name,
        "CAST(" + EntityBridgeListener.HONOREE_ORG_FILTER + " AS VARCHAR)"))
        || "1".equals(honoree(name,
        "CAST(CAST(" + EntityBridgeListener.HONOREE_ORG_FILTER + " AS INTEGER) AS VARCHAR)"));
  }

  @Test void testHonoreeTitlesPartyTagsAndSuffixesAreStripped() throws Exception {
    assertEquals("Kevin|Lincoln", parsed("Kevin Lincoln"));
    assertEquals("Ami|Bera", parsed("Bera, Ami"));
    assertEquals("Shelley|Capito", parsed("Sen Shelley Moore Capito"));
    assertEquals("Joe|Morelle", parsed("Cong. Joe Morelle"));
    assertEquals("Kim|Shrier", parsed("The Honorable Kim Shrier"));
    assertEquals("April|Delaney", parsed("U.S. Representative April McClain Delaney"));
    assertEquals("David|Schweikert", parsed("Rep. David Schweikert (R-AZ)"));
    assertEquals("Martin|Heinrich", parsed("Sen. Martin Heinrich (D-NM)"));
    assertEquals("John|Smith", parsed("John Smith, Jr."));
  }

  @Test void testHonoreeMiddleNameIsWhatIsLeftBetweenFirstAndLast() throws Exception {
    assertEquals("Moore", honoree("Sen Shelley Moore Capito", EntityBridgeListener.HONOREE_MIDDLE));
    // No middle name gives NULL, not an empty string; stagePersonSource wraps it in COALESCE(.., '').
    assertEquals(null, honoree("Bera, Ami", EntityBridgeListener.HONOREE_MIDDLE));
  }

  @Test void testCommitteesAndCampaignsAreNotPeople() throws Exception {
    for (String name : new String[] {"Hung Cao for Virginia", "SkinPAC", "Susan Collins PAC",
        "BARRETT BRIGADE VICTORY FUND", "Re Elect McGovern Committee", "Democratic Party of Georgia"}) {
      assertTrue(orgShaped(name), name + " should be committee-shaped");
      assertEquals(null, parsed(name), name + " should not parse as a person");
    }
  }

  @Test void testNamesThatOnlyLookLikeMarkersStayPeople() throws Exception {
    assertEquals("Isaac|Newton", parsed("Isaac Newton"));
    assertEquals("John|Forbes", parsed("John Forbes"));
  }

  @Test void testABareSurnameCannotBeMatchedSoItIsExcluded() throws Exception {
    assertEquals(null, parsed("Sen. Boozman"));
    assertEquals(null, parsed("Allred"));
  }

  private static String justice(String name, String expression) throws Exception {
    return query(expression, "SELECT '" + name + "' AS opinion_writer");
  }

  @Test void testJusticeNamesDropTheSuffixAndSplitIntoParts() throws Exception {
    assertEquals("John", justice("John G. Roberts, Jr.", EntityBridgeListener.JUSTICE_FIRST));
    assertEquals("G.", justice("John G. Roberts, Jr.", EntityBridgeListener.JUSTICE_MIDDLE));
    assertEquals("Roberts", justice("John G. Roberts, Jr.", EntityBridgeListener.JUSTICE_LAST));
    assertEquals("Elena", justice("Elena Kagan", EntityBridgeListener.JUSTICE_FIRST));
    assertEquals("Kagan", justice("Elena Kagan", EntityBridgeListener.JUSTICE_LAST));
    assertEquals("Bader", justice("Ruth Bader Ginsburg", EntityBridgeListener.JUSTICE_MIDDLE));
  }
}
