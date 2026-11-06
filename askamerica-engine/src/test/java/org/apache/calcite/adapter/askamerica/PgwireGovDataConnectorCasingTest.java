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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** An unquoted identifier is lower case in the pgwire-govdata server, as in the embedded driver. */
@Tag("unit")
class PgwireGovDataConnectorCasingTest {

  @Test void serverIsStartedWithLowerCaseUnquotedIdentifiers() {
    List<String> args = PgwireGovDataConnector.jdbcPropArgs();
    assertTrue(args.contains("unquotedCasing=TO_LOWER"), args.toString());
    assertTrue(args.contains("caseSensitive=false"), args.toString());
    assertTrue(args.contains(
        "parserFactory=org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY"),
        args.toString());
  }

  @Test void everyPropertyIsPassedAsItsOwnJdbcPropArgument() {
    List<String> args = PgwireGovDataConnector.jdbcPropArgs();
    assertEquals(2 * PgwireGovDataConnector.JDBC_PROPS.length, args.size());
    for (int i = 0; i < args.size(); i += 2) {
      assertEquals("--jdbc-prop", args.get(i));
    }
  }

  private static String labelOfUnquotedRiver(boolean withServerProps) throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("fun", "standard,postgresql,spatial,mssql,bigquery");
    if (withServerProps) {
      for (String prop : PgwireGovDataConnector.JDBC_PROPS) {
        int eq = prop.indexOf('=');
        props.setProperty(prop.substring(0, eq), prop.substring(eq + 1));
      }
    }
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         ResultSet rs = conn.createStatement()
             .executeQuery("SELECT river, \"name\" FROM (VALUES (1, 2)) AS t(river, \"name\")")) {
      return rs.getMetaData().getColumnLabel(1) + "," + rs.getMetaData().getColumnLabel(2);
    }
  }

  @Test void unquotedIdentifierComesBackLowerWithTheServerProperties() throws Exception {
    assertEquals("river,name", labelOfUnquotedRiver(true));
  }

  @Test void withoutThePropertiesTheDefaultFoldsToUpper() throws Exception {
    assertEquals("RIVER,name", labelOfUnquotedRiver(false));
  }

  @Test void serverStateDirIsUnderTheAskAmericaHomeNotXdg() {
    File dir = PgwireGovDataConnector.serverStateDir();
    assertEquals(new File(new File(System.getProperty("user.home"), ".askamerica"),
        "pgwire-govdata-state"), dir);
    assertTrue(!dir.getPath().contains(".local"), dir.getPath());
  }
}
