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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Tests the {@code schema} property of {@code jdbc:cloudops:} URLs. No cloud is contacted:
 * providers are only called when a table is scanned.
 */
@Tag("unit")
public class CloudOpsDriverSchemaNameTest {

  private static final String CREDENTIALS =
      "aws.accessKeyId=test-key;aws.secretAccessKey=test-secret;aws.accountIds=111111111111;"
          + "aws.region=us-east-1";

  private static Connection connect(String url) throws SQLException {
    return new CloudOpsDriver().connect(url, new Properties());
  }

  @Test public void defaultSchemaIsCloud() throws SQLException {
    try (Connection connection = connect("jdbc:cloudops:" + CREDENTIALS)) {
      assertThat(connection.getSchema(), is("cloud"));
      CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
      assertThat(calcite.getRootSchema().subSchemas().get("cloud"), is(notNullValue()));
    }
  }

  @Test public void schemaPropertyNamesTheSchema() throws SQLException {
    try (Connection connection = connect("jdbc:cloudops:schema=inventory;" + CREDENTIALS)) {
      assertThat(connection.getSchema(), is("inventory"));
      CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
      assertThat(calcite.getRootSchema().subSchemas().get("cloud"), is(nullValue()));
      Table compute = calcite.getRootSchema().subSchemas().get("inventory")
          .tables().get("compute_resources");
      assertThat(compute, is(notNullValue()));

      // Foreign keys name their tables by schema, so they follow the schema's name
      List<RelReferentialConstraint> constraints =
          compute.getStatistic().getReferentialConstraints();
      assertThat(constraints, is(not(empty())));
      for (RelReferentialConstraint constraint : constraints) {
        assertThat(constraint.getSourceQualifiedName().get(0), is("inventory"));
        assertThat(constraint.getTargetQualifiedName().get(0), is("inventory"));
      }
    }
  }

  @Test public void emptySchemaPropertyIsRejected() {
    try {
      connect("jdbc:cloudops:schema= ;" + CREDENTIALS).close();
    } catch (SQLException e) {
      assertThat(e.getMessage().contains("schema property is empty"), is(true));
      return;
    }
    throw new AssertionError("expected an empty schema name to be rejected");
  }
}
