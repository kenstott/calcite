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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * ORDER BY / OFFSET / FETCH reach a cloud-ops table through the planner, and the rows that
 * come back are the ones SQL asks for.
 */
@Tag("unit")
public class SortLimitPushdownTest {

  private Connection connection;
  private RecordingTable table;

  @BeforeEach public void setUp() throws SQLException {
    CloudOpsConfig config =
        new CloudOpsConfig(Arrays.asList("azure", "aws"),
            new CloudOpsConfig.AzureConfig("tenant", "client", "secret",
                Collections.singletonList("sub-1")),
            null,
            new CloudOpsConfig.AWSConfig(Collections.singletonList("111111111111"),
                "us-east-1", "key", "secret", null),
            false, 5, false);
    table = new RecordingTable(config);
    connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    connection.unwrap(CalciteConnection.class).getRootSchema()
        .add("cloud", new AbstractSchema() {
          @Override protected Map<String, Table> getTableMap() {
            return Collections.<String, Table>singletonMap("things", table);
          }
        });
  }

  @AfterEach public void tearDown() throws SQLException {
    connection.close();
  }

  private List<String> query(String sql) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      int columns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
          row.append(i == 1 ? "" : "|").append(resultSet.getObject(i));
        }
        rows.add(row.toString());
      }
    }
    return rows;
  }

  @Test public void orderByLimitOffsetReachesTheTable() throws SQLException {
    // Descending puts the null size first: d, f, c, e, b, a
    List<String> rows =
        query("select * from cloud.things order by size desc limit 2 offset 1");

    assertThat(rows, is(Arrays.asList("aws|f|50|us", "aws|c|40|us")));
    assertThat(table.sortedScans, is(1));
    assertThat(table.lastCollation, is(notNullValue()));
    assertThat(table.lastCollation.getFieldCollations().size(), is(1));
    RelFieldCollation field = table.lastCollation.getFieldCollations().get(0);
    assertThat(field.getFieldIndex(), is(2));
    assertThat(field.getDirection(), is(RelFieldCollation.Direction.DESCENDING));
    assertThat(RexLiteral.intValue(table.lastOffset), is(1));
    assertThat(RexLiteral.intValue(table.lastFetch), is(2));
    // A sorted query needs every row of every provider: no provider may truncate
    assertThat(table.providerPaginations.size(), is(2));
    for (CloudOpsPaginationHandler pagination : table.providerPaginations) {
      assertThat(pagination.hasPagination(), is(false));
    }
    for (CloudOpsSortHandler sort : table.providerSorts) {
      assertThat(sort.hasSort(), is(false));
    }
  }

  @Test public void nullsFollowTheRequestedDirection() throws SQLException {
    assertThat(query("select name from cloud.things order by size nulls first, name"),
        is(Arrays.asList("d", "a", "b", "e", "c", "f")));
    assertThat(query("select name from cloud.things order by size desc nulls last, name desc"),
        is(Arrays.asList("f", "c", "e", "b", "a", "d")));
    assertThat(table.sortedScans, is(2));
  }

  @Test public void sortOnAColumnThatIsNotSelected() throws SQLException {
    List<String> rows = query("select name from cloud.things order by size desc limit 3");

    assertThat(rows, is(Arrays.asList("d", "f", "c")));
    assertThat(table.sortedScans, is(1));
    assertThat(table.lastCollation.getFieldCollations().get(0).getFieldIndex(), is(2));
  }

  @Test public void limitAloneCapsEachProviderAndSkipsTheOffsetOnce() throws SQLException {
    List<String> rows = query("select name from cloud.things limit 2 offset 3");

    // Providers answer in a fixed order here (azure a, d, e; aws b, c, f), and each was
    // asked for no more than offset + fetch rows, never for an offset
    assertThat(rows.size(), is(2));
    assertThat(table.sortedScans, is(1));
    assertThat(table.lastCollation.getFieldCollations().isEmpty(), is(true));
    assertThat(table.providerPaginations.size(), is(2));
    for (CloudOpsPaginationHandler pagination : table.providerPaginations) {
      assertThat(pagination.hasPagination(), is(true));
      assertThat(pagination.getOffset(), is(0L));
      assertThat(pagination.getLimit(), is(5L));
    }
    // Skipped once: 6 rows, offset 3, fetch 2 leaves rows 4 and 5 of the combined list
    List<String> all = query("select name from cloud.things");
    assertThat(all.size(), is(6));
    assertThat(all.containsAll(rows), is(true));
  }

  @Test public void offsetWithoutFetchKeepsTheRest() throws SQLException {
    List<String> rows = query("select name from cloud.things order by name offset 4");

    assertThat(rows, is(Arrays.asList("e", "f")));
    assertThat(table.lastFetch, is(nullValue()));
    assertThat(RexLiteral.intValue(table.lastOffset), is(4));
  }

  @Test public void orderByWithWhereIsNotPushedAndIsCorrect() throws SQLException {
    List<String> rows =
        query("select name, size from cloud.things where region = 'us' "
            + "order by size desc limit 2");

    assertThat(rows, is(Arrays.asList("f|50", "c|40")));
    // The limit must come after the filter, which the engine applies: no sorted scan
    assertThat(table.sortedScans, is(0));
    assertThat(table.plainScans, is(1));
  }

  @Test public void limitOverAFilteredSubqueryIsCorrect() throws SQLException {
    List<String> rows =
        query("select name from (select * from cloud.things order by size desc limit 3) "
            + "where region = 'us'");

    // Top three by size descending are d (null), f, c; of those, f and c are in 'us'
    assertThat(rows, is(Arrays.asList("f", "c")));
    assertThat(table.sortedScans, is(1));
  }

  @Test public void plainSelectWhereAndProjectionStillWork() throws SQLException {
    assertThat(query("select * from cloud.things").size(), is(6));
    assertThat(table.sortedScans, is(0));

    List<String> filtered = query("select name from cloud.things where size > 25");
    Collections.sort(filtered);
    assertThat(filtered, is(Arrays.asList("c", "e", "f")));

    List<String> provider =
        query("select name, region from cloud.things where cloud_provider = 'aws'");
    Collections.sort(provider);
    assertThat(provider, is(Arrays.asList("b|us", "c|us", "f|us")));
    assertThat(table.sortedScans, is(0));

    assertThat(query("select count(*) from cloud.things"), is(Collections.singletonList("6")));
  }

  @Test public void boundLimitIsLeftToTheEngine() throws SQLException {
    try (PreparedStatement statement =
             connection.prepareStatement("select name from cloud.things order by name limit ?")) {
      statement.setInt(1, 2);
      try (ResultSet resultSet = statement.executeQuery()) {
        List<String> rows = new ArrayList<>();
        while (resultSet.next()) {
          rows.add(resultSet.getString(1));
        }
        assertThat(rows, is(Arrays.asList("a", "b")));
      }
    }
    // The engine splits the bound limit off and applies it itself; only the sort is pushed
    assertThat(table.sortedScans, is(1));
    assertThat(table.lastCollation.getFieldCollations().get(0).getFieldIndex(), is(1));
    assertThat(table.lastOffset, is(nullValue()));
    assertThat(table.lastFetch, is(nullValue()));
  }

  /** Six fixed rows from two providers; records what each scan and provider call was given. */
  private static class RecordingTable extends AbstractCloudOpsTable {
    int sortedScans;
    int plainScans;
    RelCollation lastCollation;
    RexNode lastOffset;
    RexNode lastFetch;
    final List<CloudOpsPaginationHandler> providerPaginations = new ArrayList<>();
    final List<CloudOpsSortHandler> providerSorts = new ArrayList<>();

    RecordingTable(CloudOpsConfig config) {
      super(config);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("cloud_provider", SqlTypeName.VARCHAR)
          .add("name", SqlTypeName.VARCHAR).nullable(true)
          .add("size", SqlTypeName.INTEGER).nullable(true)
          .add("region", SqlTypeName.VARCHAR).nullable(true)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
        int[] projects) {
      plainScans++;
      return super.scan(root, filters, projects);
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
        int[] projects, RelCollation collation, RexNode offset, RexNode fetch) {
      if (collation != null || offset != null || fetch != null) {
        sortedScans++;
        lastCollation = collation;
        lastOffset = offset;
        lastFetch = fetch;
      }
      return super.scan(root, filters, projects, collation, offset, fetch);
    }

    private synchronized List<Object[]> record(CloudOpsSortHandler sortHandler,
        CloudOpsPaginationHandler paginationHandler, Object[]... rows) {
      providerSorts.add(sortHandler);
      providerPaginations.add(paginationHandler);
      // Behave as the real providers do: honour a row cap when one is given
      return new ArrayList<>(paginationHandler.applyClientSidePagination(Arrays.asList(rows)));
    }

    @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
        CloudOpsProjectionHandler projectionHandler, CloudOpsSortHandler sortHandler,
        CloudOpsPaginationHandler paginationHandler, CloudOpsFilterHandler filterHandler) {
      return record(sortHandler, paginationHandler,
          new Object[] {"azure", "a", 10, "eu"},
          new Object[] {"azure", "d", null, "eu"},
          new Object[] {"azure", "e", 30, "eu"});
    }

    @Override protected List<Object[]> queryGCP(List<String> projectIds,
        CloudOpsProjectionHandler projectionHandler, CloudOpsSortHandler sortHandler,
        CloudOpsPaginationHandler paginationHandler, CloudOpsFilterHandler filterHandler) {
      throw new AssertionError("gcp is not configured in this test");
    }

    @Override protected List<Object[]> queryAWS(List<String> accountIds,
        CloudOpsProjectionHandler projectionHandler, CloudOpsSortHandler sortHandler,
        CloudOpsPaginationHandler paginationHandler, CloudOpsFilterHandler filterHandler) {
      return record(sortHandler, paginationHandler,
          new Object[] {"aws", "b", 20, "us"},
          new Object[] {"aws", "c", 40, "us"},
          new Object[] {"aws", "f", 50, "us"});
    }
  }
}
