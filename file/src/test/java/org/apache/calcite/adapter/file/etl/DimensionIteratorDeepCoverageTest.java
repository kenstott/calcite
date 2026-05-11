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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link DimensionIterator}.
 * Targets uncovered branches: expand, expandStandard, expandWithContext,
 * resolveDimension, resolveRange, resolveYearRange, resolveQuery,
 * resolveCustom, resolveCustomWithContext, resolveJsonCatalog,
 * planPartitions, expandPartition, cartesianProduct, and truncateForLog.
 */
@Tag("unit")
class DimensionIteratorDeepCoverageTest {

  // ====================================================================
  // Tests for constructors and factory methods
  // ====================================================================

  @Test
  void testDefaultConstructor() {
    DimensionIterator iterator = new DimensionIterator();
    assertNotNull(iterator);
  }

  @Test
  void testConstructorWithConnection() throws SQLException {
    Connection conn = mock(Connection.class);
    DimensionIterator iterator = new DimensionIterator(conn);
    assertNotNull(iterator);
  }

  @Test
  void testConstructorWithResolverAndStorageProvider() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(resolver, provider);
    assertNotNull(iterator);
  }

  @Test
  void testConstructorWithAllThreeArgs() throws SQLException {
    Connection conn = mock(Connection.class);
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(conn, resolver, provider);
    assertNotNull(iterator);
  }

  @Test
  void testStaticCreateNoArgs() {
    DimensionIterator iterator = DimensionIterator.create();
    assertNotNull(iterator);
  }

  @Test
  void testStaticCreateWithConnection() throws SQLException {
    Connection conn = mock(Connection.class);
    DimensionIterator iterator = DimensionIterator.create(conn);
    assertNotNull(iterator);
  }

  // ====================================================================
  // Tests for expand with null/empty dimensions
  // ====================================================================

  @Test
  void testExpandWithNull() {
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> result = iterator.expand(null);
    assertEquals(1, result.size());
    assertTrue(result.get(0).isEmpty());
  }

  @Test
  void testExpandWithEmpty() {
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> result = iterator.expand(Collections.emptyMap());
    assertEquals(1, result.size());
    assertTrue(result.get(0).isEmpty());
  }

  // ====================================================================
  // Tests for resolveDimension - RANGE
  // ====================================================================

  @Test
  void testResolveRangeBasic() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2023)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("2020", "2021", "2022", "2023"), values);
  }

  @Test
  void testResolveRangeWithStep() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2030)
        .step(5)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("2020", "2025", "2030"), values);
  }

  @Test
  void testResolveRangeWithNegativeStep() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("countdown")
        .type(DimensionType.RANGE)
        .start(5)
        .end(1)
        .step(-1)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("5", "4", "3", "2", "1"), values);
  }

  @Test
  void testResolveRangeWithZeroStep() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .step(0)
        .build();

    // step of 0 is treated as 1
    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("2020", "2021", "2022"), values);
  }

  @Test
  void testResolveRangeMissingStart() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(null)
        .end(2023)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveRangeMissingEnd() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(null)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  // ====================================================================
  // Tests for resolveDimension - LIST
  // ====================================================================

  @Test
  void testResolveList() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("NORTH", "SOUTH", "EAST", "WEST"))
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(4, values.size());
    assertEquals("NORTH", values.get(0));
  }

  @Test
  void testResolveListEmpty() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("empty")
        .type(DimensionType.LIST)
        .values(Collections.emptyList())
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  // ====================================================================
  // Tests for resolveDimension - YEAR_RANGE
  // ====================================================================

  @Test
  void testResolveYearRangeBasic() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2020)
        .end(2022)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.contains("2020"));
    assertTrue(values.contains("2021"));
    assertTrue(values.contains("2022"));
  }

  @Test
  void testResolveYearRangeWithCurrentEnd() {
    DimensionIterator iterator = new DimensionIterator();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 2)
        .end(null)  // null = current year
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.contains(String.valueOf(currentYear)));
    assertEquals(3, values.size());
  }

  @Test
  void testResolveYearRangeWithDataLag() {
    DimensionIterator iterator = new DimensionIterator();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 3)
        .end(null)
        .dataLag(1)
        .build();

    List<String> values = iterator.resolveDimension(config);
    // Should not contain currentYear due to dataLag=1
    assertFalse(values.contains(String.valueOf(currentYear)));
    assertTrue(values.contains(String.valueOf(currentYear - 1)));
  }

  @Test
  void testResolveYearRangeWithExcludeYears() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2018)
        .end(2022)
        .excludeYears(Arrays.asList(2019, 2021))
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(3, values.size());
    assertTrue(values.contains("2018"));
    assertFalse(values.contains("2019"));
    assertTrue(values.contains("2020"));
    assertFalse(values.contains("2021"));
    assertTrue(values.contains("2022"));
  }

  @Test
  void testResolveYearRangeMissingStart() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(null)
        .end(2023)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveYearRangeWithReleaseMonth() {
    DimensionIterator iterator = new DimensionIterator();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int currentMonth = Calendar.getInstance().get(Calendar.MONTH) + 1;

    // Use a release month in the future (December) so the lag year gets adjusted
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 5)
        .end(null)
        .dataLag(1)
        .releaseMonth(12) // Release in December
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertFalse(values.isEmpty());

    // If current month < 12, lag year is reduced by another year
    if (currentMonth < 12) {
      assertFalse(values.contains(String.valueOf(currentYear - 1)),
          "Should exclude currentYear-1 when before release month");
    }
  }

  @Test
  void testResolveYearRangeWithReleaseMonthInPast() {
    DimensionIterator iterator = new DimensionIterator();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int currentMonth = Calendar.getInstance().get(Calendar.MONTH) + 1;

    // Use release month of January - always in the past or current
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 3)
        .end(null)
        .dataLag(1)
        .releaseMonth(1) // Release in January
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertFalse(values.isEmpty());
  }

  @Test
  void testResolveYearRangeWithCustomStep() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2010)
        .end(2020)
        .step(5)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("2010", "2015", "2020"), values);
  }

  // ====================================================================
  // Tests for resolveDimension - QUERY
  // ====================================================================

  @Test
  void testResolveQueryWithNoConnection() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("SELECT DISTINCT region FROM regions")
        .build();

    assertThrows(IllegalStateException.class, () -> iterator.resolveDimension(config));
  }

  @Test
  void testResolveQueryWithConnection() throws SQLException {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString(1)).thenReturn("US", "CA");

    DimensionIterator iterator = new DimensionIterator(conn);
    DimensionConfig config = DimensionConfig.builder()
        .name("country")
        .type(DimensionType.QUERY)
        .sql("SELECT DISTINCT country FROM countries")
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("US", "CA"), values);
  }

  @Test
  void testResolveQueryWithNullSql() throws SQLException {
    Connection conn = mock(Connection.class);
    DimensionIterator iterator = new DimensionIterator(conn);
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql(null)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveQueryWithEmptySql() throws SQLException {
    Connection conn = mock(Connection.class);
    DimensionIterator iterator = new DimensionIterator(conn);
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("")
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveQueryHandlesSqlException() throws SQLException {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenThrow(new SQLException("Query failed"));

    DimensionIterator iterator = new DimensionIterator(conn);
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("INVALID SQL")
        .build();

    assertThrows(RuntimeException.class, () -> iterator.resolveDimension(config));
  }

  @Test
  void testResolveQuerySkipsNullValues() throws SQLException {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, true, false);
    when(rs.getString(1)).thenReturn("A", null, "B");

    DimensionIterator iterator = new DimensionIterator(conn);
    DimensionConfig config = DimensionConfig.builder()
        .name("val")
        .type(DimensionType.QUERY)
        .sql("SELECT val FROM vals")
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(Arrays.asList("A", "B"), values);
  }

  // ====================================================================
  // Tests for resolveDimension - CUSTOM
  // ====================================================================

  @Test
  void testResolveCustomWithNoResolver() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build();

    assertThrows(IllegalStateException.class, () -> iterator.resolveDimension(config));
  }

  @Test
  void testResolveCustomWithResolver() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(eq("custom_dim"), any(), any(), any()))
        .thenReturn(Arrays.asList("val1", "val2"));

    DimensionIterator iterator = new DimensionIterator(resolver, provider);
    DimensionConfig config = DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertEquals(2, values.size());
  }

  @Test
  void testResolveCustomWithResolverReturningNull() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(anyString(), any(), any(), any())).thenReturn(null);

    DimensionIterator iterator = new DimensionIterator(resolver, provider);
    DimensionConfig config = DimensionConfig.builder()
        .name("null_dim")
        .type(DimensionType.CUSTOM)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveCustomWithResolverThrowingException() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(anyString(), any(), any(), any()))
        .thenThrow(new RuntimeException("Resolver failed"));

    DimensionIterator iterator = new DimensionIterator(resolver, provider);
    DimensionConfig config = DimensionConfig.builder()
        .name("failing_dim")
        .type(DimensionType.CUSTOM)
        .build();

    assertThrows(RuntimeException.class, () -> iterator.resolveDimension(config));
  }

  // ====================================================================
  // Tests for resolveDimension - JSON_CATALOG
  // ====================================================================

  @Test
  void testResolveJsonCatalogWithNullSource() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("catalog_dim")
        .type(DimensionType.JSON_CATALOG)
        .source(null)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveJsonCatalogWithEmptySource() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("catalog_dim")
        .type(DimensionType.JSON_CATALOG)
        .source("")
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  @Test
  void testResolveJsonCatalogWithInvalidSource() {
    DimensionIterator iterator = new DimensionIterator();
    DimensionConfig config = DimensionConfig.builder()
        .name("catalog_dim")
        .type(DimensionType.JSON_CATALOG)
        .source("/nonexistent/resource.json")
        .path("items")
        .build();

    assertThrows(RuntimeException.class, () -> iterator.resolveDimension(config));
  }

  // ====================================================================
  // Tests for expand with standard dimensions (Cartesian product)
  // ====================================================================

  @Test
  void testExpandCartesianProduct() {
    DimensionIterator iterator = new DimensionIterator();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("NORTH", "SOUTH"))
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    assertEquals(4, result.size()); // 2 years x 2 regions

    // Verify all combinations exist
    boolean found2020North = false;
    boolean found2020South = false;
    boolean found2021North = false;
    boolean found2021South = false;
    for (Map<String, String> combo : result) {
      if ("2020".equals(combo.get("year")) && "NORTH".equals(combo.get("region"))) {
        found2020North = true;
      }
      if ("2020".equals(combo.get("year")) && "SOUTH".equals(combo.get("region"))) {
        found2020South = true;
      }
      if ("2021".equals(combo.get("year")) && "NORTH".equals(combo.get("region"))) {
        found2021North = true;
      }
      if ("2021".equals(combo.get("year")) && "SOUTH".equals(combo.get("region"))) {
        found2021South = true;
      }
    }
    assertTrue(found2020North && found2020South && found2021North && found2021South);
  }

  @Test
  void testExpandWithEmptyDimensionSkipped() {
    DimensionIterator iterator = new DimensionIterator();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    // This dimension has no start/end, so resolveRange returns empty
    dimensions.put("empty_range", DimensionConfig.builder()
        .name("empty_range")
        .type(DimensionType.RANGE)
        .start(null)
        .end(null)
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    // Empty dimension is skipped, returns single empty combination
    assertEquals(1, result.size());
    assertTrue(result.get(0).isEmpty());
  }

  // ====================================================================
  // Tests for expandWithContext (CUSTOM dimensions)
  // ====================================================================

  @Test
  void testExpandWithContextSingleCustom() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);

    // When context has state=CA, return CA values
    when(resolver.resolve(eq("city"), any(), argThat(ctx -> "CA".equals(ctx.get("state"))), any()))
        .thenReturn(Arrays.asList("LA", "SF"));
    when(resolver.resolve(eq("city"), any(), argThat(ctx -> "NY".equals(ctx.get("state"))), any()))
        .thenReturn(Arrays.asList("NYC", "Buffalo"));

    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    assertEquals(4, result.size()); // 2 states x 2 cities each
  }

  @Test
  void testExpandWithContextCustomReturnsEmpty() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(anyString(), any(), any(), any()))
        .thenReturn(Collections.emptyList());

    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    // CUSTOM dimension empty causes entire chain to collapse
    assertTrue(result.isEmpty());
  }

  @Test
  void testExpandWithCustomButNoResolver() {
    // When hasCustomDimensions is true but dimensionResolver is null, falls back to standard
    DimensionIterator iterator = new DimensionIterator();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("list_dim", DimensionConfig.builder()
        .name("list_dim")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "B"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    // Without resolver, standard expansion is used, which calls resolveCustom which throws
    assertThrows(IllegalStateException.class, () -> iterator.expand(dimensions));
  }

  // ====================================================================
  // Tests for planPartitions
  // ====================================================================

  @Test
  void testPlanPartitionsWithNull() {
    DimensionIterator iterator = new DimensionIterator();
    assertNull(iterator.planPartitions(null));
  }

  @Test
  void testPlanPartitionsWithEmpty() {
    DimensionIterator iterator = new DimensionIterator();
    assertNull(iterator.planPartitions(Collections.emptyMap()));
  }

  @Test
  void testPlanPartitionsWithNoCustomDimensions() {
    DimensionIterator iterator = new DimensionIterator();
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    assertNull(iterator.planPartitions(dimensions));
  }

  @Test
  void testPlanPartitionsWithCustomButNoResolver() {
    DimensionIterator iterator = new DimensionIterator();
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    assertNull(iterator.planPartitions(dimensions));
  }

  @Test
  void testPlanPartitionsWithCustomAndContextKey() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY", "TX"))
        .build());

    Map<String, String> props = new HashMap<>();
    props.put("contextKey", "state");
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);
    assertEquals("state", plan.getContextKey());
    assertEquals(3, plan.getPartitionCount());
    assertEquals(Arrays.asList("CA", "NY", "TX"), plan.getContextValues());
  }

  @Test
  void testPlanPartitionsWithContextKeyFromPreviousDimension() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    // No explicit contextKey property, should infer from previous dimension
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);
    assertEquals("state", plan.getContextKey());
  }

  @Test
  void testPlanPartitionsWithCustomAsFirstDimNoContextKey() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    // CUSTOM is the FIRST dimension, so there's no previous dimension as context key
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNull(plan, "Should return null when no context key can be identified");
  }

  // ====================================================================
  // Tests for expandPartition
  // ====================================================================

  @Test
  void testExpandPartitionBasic() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(eq("city"), any(), any(), any()))
        .thenReturn(Arrays.asList("LA", "SF"));

    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());

    Map<String, String> props = new HashMap<>();
    props.put("contextKey", "state");
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);

    DimensionPartition partition = iterator.expandPartition(plan, "CA");
    assertNotNull(partition);
    assertEquals(2, partition.getCombinations().size());
    assertEquals("CA", partition.getPartitionContext().get("state"));
  }

  @Test
  void testExpandPartitionEmptyGroupCombos() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());

    Map<String, String> props = new HashMap<>();
    props.put("contextKey", "state");
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);

    // Expand for a context value that doesn't exist in the plan
    DimensionPartition partition = iterator.expandPartition(plan, "UNKNOWN_STATE");
    assertNull(partition);
  }

  @Test
  void testExpandPartitionCustomReturnsEmpty() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(anyString(), any(), any(), any()))
        .thenReturn(Collections.emptyList());

    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());

    Map<String, String> props = new HashMap<>();
    props.put("contextKey", "state");
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);

    // CUSTOM dimension returns empty, so partition should be null
    DimensionPartition partition = iterator.expandPartition(plan, "CA");
    assertNull(partition);
  }

  // ====================================================================
  // Tests for truncateForLog (private method)
  // ====================================================================

  @Test
  void testTruncateForLogSmallList() throws Exception {
    DimensionIterator iterator = new DimensionIterator();
    java.lang.reflect.Method method = DimensionIterator.class.getDeclaredMethod(
        "truncateForLog", List.class);
    method.setAccessible(true);

    List<String> small = Arrays.asList("a", "b", "c");
    String result = (String) method.invoke(iterator, small);
    assertEquals("[a, b, c]", result);
  }

  @Test
  void testTruncateForLogLargeList() throws Exception {
    DimensionIterator iterator = new DimensionIterator();
    java.lang.reflect.Method method = DimensionIterator.class.getDeclaredMethod(
        "truncateForLog", List.class);
    method.setAccessible(true);

    List<String> large = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      large.add("val" + i);
    }
    String result = (String) method.invoke(iterator, large);
    assertTrue(result.contains("..."));
    assertTrue(result.contains("20 total"));
  }

  @Test
  void testTruncateForLogExactlyFive() throws Exception {
    DimensionIterator iterator = new DimensionIterator();
    java.lang.reflect.Method method = DimensionIterator.class.getDeclaredMethod(
        "truncateForLog", List.class);
    method.setAccessible(true);

    List<String> five = Arrays.asList("a", "b", "c", "d", "e");
    String result = (String) method.invoke(iterator, five);
    assertEquals("[a, b, c, d, e]", result);
  }

  // ====================================================================
  // Tests for three-dimension Cartesian product
  // ====================================================================

  @Test
  void testExpandThreeDimensions() {
    DimensionIterator iterator = new DimensionIterator();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("N", "S"))
        .build());
    dimensions.put("type", DimensionConfig.builder()
        .name("type")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "B", "C"))
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    assertEquals(12, result.size()); // 2 x 2 x 3

    // Verify each combination has all 3 keys
    for (Map<String, String> combo : result) {
      assertEquals(3, combo.size());
      assertNotNull(combo.get("year"));
      assertNotNull(combo.get("region"));
      assertNotNull(combo.get("type"));
    }
  }

  // ====================================================================
  // Tests for single-dimension expansion
  // ====================================================================

  @Test
  void testExpandSingleDimension() {
    DimensionIterator iterator = new DimensionIterator();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    List<Map<String, String>> result = iterator.expand(dimensions);
    assertEquals(3, result.size());
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("2021", result.get(1).get("year"));
    assertEquals("2022", result.get(2).get("year"));
  }

  // ====================================================================
  // Tests for multiple CUSTOM dimensions in partition plan
  // ====================================================================

  @Test
  void testPlanPartitionsMultipleCustomDimensions() {
    DimensionResolver resolver = mock(DimensionResolver.class);
    StorageProvider provider = mock(StorageProvider.class);
    when(resolver.resolve(anyString(), any(), any(), any()))
        .thenReturn(Arrays.asList("v1", "v2"));

    DimensionIterator iterator = new DimensionIterator(resolver, provider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());

    Map<String, String> props = new HashMap<>();
    props.put("contextKey", "state");
    dimensions.put("city", DimensionConfig.builder()
        .name("city")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build());

    dimensions.put("district", DimensionConfig.builder()
        .name("district")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);
    assertEquals(2, plan.getCustomDimNames().size());
    assertTrue(plan.getCustomDimNames().contains("city"));
    assertTrue(plan.getCustomDimNames().contains("district"));

    // Expand partition for CA - should cross product both custom dims
    DimensionPartition partition = iterator.expandPartition(plan, "CA");
    assertNotNull(partition);
    // 1 prefix combo x 2 city values x 2 district values = 4
    assertEquals(4, partition.getCombinations().size());
  }
}
