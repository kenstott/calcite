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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Expands dimension definitions into concrete value combinations for batch processing.
 *
 * <p>DimensionIterator takes dimension configurations and produces a list of
 * parameter maps, where each map represents one combination of dimension values.
 * This is used by the ETL pipeline to iterate over batch combinations.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Define dimensions
 * Map<String, DimensionConfig> dimensions = new LinkedHashMap<>();
 * dimensions.put("year", DimensionConfig.builder()
 *     .name("year")
 *     .type(DimensionType.RANGE)
 *     .start(2020)
 *     .end(2024)
 *     .build());
 * dimensions.put("region", DimensionConfig.builder()
 *     .name("region")
 *     .type(DimensionType.LIST)
 *     .values(Arrays.asList("NORTH", "SOUTH", "EAST", "WEST"))
 *     .build());
 *
 * // Expand to combinations
 * DimensionIterator iterator = new DimensionIterator();
 * List<Map<String, String>> combinations = iterator.expand(dimensions);
 *
 * // Result: 20 combinations (5 years x 4 regions)
 * // [{year=2020, region=NORTH}, {year=2020, region=SOUTH}, ...]
 * }</pre>
 *
 * <h3>Dimension Types</h3>
 * <ul>
 *   <li>{@code RANGE} - Numeric sequence (start, end, step)</li>
 *   <li>{@code LIST} - Explicit value list</li>
 *   <li>{@code QUERY} - SQL query to fetch values</li>
 *   <li>{@code YEAR_RANGE} - Year range with current year support</li>
 * </ul>
 *
 * @see DimensionConfig
 * @see DimensionType
 * @see HiveParquetWriter
 */
public class DimensionIterator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionIterator.class);

  private final Connection queryConnection;
  private final DimensionResolver dimensionResolver;
  private final StorageProvider storageProvider;

  /**
   * Creates a DimensionIterator without SQL query or custom resolver support.
   * Query-type and custom-type dimensions will throw an exception.
   */
  public DimensionIterator() {
    this.queryConnection = null;
    this.dimensionResolver = null;
    this.storageProvider = null;
  }

  /**
   * Creates a DimensionIterator with SQL query support.
   *
   * @param queryConnection JDBC connection for executing query-type dimensions
   */
  public DimensionIterator(Connection queryConnection) {
    this.queryConnection = queryConnection;
    this.dimensionResolver = null;
    this.storageProvider = null;
  }

  /**
   * Creates a DimensionIterator with custom dimension resolver and storage provider.
   *
   * @param dimensionResolver Custom resolver for CUSTOM type dimensions
   * @param storageProvider Storage provider for file access (local or S3)
   */
  public DimensionIterator(DimensionResolver dimensionResolver, StorageProvider storageProvider) {
    this.queryConnection = null;
    this.dimensionResolver = dimensionResolver;
    this.storageProvider = storageProvider;
  }

  /**
   * Creates a DimensionIterator with SQL query, custom resolver, and storage provider.
   *
   * @param queryConnection JDBC connection for executing query-type dimensions
   * @param dimensionResolver Custom resolver for CUSTOM type dimensions
   * @param storageProvider Storage provider for file access (local or S3)
   */
  public DimensionIterator(Connection queryConnection, DimensionResolver dimensionResolver,
      StorageProvider storageProvider) {
    this.queryConnection = queryConnection;
    this.dimensionResolver = dimensionResolver;
    this.storageProvider = storageProvider;
  }

  /**
   * Expands dimension configurations into all value combinations.
   *
   * <p>This method computes the Cartesian product of all dimension values.
   * For example, if dimension A has values [1, 2] and dimension B has values
   * [X, Y, Z], the result will be 6 combinations.
   *
   * <p>For CUSTOM type dimensions, the resolver is called with context from
   * previously-resolved dimensions, enabling dependent dimension patterns.
   *
   * @param dimensions Map of dimension name to configuration
   * @return List of parameter maps, one per combination
   */
  public List<Map<String, String>> expand(Map<String, DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      LOGGER.debug("No dimensions to expand, returning single empty combination");
      return Collections.singletonList(Collections.<String, String>emptyMap());
    }

    // Context-aware expansion is needed for CUSTOM dimensions (external resolver)
    // and for calendar period dimensions (quarter/month/week/day read coarser
    // periods such as year/month from the context).
    boolean hasCustomDimensions = false;
    boolean hasPeriodDimensions = false;
    for (DimensionConfig config : dimensions.values()) {
      if (config.getType() == DimensionType.CUSTOM) {
        hasCustomDimensions = true;
      } else if (CalendarPeriodProvider.isPeriodUnit(config.getType())) {
        hasPeriodDimensions = true;
      }
    }

    List<Map<String, String>> combinations;
    if ((hasCustomDimensions && dimensionResolver != null) || hasPeriodDimensions) {
      combinations = expandWithContext(dimensions);
    } else {
      combinations = expandStandard(dimensions);
    }

    // For every YEAR_RANGE dimension inject effective_year = year - dataLag into each combination.
    // When dataLag=0, effective_year equals year (no distinction). When dataLag>0, effective_year
    // is the actual data year; the iteration variable year is the publish year.
    // URL templates use ${effective_year} for APIs that take the data year (most schemas).
    // The Iceberg writer uses effective_year as the year partition key.
    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      DimensionConfig config = entry.getValue();
      if (config.getType() != DimensionType.YEAR_RANGE) {
        continue;
      }
      int dataLag = config.getDataLag() != null ? config.getDataLag() : 0;
      for (Map<String, String> combo : combinations) {
        String yearVal = combo.get(entry.getKey());
        if (yearVal != null) {
          try {
            combo.put("effective_year", String.valueOf(Integer.parseInt(yearVal) - dataLag));
          } catch (NumberFormatException e) {
            // non-numeric year value; skip companion injection
          }
        }
      }
    }

    // For every monthlyYTD YEAR_RANGE dimension, inject a per-year `month` companion so a
    // monthly-cadence feed fetches one year-end file per completed year (month=12) and a
    // year-to-date window for the in-progress year (month = last completed month). This
    // replaces the global GOVDATA_CURRENT_MONTH that truncated every historical year to the
    // current month. `month` is a derived companion (like effective_year), not a cartesian axis.
    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      DimensionConfig config = entry.getValue();
      if (config.getType() != DimensionType.YEAR_RANGE || !config.isMonthlyYtd()) {
        continue;
      }
      // Calendar.MONTH is 0-based, so its value is exactly the number of the last fully
      // completed month: June (MONTH=5) -> 5 (May is the last closed month); January
      // (MONTH=0) -> 0, meaning no month has completed yet for the current year.
      int currentYear = Calendar.getInstance().get(Calendar.YEAR);
      int lastCompletedMonth = Calendar.getInstance().get(Calendar.MONTH);
      int dropped = 0;
      Iterator<Map<String, String>> it = combinations.iterator();
      while (it.hasNext()) {
        Map<String, String> combo = it.next();
        String yearVal = combo.get(entry.getKey());
        if (yearVal == null) {
          continue;
        }
        int year;
        try {
          year = Integer.parseInt(yearVal);
        } catch (NumberFormatException e) {
          continue;
        }
        if (year < currentYear) {
          combo.put("month", "12");
        } else if (lastCompletedMonth >= 1) {
          combo.put("month", lastCompletedMonth < 10
              ? "0" + lastCompletedMonth : String.valueOf(lastCompletedMonth));
        } else {
          // Current year with no completed month yet (January) — nothing to fetch for it.
          it.remove();
          dropped++;
        }
      }
      if (dropped > 0) {
        LOGGER.info("monthlyYTD '{}': dropped {} current-year combination(s) "
            + "with no completed month yet", entry.getKey(), dropped);
      }
    }

    return combinations;
  }

  /**
   * Creates a lightweight plan for partitioned dimension expansion.
   *
   * <p>For pipelines with CUSTOM dimensions (e.g., {@code ori} resolved per {@code state_abbr}),
   * the full Cartesian product can be millions of combinations. This method builds a plan
   * that allows expanding one partition at a time via {@link #expandPartition}, keeping
   * only one partition's combinations in memory.
   *
   * <p>Returns {@code null} if no CUSTOM dimensions exist or no context key can be identified.
   * Callers should fall back to {@link #expand} in that case.
   *
   * @param dimensions Map of dimension name to configuration
   * @return Partition plan, or null if partitioning is not applicable
   */
  public DimensionPartitionPlan planPartitions(Map<String, DimensionConfig> dimensions) {
    if (dimensions == null || dimensions.isEmpty()) {
      return null;
    }

    // Collect all CUSTOM dimensions (in declaration order)
    List<String> customDimNames = new ArrayList<String>();
    List<DimensionConfig> customDimConfigs = new ArrayList<DimensionConfig>();
    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      if (entry.getValue().getType() == DimensionType.CUSTOM) {
        customDimNames.add(entry.getKey());
        customDimConfigs.add(entry.getValue());
      }
    }
    if (customDimNames.isEmpty() || dimensionResolver == null) {
      return null;
    }

    String firstCustomDimName = customDimNames.get(0);
    DimensionConfig firstCustomDimConfig = customDimConfigs.get(0);

    // Expand all non-CUSTOM dimensions (the "prefix" combinations)
    Map<String, DimensionConfig> prefixDimensions = new LinkedHashMap<String, DimensionConfig>();
    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      if (entry.getValue().getType() != DimensionType.CUSTOM) {
        prefixDimensions.put(entry.getKey(), entry.getValue());
      }
    }
    List<Map<String, String>> prefixCombinations = expandStandard(prefixDimensions);
    LOGGER.info("Prefix expansion (non-CUSTOM): {} combinations", prefixCombinations.size());

    // Identify context key from properties or fallback
    String contextKey = null;
    if (firstCustomDimConfig.getProperties() != null) {
      contextKey = firstCustomDimConfig.getProperties().get("contextKey");
    }
    if (contextKey == null) {
      String prevKey = null;
      for (String key : dimensions.keySet()) {
        if (key.equals(firstCustomDimName)) {
          break;
        }
        prevKey = key;
      }
      contextKey = prevKey;
    }
    if (contextKey == null) {
      LOGGER.warn("CUSTOM dimension '{}' has no identifiable context key, "
          + "falling back to standard expansion", firstCustomDimName);
      return null;
    }
    LOGGER.info("Partitioning by context key '{}' for {} CUSTOM dimension(s) {}",
        contextKey, customDimNames.size(), customDimNames);

    // Group prefix combos by context key value
    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();
    Set<String> contextValues = new TreeSet<String>();
    for (Map<String, String> combo : prefixCombinations) {
      String val = combo.get(contextKey);
      if (val != null) {
        contextValues.add(val);
        List<Map<String, String>> group = prefixByContext.get(val);
        if (group == null) {
          group = new ArrayList<Map<String, String>>();
          prefixByContext.put(val, group);
        }
        group.add(combo);
      }
    }
    LOGGER.info("Found {} distinct '{}' values for partitioning",
        contextValues.size(), contextKey);

    return new DimensionPartitionPlan(contextKey,
        new ArrayList<String>(contextValues), customDimNames, customDimConfigs,
        prefixByContext);
  }

  /**
   * Expands a single partition from a partition plan.
   *
   * <p>Resolves the CUSTOM dimension for the given context value and produces
   * the full combinations for that partition only. The caller should discard
   * the returned partition after processing to allow GC.
   *
   * @param plan The partition plan from {@link #planPartitions}
   * @param contextValue The context value to expand (e.g., "CA")
   * @return The expanded partition, or null if the CUSTOM dimension resolved to empty
   */
  public DimensionPartition expandPartition(DimensionPartitionPlan plan, String contextValue) {
    List<Map<String, String>> groupCombos = plan.getPrefixCombinations(contextValue);
    if (groupCombos.isEmpty()) {
      return null;
    }

    List<String> customDimNames = plan.getCustomDimNames();
    List<DimensionConfig> customDimConfigs = plan.getCustomDimConfigs();

    // Start with prefix combinations, then iteratively cross-product each CUSTOM dimension
    List<Map<String, String>> currentCombos = groupCombos;

    for (int d = 0; d < customDimNames.size(); d++) {
      String dimName = customDimNames.get(d);
      DimensionConfig dimConfig = customDimConfigs.get(d);

      // Resolve this CUSTOM dimension PER COMBO, using that combo's own context. A dependent
      // custom dimension (e.g. a pubEndDate paired with a prior pubStartDate) returns a different
      // value for each prior-dimension value, so resolving once from a single "representative"
      // combo and cross-producting would wrongly stamp one value (the first combo's) onto every
      // combination. Memoize by context so an independent resolver (same result for every combo,
      // e.g. ORIs keyed only on the partition's state) still resolves at most once per context.
      Map<Map<String, String>, List<String>> valuesByContext =
          new HashMap<Map<String, String>, List<String>>();
      List<Map<String, String>> expanded = new ArrayList<Map<String, String>>();
      for (Map<String, String> combo : currentCombos) {
        List<String> customValues = valuesByContext.get(combo);
        if (customValues == null) {
          customValues = resolveCustomWithContext(dimConfig, combo);
          valuesByContext.put(new LinkedHashMap<String, String>(combo), customValues);
        }
        for (String customVal : customValues) {
          Map<String, String> full = new LinkedHashMap<String, String>(combo);
          full.put(dimName, customVal);
          expanded.add(full);
        }
      }

      if (expanded.isEmpty()) {
        LOGGER.debug("CUSTOM dimension '{}' resolved to empty for {}={}, skipping partition",
            dimName, plan.getContextKey(), contextValue);
        return null;
      }

      LOGGER.debug("CUSTOM dimension '{}' for {}={}: {} -> {} combinations (per-combo)",
          dimName, plan.getContextKey(), contextValue,
          currentCombos.size(), expanded.size());

      currentCombos = expanded;
    }

    Map<String, String> partContext =
        Collections.singletonMap(plan.getContextKey(), contextValue);
    LOGGER.debug("Partition {}={}: {} total combinations after {} CUSTOM dimension(s)",
        plan.getContextKey(), contextValue, currentCombos.size(), customDimNames.size());

    return new DimensionPartition(partContext, currentCombos);
  }

  /**
   * Standard expansion without context (original behavior).
   */
  private List<Map<String, String>> expandStandard(Map<String, DimensionConfig> dimensions) {
    List<String> dimensionNames = new ArrayList<String>();
    List<List<String>> dimensionValues = new ArrayList<List<String>>();

    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      DimensionConfig config = entry.getValue();

      List<String> values = resolveDimension(config);
      if (values.isEmpty()) {
        // A defined dimension with no values zeroes the Cartesian product (X × ∅ = ∅).
        // Dropping it (the old behavior) emitted combinations missing a required fetch
        // variable — e.g. a {year}-templated bulk URL left unresolved when a lagged-annual
        // yearRange inverts to [2026, 2025] in a daily pass — which then materialised a broken,
        // empty Iceberg table. Return zero combinations so the table is cleanly skipped for this
        // pass (mirrors expandWithContext's newResult-empty guard).
        LOGGER.warn("Dimension '{}' resolved to empty values — yielding zero combinations "
            + "(nothing to process this pass)", name);
        return Collections.emptyList();
      }

      dimensionNames.add(name);
      dimensionValues.add(values);
      LOGGER.debug("Dimension '{}' resolved to {} values: {}",
          name, values.size(), truncateForLog(values));
    }

    if (dimensionNames.isEmpty()) {
      LOGGER.debug("All dimensions resolved to empty, returning single empty combination");
      return Collections.singletonList(Collections.<String, String>emptyMap());
    }

    // Compute Cartesian product
    List<Map<String, String>> combinations = cartesianProduct(dimensionNames, dimensionValues);
    LOGGER.info("Expanded {} dimensions into {} combinations",
        dimensionNames.size(), combinations.size());

    return combinations;
  }

  /**
   * Context-aware expansion for CUSTOM dimensions.
   *
   * <p>Iteratively expands dimensions, passing context to the resolver for
   * CUSTOM dimensions so they can return values based on previously-selected
   * dimension values.
   */
  private List<Map<String, String>> expandWithContext(Map<String, DimensionConfig> dimensions) {
    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    result.add(new LinkedHashMap<String, String>());

    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      DimensionConfig config = entry.getValue();

      List<Map<String, String>> newResult = new ArrayList<Map<String, String>>();

      for (Map<String, String> existing : result) {
        // Resolve dimension values with context from existing combination
        List<String> values;
        if (config.getType() == DimensionType.CUSTOM) {
          values = resolveCustomWithContext(config, existing);
        } else if (CalendarPeriodProvider.isPeriodUnit(config.getType())) {
          values = CalendarPeriodProvider.values(config.getType(), config.getWeekYear(),
              existing, todayUtc(), config.isSkipWeekends());
        } else {
          values = resolveDimension(config);
        }

        if (values.isEmpty()) {
          LOGGER.debug("Dimension '{}' resolved to empty for context {}, skipping",
              name, existing);
          continue;
        }

        // Expand this combination with each dimension value
        for (String value : values) {
          Map<String, String> newCombination = new LinkedHashMap<String, String>(existing);
          newCombination.put(name, value);
          newResult.add(newCombination);
        }
      }

      if (newResult.isEmpty()) {
        LOGGER.warn("Dimension '{}' resulted in no valid combinations", name);
        return new ArrayList<Map<String, String>>();
      }

      result = newResult;
      LOGGER.debug("After dimension '{}': {} combinations", name, result.size());
    }

    LOGGER.info("Expanded {} dimensions into {} combinations (context-aware)",
        dimensions.size(), result.size());
    return result;
  }

  /**
   * Resolves a CUSTOM dimension with context from other dimensions.
   */
  private List<String> resolveCustomWithContext(DimensionConfig config,
      Map<String, String> context) {
    try {
      List<String> values =
          dimensionResolver.resolve(config.getName(), config, context, storageProvider);
      if (values == null) {
        throw new IllegalStateException(
            "DimensionResolver for dimension '" + config.getName()
            + "' returned null; a resolver with no values must return an empty list, not null");
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Custom dimension '{}' resolved to {} values for context {}",
            config.getName(), values.size(), context);
      }
      return values;
    } catch (RuntimeException e) {
      LOGGER.error("DimensionResolver failed for '{}' with context {}: {}",
          config.getName(), context, e.getMessage());
      throw new RuntimeException(
          "Failed to resolve custom dimension '" + config.getName() + "'", e);
    }
  }

  /**
   * Expands a single dimension configuration to its list of values.
   *
   * @param config Dimension configuration
   * @return List of string values
   */
  public List<String> resolveDimension(DimensionConfig config) {
    switch (config.getType()) {
      case RANGE:
        return resolveRange(config);
      case LIST:
        return resolveList(config);
      case QUERY:
        return resolveQuery(config);
      case YEAR_RANGE:
        return resolveYearRange(config);
      case CUSTOM:
        return resolveCustom(config);
      case JSON_CATALOG:
        return resolveJsonCatalog(config);
      case QUARTER:
      case MONTH:
      case WEEK:
      case DAY:
      case DAY_OF_WEEK:
        // Fallback when a period dimension is resolved without context (e.g. no
        // coarser year dimension). Context-aware expansion is the normal path.
        return CalendarPeriodProvider.values(config.getType(), config.getWeekYear(),
            Collections.<String, String>emptyMap(), todayUtc(), config.isSkipWeekends());
      default:
        throw new IllegalStateException("Unknown dimension type '" + config.getType()
            + "' for dimension '" + config.getName() + "'");
    }
  }

  /** Current date in UTC; the anchor for current-period capping in providers. */
  private static LocalDate todayUtc() {
    return LocalDate.now(ZoneOffset.UTC);
  }

  /**
   * Resolves a LIST type dimension, applying minYear/maxYear/dataLag bounds if set.
   */
  private List<String> resolveList(DimensionConfig config) {
    List<String> values = config.getValues();
    Integer minYear = config.getMinYear();
    Integer maxYear = config.getMaxYear();
    Integer dataLag = config.getDataLag();
    int effectiveMax = (maxYear != null) ? maxYear : Integer.MAX_VALUE;
    if (dataLag != null && dataLag > 0) {
      int lagYear = Calendar.getInstance().get(Calendar.YEAR) - dataLag;
      effectiveMax = Math.min(effectiveMax, lagYear);
    }
    boolean hasFilter = minYear != null || effectiveMax != Integer.MAX_VALUE;
    List<String> result;
    if (!hasFilter) {
      result = values;
    } else {
      List<String> filtered = new ArrayList<String>();
      for (String value : values) {
        try {
          int year = Integer.parseInt(value.trim());
          if (minYear != null && year < minYear) {
            LOGGER.warn("List dimension '{}': value {} predates data floor {} — skipping",
                config.getName(), year, minYear);
            continue;
          }
          if (effectiveMax != Integer.MAX_VALUE && year > effectiveMax) {
            LOGGER.warn("List dimension '{}': value {} exceeds data ceiling {} — skipping",
                config.getName(), year, effectiveMax);
            continue;
          }
          filtered.add(value);
        } catch (NumberFormatException e) {
          filtered.add(value);
        }
      }
      result = filtered;
    }
    if (config.isDescending()) {
      List<String> reversed = new ArrayList<String>(result);
      Collections.reverse(reversed);
      return reversed;
    }
    return result;
  }

  /**
   * Resolves a RANGE type dimension.
   */
  private List<String> resolveRange(DimensionConfig config) {
    Integer start = config.getStart();
    Integer end = config.getEnd();
    Integer step = config.getStep();

    if (start == null || end == null) {
      LOGGER.warn("Range dimension '{}' missing start or end", config.getName());
      return new ArrayList<String>();
    }

    if (step == null || step == 0) {
      step = 1;
    }

    List<String> values = new ArrayList<String>();
    if (step > 0) {
      for (int i = start; i <= end; i += step) {
        values.add(String.valueOf(i));
      }
    } else {
      for (int i = start; i >= end; i += step) {
        values.add(String.valueOf(i));
      }
    }

    if (config.isDescending()) {
      Collections.reverse(values);
    }
    return values;
  }

  /**
   * Resolves a YEAR_RANGE type dimension.
   *
   * <p>Iterates over publish years. The {@code effective_year} companion (publish year - dataLag)
   * is injected into each combination by {@link #expand} after resolution.
   *
   * <p>End cap: the latest valid publish year is derived from {@code current_year - dataLag}
   * (the effective year floor) plus {@code dataLag}, which equals {@code current_year} adjusted
   * for {@code releaseMonth}. This ensures the URL template parameter {@code ${effective_year}}
   * never exceeds the latest available data year.
   */
  private List<String> resolveYearRange(DimensionConfig config) {
    Integer start = config.getStart();
    Integer end = config.getEnd();
    Integer step = config.getStep();
    Integer dataLag = config.getDataLag();

    if (start == null) {
      LOGGER.warn("Year range dimension '{}' missing start", config.getName());
      return new ArrayList<String>();
    }

    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int lag = dataLag != null ? dataLag : 0;

    if (step == null || step == 0) {
      step = 1;
    }

    // Latest available data year = currentYear - dataLag, adjusted for releaseMonth.
    int latestEffectiveYear = currentYear - lag;
    Integer releaseMonth = config.getReleaseMonth();
    if (releaseMonth != null && releaseMonth >= 1 && releaseMonth <= 12) {
      int currentMonth = Calendar.getInstance().get(Calendar.MONTH) + 1;
      if (currentMonth < releaseMonth) {
        latestEffectiveYear = latestEffectiveYear - 1;
        LOGGER.debug("Year range dimension '{}' adjusted for releaseMonth={}: latestEffectiveYear={}",
            config.getName(), releaseMonth, latestEffectiveYear);
      }
    }

    // Ceiling depends on cadence:
    //  - Contiguous (step==1): iterate publish years up to the current year; the
    //    effective_year offset (applied in expand()) accounts for dataLag.
    //  - Stepped (step>1): the year IS the data year, so cap at currentYear - dataLag.
    int rangeCeiling = (step > 1) ? latestEffectiveYear : latestEffectiveYear + lag;
    int configuredEnd = (end != null) ? end : rangeCeiling;
    int effectiveEnd = Math.min(rangeCeiling, configuredEnd);

    LOGGER.debug("Year range dimension '{}': rangeCeiling={}, configuredEnd={}, effectiveEnd={}",
        config.getName(), rangeCeiling, configuredEnd, effectiveEnd);

    // Enforce hard data-availability bounds
    Integer minYear = config.getMinYear();
    if (minYear != null && start < minYear) {
      LOGGER.warn("Year range dimension '{}': requested start {} predates data floor {} — clamping to {}",
          config.getName(), start, minYear, minYear);
      start = minYear;
    }
    Integer maxYear = config.getMaxYear();
    if (maxYear != null && effectiveEnd > maxYear) {
      LOGGER.warn("Year range dimension '{}': requested end {} exceeds data ceiling {} — clamping to {}",
          config.getName(), effectiveEnd, maxYear, maxYear);
      effectiveEnd = maxYear;
    }

    if (start > effectiveEnd) {
      LOGGER.warn("Year range dimension '{}': no years in range [{}, {}] — skipping",
          config.getName(), start, effectiveEnd);
      return new ArrayList<String>();
    }

    List<Integer> excludeYears = config.getExcludeYears();
    List<String> values = new ArrayList<String>();

    Integer cadenceStart = config.getCadenceStart();
    Integer cadenceLength = config.getCadenceLength();

    if (cadenceStart != null && cadenceLength != null && cadenceLength > 0) {
      // Cadence mode: generate years anchored at cadenceStart with cadenceLength interval,
      // filtered to [start, effectiveEnd]. Descending so most recent cycle processes first.
      if (effectiveEnd >= cadenceStart) {
        int stepsToEnd = (effectiveEnd - cadenceStart) / cadenceLength;
        int latestCadenceYear = cadenceStart + stepsToEnd * cadenceLength;
        for (int year = latestCadenceYear; year >= start && year >= cadenceStart; year -= cadenceLength) {
          if (excludeYears != null && excludeYears.contains(year)) {
            LOGGER.debug("Year range dimension '{}' excluding cadence year {}", config.getName(), year);
            continue;
          }
          values.add(String.valueOf(year));
        }
      }
    } else {
      // Anchor the grid at `start`: emitted years are exactly
      // start, start+step, start+2*step, ... — never relative to the current
      // year. Descend from the highest aligned year <= effectiveEnd down to start.
      // (For step==1 this equals effectiveEnd, preserving contiguous behavior.)
      int latestAligned = start + ((effectiveEnd - start) / step) * step;
      for (int year = latestAligned; year >= start; year -= step) {
        if (excludeYears != null && excludeYears.contains(year)) {
          LOGGER.debug("Year range dimension '{}' excluding year {}", config.getName(), year);
          continue;
        }
        values.add(String.valueOf(year));
      }
    }

    return values;
  }

  /**
   * Resolves a QUERY type dimension by executing SQL.
   */
  private List<String> resolveQuery(DimensionConfig config) {
    if (queryConnection == null) {
      throw new IllegalStateException(
          "Query dimension '" + config.getName() + "' requires a database connection");
    }

    String sql = config.getSql();
    if (sql == null || sql.isEmpty()) {
      LOGGER.warn("Query dimension '{}' has no SQL", config.getName());
      return new ArrayList<String>();
    }

    List<String> values = new ArrayList<String>();
    try (Statement stmt = queryConnection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String value = rs.getString(1);
        if (value != null) {
          values.add(value);
        }
      }
      LOGGER.debug("Query dimension '{}' returned {} values", config.getName(), values.size());
    } catch (SQLException e) {
      LOGGER.error("Failed to execute query dimension '{}': {}",
          config.getName(), e.getMessage());
      throw new RuntimeException(
          "Failed to resolve query dimension '" + config.getName() + "'", e);
    }

    if (config.isDescending()) {
      Collections.reverse(values);
    }
    return values;
  }

  /**
   * Resolves a CUSTOM type dimension by calling the DimensionResolver.
   *
   * <p>Custom dimensions allow adapters to provide dynamic dimension values
   * from external sources like catalog APIs, databases, or computed values.
   *
   * <p>Note: This method is called from standard expansion without context.
   * For context-aware resolution, use {@link #resolveCustomWithContext}.
   *
   * @param config Dimension configuration
   * @return List of resolved values
   */
  private List<String> resolveCustom(DimensionConfig config) {
    if (dimensionResolver == null) {
      throw new IllegalStateException(
          "Custom dimension '" + config.getName() + "' requires a DimensionResolver. "
          + "Configure hooks.dimensionResolver in the schema.");
    }

    // Call with empty context (no prior dimensions available)
    return resolveCustomWithContext(config, Collections.<String, String>emptyMap());
  }

  /**
   * Resolves a JSON_CATALOG type dimension by loading values from a JSON resource file.
   *
   * <p>Uses {@link JsonCatalogResolver} to load and extract values from the
   * JSON resource specified by the dimension's source and path properties.
   *
   * @param config Dimension configuration with source and path
   * @return List of resolved values
   */
  private List<String> resolveJsonCatalog(DimensionConfig config) {
    String source = config.getSource();
    String path = config.getPath();

    if (source == null || source.isEmpty()) {
      throw new IllegalStateException(
          "JSON_CATALOG dimension '" + config.getName() + "' has no source configured");
    }

    try {
      List<String> values =
          JsonCatalogResolver.resolve(DimensionIterator.class, source, path);
      LOGGER.info("JSON catalog dimension '{}' loaded {} values from {}",
          config.getName(), values.size(), source);
      return values;
    } catch (RuntimeException e) {
      LOGGER.error("Failed to resolve JSON catalog dimension '{}': {}",
          config.getName(), e.getMessage());
      throw new RuntimeException(
          "Failed to resolve JSON catalog dimension '" + config.getName() + "'", e);
    }
  }

  /**
   * Computes the Cartesian product of dimension values.
   *
   * @param names Dimension names (for map keys)
   * @param values List of value lists (one per dimension)
   * @return List of maps representing all combinations
   */
  private List<Map<String, String>> cartesianProduct(
      List<String> names, List<List<String>> values) {

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();

    // Start with a single empty combination
    result.add(new LinkedHashMap<String, String>());

    // For each dimension, expand existing combinations
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      List<String> dimValues = values.get(i);

      List<Map<String, String>> newResult = new ArrayList<Map<String, String>>();

      for (Map<String, String> existing : result) {
        for (String value : dimValues) {
          Map<String, String> newCombination = new LinkedHashMap<String, String>(existing);
          newCombination.put(name, value);
          newResult.add(newCombination);
        }
      }

      result = newResult;
    }

    return result;
  }

  /**
   * Truncates a list for logging purposes.
   */
  private String truncateForLog(List<String> values) {
    if (values.size() <= 5) {
      return values.toString();
    }
    List<String> preview = values.subList(0, 5);
    return preview.toString() + "... (" + values.size() + " total)";
  }

  /**
   * Creates a dimension iterator without query support.
   */
  public static DimensionIterator create() {
    return new DimensionIterator();
  }

  /**
   * Creates a dimension iterator with query support.
   *
   * @param queryConnection JDBC connection for SQL queries
   */
  public static DimensionIterator create(Connection queryConnection) {
    return new DimensionIterator(queryConnection);
  }
}
