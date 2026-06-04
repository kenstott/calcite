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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Resolves the {@code layer} dimension for {@code timber_sales} by dispatching
 * to the correct {@code EDW_TimberHarvest_01/MapServer/<id>} layer for a given
 * fiscal year.
 *
 * <p>The USFS FACTS timber harvest service splits history into era-specific
 * layers. Each layer publishes the identical 71-field schema, so no per-era
 * column stitching is required — only the URL path segment differs.
 *
 * <table>
 *   <caption>Layer dispatch table (matches MapServer layer titles).</caption>
 *   <tr><th>fy_completed</th><th>layer</th></tr>
 *   <tr><td>2021 — current</td><td>11</td></tr>
 *   <tr><td>2011 — 2020   </td><td>0</td></tr>
 *   <tr><td>2001 — 2010   </td><td>1</td></tr>
 *   <tr><td>1991 — 2000   </td><td>2</td></tr>
 *   <tr><td>1981 — 1990   </td><td>3</td></tr>
 *   <tr><td>1971 — 1980   </td><td>4</td></tr>
 *   <tr><td>1956 — 1970   </td><td>5</td></tr>
 *   <tr><td>1946 — 1955   </td><td>6</td></tr>
 *   <tr><td>1820 — 1945   </td><td>7</td></tr>
 * </table>
 *
 * <p>The {@code year} dimension must appear before {@code layer} in the
 * dimension list so the year is in the resolution context when this resolver
 * runs.
 */
public class UsfsTimberLayerResolver implements DimensionResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsfsTimberLayerResolver.class);

  public UsfsTimberLayerResolver() {
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"layer".equals(dimensionName)) {
      return Collections.emptyList();
    }
    String yearStr = context.get("year");
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IllegalStateException(
          "UsfsTimberLayerResolver: 'year' must be resolved before 'layer'. "
          + "Declare year before layer in the dimensions list.");
    }
    int year;
    try {
      year = Integer.parseInt(yearStr);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "UsfsTimberLayerResolver: year context value is not numeric: " + yearStr, e);
    }
    String layer = layerForYear(year);
    LOGGER.debug("UsfsTimberLayerResolver: year={} -> layer={}", year, layer);
    return Collections.singletonList(layer);
  }

  /**
   * Returns the MapServer layer ID covering the given fiscal year.
   * Package-private for unit testing.
   */
  static String layerForYear(int year) {
    if (year >= 2021) return "11";
    if (year >= 2011) return "0";
    if (year >= 2001) return "1";
    if (year >= 1991) return "2";
    if (year >= 1981) return "3";
    if (year >= 1971) return "4";
    if (year >= 1956) return "5";
    if (year >= 1946) return "6";
    if (year >= 1820) return "7";
    throw new IllegalArgumentException(
        "UsfsTimberLayerResolver: year " + year + " is before earliest FACTS layer (1820)");
  }
}
