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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Resolves Building Permits Survey (BPS) year dimensions dynamically.
 *
 * <p>BPS data is published annually in December. The year dimension uses YYMM format
 * (e.g., 2112 = December 2021, 2212 = December 2022) for Census API URL compatibility.
 *
 * <p>This resolver generates the YYMM codes from a yearRange configuration, avoiding
 * hardcoded year lists that become stale.
 *
 * <p>Schema configuration:
 * <pre>{@code
 * dimensions:
 *   year:
 *     type: yearRange
 *     start: 2020
 *     dataLag: 1
 *
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.census.BuildingPermitsDimensionResolver"
 * }</pre>
 */
public class BuildingPermitsDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BuildingPermitsDimensionResolver.class);

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"year".equals(dimensionName)) {
      return Collections.emptyList();
    }

    Integer startYear = config.getStart();
    Integer dataLag = config.getDataLag();

    if (startYear == null) {
      startYear = 2020;
    }
    if (dataLag == null) {
      dataLag = 1;
    }

    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int effectiveMaxYear = currentYear - dataLag;

    List<String> yymms = new ArrayList<>();
    for (int year = startYear; year <= effectiveMaxYear; year++) {
      String yymm = toYYMM(year);
      yymms.add(yymm);
      LOGGER.debug("BPS-YEARS: generated YYMM code {} for year {}", yymm, year);
    }

    LOGGER.info("BPS-YEARS: resolved {} year codes (start={}, maxYear={}, dataLag={})",
        yymms.size(), startYear, effectiveMaxYear, dataLag);
    return yymms;
  }

  /**
   * Convert year to YYMM format. December month (12) is fixed for BPS.
   * Example: 2021 -> 2112, 2022 -> 2212
   */
  private String toYYMM(int year) {
    int yy = year - 2000;
    int mm = 12;
    return String.format("%d%02d", yy, mm);
  }
}
