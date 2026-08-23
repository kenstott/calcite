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
 * Resolves Building Permits Survey (BPS) period_code dimensions dynamically.
 *
 * <p>BPS data is published annually in December. The period_code dimension uses YYMM format
 * (e.g., 2112 = December 2021, 2212 = December 2022) for Census API URL compatibility. It is
 * a URL code, not a year — e.g. 2012 means December 2020, not the year 2012 — so it must never
 * be filtered as a year; see the table's survey_year/survey_month columns for that.
 *
 * <p>This resolver generates the YYMM codes from a yearRange configuration, avoiding
 * hardcoded year lists that become stale.
 *
 * <p>Schema configuration:
 * <pre>{@code
 * dimensions:
 *   period_code:
 *     type: custom
 *     start: "${GOVDATA_START_YEAR:2010}"   # publish-year demarcation (daily -> current year)
 *     dataLag: 1                             # data year = publish - dataLag; emitted YYMM is final
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
    if (!"period_code".equals(dimensionName)) {
      return Collections.emptyList();
    }

    // The year demarcation is in PUBLISH years (the global GOVDATA_START_YEAR boundary, passed
    // as the 'start' operand). BPS's URL needs the DATA year (stYYMMy.txt), and data/effective
    // year = publish - dataLag, so iterate publish years from the global start to the current
    // year and convert each to its data year. The emitted YYMM is the FINAL dimension value
    // (used directly in the URL — no downstream transform). Daily mode (start = current publish
    // year) therefore resolves to the latest available data year (publish - lag) rather than
    // requesting the not-yet-published current-year file.
    Integer startPublish = config.getStart();
    Integer dataLag = config.getDataLag();

    if (startPublish == null) {
      startPublish = 2010;
    }
    if (dataLag == null) {
      dataLag = 1;
    }

    int currentYear = Calendar.getInstance().get(Calendar.YEAR);

    List<String> periodCodes = new ArrayList<>();
    for (int publishYear = startPublish; publishYear <= currentYear; publishYear++) {
      int dataYear = publishYear - dataLag;
      String periodCode = toYYMM(dataYear);
      periodCodes.add(periodCode);
      LOGGER.debug("BPS-PERIODS: publish {} -> data {} -> period_code {}",
          publishYear, dataYear, periodCode);
    }

    LOGGER.info("BPS-PERIODS: resolved {} period codes (startPublish={}, currentYear={}, "
        + "dataLag={})", periodCodes.size(), startPublish, currentYear, dataLag);
    return periodCodes;
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
