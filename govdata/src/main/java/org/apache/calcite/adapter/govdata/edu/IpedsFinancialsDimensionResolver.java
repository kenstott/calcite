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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Computes fiscal_yy1 and fiscal_yy2 from the resolved year dimension for IPEDS Financials.
 *
 * <p>NCES ZIP files use a 2-digit fiscal-year pair in their filename:
 * F{YY1}{YY2}_{FORM}.zip where YY1 = (year-1) % 100 and YY2 = year % 100.
 * Example: year=2022 → fiscal_yy1=21, fiscal_yy2=22 → F2122_F1A.zip.
 */
public class IpedsFinancialsDimensionResolver implements DimensionResolver {

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    String yearStr = context.get("year");
    if (yearStr == null) {
      return Collections.emptyList();
    }
    int year;
    try {
      year = Integer.parseInt(yearStr);
    } catch (NumberFormatException e) {
      return Collections.emptyList();
    }
    // dataLag shifts the publish (iteration) year to the latest published vintage. effective_year
    // is injected AFTER custom-dimension resolution, so it is not yet in context here — read the
    // lag from this dimension's own config and derive the vintage year directly.
    int lag = config.getDataLag() != null ? config.getDataLag() : 0;
    int vintageYear = year - lag;
    switch (dimensionName) {
    case "fiscal_yy1":
      return Collections.singletonList(String.format("%02d", (vintageYear - 1) % 100));
    case "fiscal_yy2":
      return Collections.singletonList(String.format("%02d", vintageYear % 100));
    default:
      return Collections.emptyList();
    }
  }
}
