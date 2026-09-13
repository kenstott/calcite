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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashSet;
import java.util.Set;

public class OewsSourceStructureValidator extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("OEWS: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      Set<String> distinctYears = new HashSet<>();

      for (JsonNode row : data) {
        String year = getString(row, "year");
        if (year != null && !year.isEmpty()) {
          distinctYears.add(year);
        }
      }

      if (distinctYears.isEmpty()) {
        LOGGER.warn("OEWS: response contained no year values, assuming single year snapshot");
        return response;
      }

      if (distinctYears.size() > 1) {
        String message = String.format(
            "OEWS source structure changed: %d years detected in response, expected exactly 1. "
            + "Years: %s. BLS OEWS has shifted from snapshot model to time series. "
            + "File as defect immediately.",
            distinctYears.size(), distinctYears);
        LOGGER.error(message);
        throw new IllegalStateException(message);
      }

      String singleYear = distinctYears.iterator().next();
      LOGGER.debug("OEWS: source structure valid, single year={}", singleYear);
      return response;

    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("OEWS: failed to validate source structure for "
          + context.getUrl(), e);
    }
  }
}
