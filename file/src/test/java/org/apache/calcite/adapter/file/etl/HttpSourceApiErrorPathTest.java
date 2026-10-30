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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An {@code errorPath} may list several locations separated by {@code |}: BEA reports a
 * request-level failure at {@code BEAAPI.Error}, beside {@code Request}, while a dataset-level one
 * sits under {@code BEAAPI.Results.Error}. A body with an error at either location must not be
 * cached.
 */
@Tag("unit")
class HttpSourceApiErrorPathTest {

  private static final String ERROR_PATH = "BEAAPI.Results.Error|BEAAPI.Error";

  private static final String REQUEST_LEVEL_ERROR = "{\"BEAAPI\":{\"Request\":{},\"Error\":{"
      + "\"APIErrorDescription\":\"Error retrieving NIPA data.\",\"APIErrorCode\":\"201\","
      + "\"ErrorDetail\":{\"Description\":\"Data for this table and frequency are not currently"
      + " available.\"}}}}";

  private static String check(String body, String errorPath) throws Exception {
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("format", "json");
    response.put("errorPath", errorPath);
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("url", "https://api.example.com/data");
    source.put("response", response);
    HttpSourceConfig config = HttpSourceConfig.fromMap(source);
    HttpSource httpSource = new HttpSource(config);
    Method m = HttpSource.class.getDeclaredMethod("checkForApiError", String.class,
        HttpSourceConfig.ResponseConfig.class);
    m.setAccessible(true);
    try {
      return (String) m.invoke(httpSource, body, config.getResponse());
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  @Test void requestLevelErrorIsReported() throws Exception {
    String error = check(REQUEST_LEVEL_ERROR, ERROR_PATH);
    assertNotNull(error);
    assertTrue(error.contains("201"));
  }

  @Test void requestLevelErrorIsMissedByTheResultsPathAlone() throws Exception {
    assertNull(check(REQUEST_LEVEL_ERROR, "BEAAPI.Results.Error"));
  }

  @Test void resultsLevelErrorIsStillReported() throws Exception {
    String body = "{\"BEAAPI\":{\"Results\":{\"Error\":{\"APIErrorCode\":\"40\","
        + "\"APIErrorDescription\":\"Invalid parameter\"}}}}";
    assertNotNull(check(body, ERROR_PATH));
  }

  @Test void noDataErrorIsStillCachedAsEmpty() throws Exception {
    String body = "{\"BEAAPI\":{\"Request\":{},\"Error\":{\"APIErrorCode\":\"201\","
        + "\"APIErrorDescription\":\"No data exists for the requested year\"}}}";
    assertNull(check(body, ERROR_PATH));
  }

  @Test void successfulBodyHasNoError() throws Exception {
    String body = "{\"BEAAPI\":{\"Request\":{},\"Results\":{\"Data\":[{\"a\":1}]}}}";
    assertNull(check(body, ERROR_PATH));
  }
}
