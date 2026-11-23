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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SharedHttpClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Response transformer for {@code census.acs_health_insurance}.
 *
 * <p>The table's age/sex cells come from the ACS 5-Year detailed tables (B27001), but the
 * all-ages totals of people with private (S2703) and public (S2704) coverage exist only in the
 * subject tables, a different Census API dataset ({@code acs/acs5/subject}). This transformer
 * runs the standard {@link CensusResponseTransformer} on the detailed response, then fetches the
 * subject variables for the same year and geography level and joins them onto each row by the
 * geography columns.
 *
 * <p>The subject variables exist from the 2017 vintage. For earlier vintages the four joined
 * fields are written as explicit nulls so the table's column expressions resolve.
 */
public class AcsHealthInsuranceTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcsHealthInsuranceTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final int FIRST_SUBJECT_YEAR = 2017;
  static final String[] SUBJECT_VARIABLES = {
      "S2703_C02_001E", "S2703_C02_001M", "S2704_C02_001E", "S2704_C02_001M"};

  private static final Pattern YEAR_IN_URL = Pattern.compile("/data/(\\d{4})/acs/acs5\\?");
  private static final Pattern GET_PARAM = Pattern.compile("([?&])get=[^&]*");
  private static final Pattern VARIABLE_NAME = Pattern.compile("^[BS]\\d{4,5}[A-I]?_.*");

  private final CensusResponseTransformer census = new CensusResponseTransformer();

  @Override public String transform(String response, RequestContext context) {
    String base = census.transform(response, context);
    int year = yearOf(context.getUrl());
    try {
      JsonNode rows = MAPPER.readTree(base);
      if (!rows.isArray() || rows.size() == 0) {
        return base;
      }
      Map<String, ObjectNode> subject = year >= FIRST_SUBJECT_YEAR
          ? fetchSubject(context, year) : null;
      int unmatched = 0;
      for (JsonNode node : rows) {
        ObjectNode row = (ObjectNode) node;
        ObjectNode match = subject == null ? null : subject.get(geoKey(row));
        for (String variable : SUBJECT_VARIABLES) {
          if (match != null && match.hasNonNull(variable)) {
            row.set(variable, match.get(variable));
          } else {
            row.putNull(variable);
            if (subject != null && match == null) {
              unmatched++;
            }
          }
        }
      }
      if (unmatched > 0) {
        LOGGER.warn("acs_health_insurance {}: {} detailed-table rows have no subject-table row",
            year, unmatched / SUBJECT_VARIABLES.length);
      }
      return MAPPER.writeValueAsString(rows);
    } catch (IOException e) {
      throw new RuntimeException("acs_health_insurance: failed to merge subject totals for "
          + year + ": " + e.getMessage(), e);
    }
  }

  private static int yearOf(String url) {
    Matcher m = YEAR_IN_URL.matcher(url);
    if (!m.find()) {
      throw new IllegalStateException("acs_health_insurance: no data year in URL path of the "
          + "detailed-table request");
    }
    return Integer.parseInt(m.group(1));
  }

  static String subjectUrl(String detailedUrl) {
    String url = detailedUrl.replace("/acs/acs5?", "/acs/acs5/subject?");
    StringBuilder get = new StringBuilder("get=NAME");
    for (String variable : SUBJECT_VARIABLES) {
      get.append(',').append(variable);
    }
    return GET_PARAM.matcher(url).replaceFirst("$1" + get);
  }

  private Map<String, ObjectNode> fetchSubject(RequestContext context, int year)
      throws IOException {
    String url = subjectUrl(context.getUrl());
    String body;
    SharedHttpClient.Response conn =
        RetryableHttp.openWithRetry(url, null, context.getRateLimit(), false);
    try {
      InputStream in = conn.getInputStream();
      body = new String(readAll(in), StandardCharsets.UTF_8);
    } finally {
      conn.disconnect();
    }
    JsonNode parsed = MAPPER.readTree(census.transform(body, context));
    if (!parsed.isArray() || parsed.size() == 0) {
      throw new IOException("acs_health_insurance " + year
          + ": subject table S2703/S2704 returned no rows");
    }
    Map<String, ObjectNode> byGeo = new HashMap<String, ObjectNode>();
    for (JsonNode node : (ArrayNode) parsed) {
      byGeo.put(geoKey((ObjectNode) node), (ObjectNode) node);
    }
    return byGeo;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) > 0) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /** Join key: every non-variable, non-name field (state, county, ...), in field order. */
  private static String geoKey(ObjectNode row) {
    StringBuilder key = new StringBuilder();
    Iterator<String> names = row.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (!VARIABLE_NAME.matcher(name).matches() && !"NAME".equals(name)) {
        key.append(name).append('=').append(row.get(name).asText()).append('|');
      }
    }
    return key.toString();
  }
}
