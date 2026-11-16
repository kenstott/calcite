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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code snap_authorized_retailers} — every store authorized to accept SNAP
 * benefits at any point in the last 20 calendar years, from the USDA FNS Historical SNAP Retailer
 * Locator zip. The zip's file name carries the covered years
 * ({@code snap-retailer-locator-data2005-2025.zip}) and changes each year, so the current link is
 * read from {@link #LANDING_URL}; a page with no link throws. One row per authorization spell:
 * a store that lost and regained authorization appears once per spell under the same Record ID.
 * An empty end date means the authorization is still active. Dates are converted from M/D/YYYY to
 * ISO yyyy-MM-dd. Latitude/longitude of exactly 0,0 mark an ungeocoded store and are emitted as
 * null.
 */
public class SnapRetailersProvider extends AbstractBulkCsvProvider {

  /** Page that links the current zip. */
  static final String LANDING_URL = "https://www.fns.usda.gov/snap/retailer/historical-data";

  private static final Pattern ZIP_LINK = Pattern.compile(
      "href=\"(/sites/default/files/resource-files/snap-retailer-locator-data[^\"]*\\.zip)\"");

  private static final Pattern US_DATE = Pattern.compile("(\\d{1,2})/(\\d{1,2})/(\\d{4})");

  private static final String[][] COLUMNS = {
      {"record_id", "Record ID", S},
      {"store_name", "Store Name", S},
      {"store_type", "Store Type", S},
      {"street_number", "Street Number", S},
      {"street_name", "Street Name", S},
      {"additional_address", "Additional Address", S},
      {"city", "City", S},
      {"state_abbr", "State", S},
      {"zip_code", "Zip Code", S},
      {"zip4", "Zip4", S},
      {"county", "County", S},
      {"latitude", "Latitude", D},
      {"longitude", "Longitude", D},
      {"authorization_date", "Authorization Date", S},
      {"end_date", "End Date", S},
  };

  @Override String tableName() {
    return "snap_authorized_retailers";
  }

  @Override String resolveUrl(Map<String, String> variables) throws IOException {
    String page = FiscalHttp.readAll(FiscalHttp.openGetWithRetry(LANDING_URL).getInputStream());
    Matcher m = ZIP_LINK.matcher(page);
    if (!m.find()) {
      throw new IOException("snap_authorized_retailers: no snap-retailer-locator-data zip link on "
          + LANDING_URL);
    }
    return "https://www.fns.usda.gov" + m.group(1);
  }

  @Override String entrySuffix() {
    return ".csv";
  }

  @Override Charset charset() {
    return StandardCharsets.UTF_8;
  }

  @Override String[][] columns() {
    return COLUMNS;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    final Iterator<Map<String, Object>> rows = super.fetch(config, variables, rawCache);
    return new Iterator<Map<String, Object>>() {
      @Override public boolean hasNext() {
        return rows.hasNext();
      }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = rows.next();
        row.put("authorization_date", isoDate(row.get("authorization_date")));
        row.put("end_date", isoDate(row.get("end_date")));
        Object lat = row.get("latitude");
        Object lon = row.get("longitude");
        if (lat != null && lon != null && ((Double) lat).doubleValue() == 0.0
            && ((Double) lon).doubleValue() == 0.0) {
          row.put("latitude", null);
          row.put("longitude", null);
        }
        return row;
      }
    };
  }

  /** Converts M/D/YYYY to yyyy-MM-dd; null stays null; any other shape throws. */
  static String isoDate(Object value) {
    if (value == null) {
      return null;
    }
    String s = (String) value;
    Matcher m = US_DATE.matcher(s);
    if (!m.matches()) {
      throw new IllegalStateException("snap_authorized_retailers: unexpected date '" + s + "'");
    }
    return String.format(java.util.Locale.ROOT, "%s-%02d-%02d", m.group(3),
        Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
  }
}
