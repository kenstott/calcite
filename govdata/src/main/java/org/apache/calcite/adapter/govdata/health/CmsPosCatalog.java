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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.TreeMap;

/**
 * The quarterly CSV releases of one Provider of Services dataset series in CMS's
 * {@code data.json} DCAT catalog.
 *
 * <p>Each quarter is its own catalog dataset; the datasets of a series share an
 * {@code inSeries} title and are ordered by their {@code temporal} start date.
 */
final class CmsPosCatalog {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Quarter start date (ISO) to CSV download URL. */
  private final TreeMap<String, String> releases;
  private final String seriesTitle;

  private CmsPosCatalog(String seriesTitle, TreeMap<String, String> releases) {
    this.seriesTitle = seriesTitle;
    this.releases = releases;
  }

  /** Parses the catalog and returns the releases of each requested series, in order. */
  static CmsPosCatalog[] parse(InputStream dataJson, String... seriesTitles)
      throws IOException {
    JsonNode datasets = MAPPER.readTree(dataJson).get("dataset");
    if (datasets == null || !datasets.isArray()) {
      throw new IOException("CMS POS: data.json has no 'dataset' array");
    }
    CmsPosCatalog[] out = new CmsPosCatalog[seriesTitles.length];
    for (int i = 0; i < seriesTitles.length; i++) {
      TreeMap<String, String> releases = new TreeMap<String, String>();
      for (JsonNode dataset : datasets) {
        if (!seriesTitles[i].equals(dataset.path("inSeries").path(0).path("title").asText())) {
          continue;
        }
        String start = dataset.path("temporal").path(0).path("startDate").asText();
        if (start.isEmpty()) {
          throw new IOException("CMS POS: dataset '" + dataset.path("title").asText()
              + "' has no temporal start date");
        }
        for (JsonNode dist : dataset.path("distribution")) {
          if ("CSV".equalsIgnoreCase(dist.path("format").asText())
              && dist.hasNonNull("downloadURL")) {
            releases.put(start, dist.get("downloadURL").asText());
          }
        }
      }
      if (releases.isEmpty()) {
        throw new IOException("CMS POS: data.json lists no CSV release in series '"
            + seriesTitles[i] + "'");
      }
      out[i] = new CmsPosCatalog(seriesTitles[i], releases);
    }
    return out;
  }

  String latestQuarter() {
    return releases.lastKey();
  }

  String csvUrl(String quarter) throws IOException {
    String url = releases.get(quarter);
    if (url == null) {
      throw new IOException("CMS POS: series '" + seriesTitle + "' has no CSV release for quarter "
          + quarter);
    }
    return url;
  }
}
