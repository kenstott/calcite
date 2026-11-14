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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Parses TSA checkpoint passenger volumes (tsa.gov/travel/passenger-volumes), an HTML table of
 * {@code M/D/YYYY} and screened-passenger count rows.
 *
 * <p>TSA serves the current calendar year only at the unsuffixed page and prior years at
 * {@code /passenger-volumes/<year>} (the current year's suffixed page is a 404). The source URL
 * is the unsuffixed page; the body is used as-is when its dates fall in the requested
 * {@code year} dimension, otherwise the {@code /<year>} page is downloaded here.
 */
public class TsaCheckpointTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter SOURCE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

  @Override public String transform(String response, RequestContext context) {
    String year = context.getDimensionValues().get("year");
    if (year == null) {
      throw new IllegalStateException("tsa_checkpoint_daily: no 'year' dimension in request");
    }
    int wanted = Integer.parseInt(year);
    try {
      ArrayNode rows = parse(response, wanted);
      if (rows.size() == 0) {
        String html = download(context.getUrl().replaceAll("/+$", "") + "/" + wanted);
        rows = parse(html, wanted);
      }
      if (rows.size() == 0) {
        throw new IllegalStateException("tsa_checkpoint_daily: no rows for year " + wanted);
      }
      return MAPPER.writeValueAsString(rows);
    } catch (IOException e) {
      throw new RuntimeException("tsa_checkpoint_daily: failed for year " + wanted, e);
    }
  }

  private static ArrayNode parse(String html, int wantedYear) {
    Document doc = Jsoup.parse(html);
    ArrayNode out = MAPPER.createArrayNode();
    for (Element tr : doc.select("table tbody tr")) {
      Element dateCell = tr.child(0);
      Element numCell = tr.child(1);
      LocalDate date = LocalDate.parse(dateCell.text().trim(), SOURCE_FORMAT);
      if (date.getYear() != wantedYear) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("travel_date", date.toString());
      row.put("passengers", Long.parseLong(numCell.text().trim().replace(",", "")));
      row.put("type", "tsa_checkpoint_daily");
      out.add(row);
    }
    return out;
  }

  private static String download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[8192];
      java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
      int n;
      while ((n = is.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    }
  }
}
