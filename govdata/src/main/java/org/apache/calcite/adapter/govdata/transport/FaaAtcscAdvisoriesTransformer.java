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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Parses the FAA ATCSCC Advisories Database (fly.faa.gov/adv/adv_list), which lists every
 * advisory sent on one calendar day: ground stops, ground delay programs, airspace flow
 * programs, reroutes, airport delay/closure and equipment-outage notices.
 *
 * <p>The source URL carries the {@code year} dimension as {@code date={year}-01-01}; the body
 * of that response is January 1. The remaining days of the year are downloaded here, one list
 * page per day (the database only answers one date per request). Days after today are not
 * requested.
 */
public class FaaAtcscAdvisoriesTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter ROW_DATE = DateTimeFormatter.ofPattern("MM/dd/yy", Locale.US);
  private static final DateTimeFormatter ROW_TIME =
      DateTimeFormatter.ofPattern("MM/dd/yy HH:mm", Locale.US);

  @Override public String transform(String response, RequestContext context) {
    String year = context.getDimensionValues().get("year");
    if (year == null) {
      throw new IllegalStateException("faa_atcscc_advisories: no 'year' dimension in request");
    }
    int wanted = Integer.parseInt(year);
    String template = context.getUrl();
    String marker = "date=" + wanted + "-01-01";
    if (!template.contains(marker)) {
      throw new IllegalStateException("faa_atcscc_advisories: url lacks " + marker + ": " + template);
    }
    LocalDate today = LocalDate.now();
    ArrayNode out = MAPPER.createArrayNode();
    try {
      LocalDate day = LocalDate.of(wanted, 1, 1);
      parse(response, day, out);
      for (day = day.plusDays(1); day.getYear() == wanted && !day.isAfter(today);
          day = day.plusDays(1)) {
        parse(download(template.replace(marker, "date=" + day)), day, out);
      }
      if (out.size() == 0) {
        throw new IllegalStateException("faa_atcscc_advisories: no advisories for " + wanted);
      }
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("faa_atcscc_advisories: failed for year " + wanted, e);
    }
  }

  private static void parse(String html, LocalDate day, ArrayNode out) {
    Document doc = Jsoup.parse(html);
    if (doc.title().contains("Error")) {
      throw new IllegalStateException("faa_atcscc_advisories: error page for " + day);
    }
    for (Element tr : doc.select("tr")) {
      if (tr.select("a[href*=adv_otherdis]").isEmpty() || tr.children().size() != 5) {
        continue;
      }
      String number = tr.child(0).text().trim();
      String facility = tr.child(1).text().trim();
      LocalDate advDate = LocalDate.parse(tr.child(2).text().trim(), ROW_DATE);
      String subject = tr.child(3).text().replace(' ', ' ').trim();
      LocalDateTime sent = LocalDateTime.parse(tr.child(4).text().trim(), ROW_TIME);
      // The list for a day holds the advisories SENT that day; an advisory numbered on the
      // previous day can be sent just after midnight, so advisory_date may precede the list day.
      if (!sent.toLocalDate().equals(day)) {
        throw new IllegalStateException("faa_atcscc_advisories: row sent " + sent
            + " on list for " + day);
      }
      String upper = subject.toUpperCase(Locale.US);
      ObjectNode row = MAPPER.createObjectNode();
      row.put("advisory_date", advDate.toString());
      row.put("advisory_number", number);
      row.put("facility", facility);
      row.put("subject", subject);
      row.put("advisory_type", classify(upper));
      row.put("is_cancellation", upper.contains("CNX") || upper.contains("CANCEL"));
      row.put("is_proposed", upper.contains("PROPOSED"));
      row.put("sent_time_utc", sent.toString());
      row.put("type", "faa_atcscc_advisories");
      out.add(row);
    }
  }

  static String classify(String upper) {
    if (upper.contains("GROUND STOP") || upper.matches(".*\\bGS\\b.*")) {
      return "GROUND_STOP";
    }
    if (upper.contains("GROUND DELAY") || upper.matches(".*\\bGDP\\b.*")) {
      return "GROUND_DELAY_PROGRAM";
    }
    if (upper.contains("OUTAGE") || upper.contains("OUT OF SERVICE")
        || upper.contains("EQUIPMENT") || upper.contains("TELECOM")) {
      return "OUTAGE";
    }
    if (upper.contains("AIRSPACE FLOW") || upper.startsWith("AFP") || upper.contains("CTOP")) {
      return "FLOW_PROGRAM";
    }
    if (upper.contains("ROUTE") || upper.contains("FCA") || upper.contains("FEA")
        || upper.contains("SWAP") || upper.contains("PLAYBOOK") || upper.contains("NRP")) {
      return "ROUTE_OR_FLOW_AREA";
    }
    if (upper.contains("DELAY") || upper.contains("CLOSED") || upper.contains("CLSD")) {
      return "AIRPORT_DELAY_OR_CLOSURE";
    }
    return "OTHER";
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
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      int n;
      while ((n = is.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    }
  }
}
