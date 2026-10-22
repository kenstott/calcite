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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SharedHttpClient;
import org.apache.calcite.adapter.file.etl.SecretRedaction;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code econ.comtrade_flows} (UN Comtrade bilateral merchandise trade).
 *
 * <p>The Comtrade v1 data API returns a JSON object {@code {"elapsedTime":..,"count":N,"data":[
 * {record},..]}} where each record is one reporter × partner × HS-6 commodity × flow observation
 * for the requested {@code (reporterCode, period, flowCode)}. A single big-reporter call can return
 * hundreds of thousands of records, so this class implements {@link StreamingResponseTransformer}:
 * {@code HttpSource} calls {@link #fetchAndTransform} directly, which opens the URL (applying the
 * {@code Ocp-Apim-Subscription-Key} header from the request context), locates the {@code data}
 * array, and streams one record at a time — heap stays O(1) per row regardless of response size.
 *
 * <p>Each source record is mapped to the clean {@code comtrade_flows} columns: the reporter/partner
 * ISO alpha-3 codes ({@code reporterISO}/{@code partnerISO}) are passed through for the
 * {@code ref.countries} FK; the World partner ({@code partnerCode = 0}) is emitted as
 * {@code partner_code = 'WLD'} with {@code is_aggregate = true}; {@code cmdCode} is split into
 * {@code hs2}/{@code hs6}; and {@code primaryValue}/{@code netWgt}/{@code qty} are coerced to
 * doubles.
 *
 * <p><b>Record cap.</b> The API silently truncates every response at the subscription tier's
 * per-call limit (100,000 records on the free Basic tier, 250,000 on Premium) and reports no true
 * total: {@code count} is the number of records returned, so a response with {@code count} equal
 * to the cap is cut off. The source URL therefore states {@code maxRecords=<cap>}, and a response
 * that reaches it is discarded and requested again as two halves of the Comtrade partner list
 * ({@code partnerCode=a,b,c}), recursively, until every slice comes back under the cap. The
 * partners are disjoint, so the slices concatenate without duplicates. A single partner that still
 * reaches the cap cannot be split further and fails the fetch.
 *
 * <p>NOTE: Comtrade field names follow the documented v1 schema; verify against a live keyed
 * response before relying on this table (requires {@code COMTRADE_API_KEY}).
 */
public class ComtradeTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ComtradeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = MAPPER.getFactory();


  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("ComtradeTransformer: no URL in context");
    }
    if (context.getRateLimit() == null) {
      throw new IllegalStateException(
          "ComtradeTransformer: source declares no rateLimit, so maxRetries is unknown for "
              + SecretRedaction.redact(url));
    }
    long cap = recordCap(url);
    if (queryParam(url, "partnerCode") != null) {
      throw new IllegalStateException("ComtradeTransformer: the source URL must not set "
          + "partnerCode, the transformer slices by partner itself: " + SecretRedaction.redact(url));
    }
    return new SliceIterator(context, url, cap, partners());
  }

  /** The request's {@code maxRecords}: the per-call limit a response must stay below. */
  static long recordCap(String url) {
    String v = queryParam(url, "maxRecords");
    if (v == null) {
      throw new IllegalStateException("ComtradeTransformer: the source URL must declare "
          + "maxRecords=<the subscription tier's per-call record limit> so a truncated response "
          + "can be told from a complete one: " + SecretRedaction.redact(url));
    }
    try {
      long cap = Long.parseLong(v);
      if (cap < 2) {
        throw new NumberFormatException(v);
      }
      return cap;
    } catch (NumberFormatException e) {
      throw new IllegalStateException("ComtradeTransformer: maxRecords must be an integer of at "
          + "least 2, got '" + v + "' in " + SecretRedaction.redact(url), e);
    }
  }

  private static String queryParam(String url, String name) {
    String query = URI.create(url).getRawQuery();
    if (query == null) {
      return null;
    }
    for (String pair : query.split("&")) {
      int eq = pair.indexOf('=');
      if (eq > 0 && pair.substring(0, eq).equals(name)) {
        try {
          return URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return null;
  }

  private static volatile List<String> partnerCodes;

  /** Every partner code Comtrade accepts, from {@code /comtrade/comtrade-partners.json}. */
  static List<String> partners() throws IOException {
    List<String> p = partnerCodes;
    if (p == null) {
      try (InputStream in =
          ComtradeTransformer.class.getResourceAsStream("/comtrade/comtrade-partners.json")) {
        if (in == null) {
          throw new IOException("ComtradeTransformer: /comtrade/comtrade-partners.json is not on "
              + "the classpath");
        }
        List<String> codes = new ArrayList<String>();
        for (com.fasterxml.jackson.databind.JsonNode n : MAPPER.readTree(in).get("partners")) {
          codes.add(n.get("m49").asText());
        }
        if (codes.size() < 2) {
          throw new IOException("ComtradeTransformer: comtrade-partners.json lists "
              + codes.size() + " partners");
        }
        p = Collections.unmodifiableList(codes);
      }
      partnerCodes = p;
    }
    return p;
  }

  /**
   * Streams the rows of a (reporter, period, flow) fetch across as many partner slices as the
   * record cap requires. A slice is opened only when the previous one is drained, so one spooled
   * response is on disk at a time. Every slice is fully read before its rows are used, so a
   * capped slice is dropped whole and none of its rows reach the caller.
   */
  private static final class SliceIterator implements Iterator<Map<String, Object>> {
    private final RequestContext context;
    private final String url;
    private final long cap;
    private final List<String> partners;
    /** Pending slices as [first, last] indices into {@link #partners}; null bounds = all. */
    private final Deque<int[]> pending = new ArrayDeque<int[]>();
    private Iterator<Map<String, Object>> current;
    private int calls;

    SliceIterator(RequestContext context, String url, long cap, List<String> partners)
        throws IOException {
      this.context = context;
      this.url = url;
      this.cap = cap;
      this.partners = partners;
      pending.push(new int[0]);
      advance();
    }

    private void advance() throws IOException {
      while (current == null || !current.hasNext()) {
        current = null;
        if (pending.isEmpty()) {
          return;
        }
        int[] slice = pending.pop();
        boolean all = slice.length == 0;
        String sliceUrl = all ? url : url + "&partnerCode="
            + String.join(",", partners.subList(slice[0], slice[1] + 1));
        Fetch fetch = new Fetch(context, sliceUrl);
        calls++;
        if (!fetch.open()) {
          continue;
        }
        if (fetch.count >= cap) {
          fetch.closeQuietly();
          int lo = all ? 0 : slice[0];
          int hi = all ? partners.size() - 1 : slice[1];
          if (lo == hi) {
            throw new IOException("Comtrade: partner " + partners.get(lo) + " alone returned "
                + fetch.count + " records, the " + cap + "-record cap, and cannot be split "
                + "further: " + SecretRedaction.redact(sliceUrl));
          }
          int mid = (lo + hi) >>> 1;
          LOGGER.info("Comtrade: {} returned the {}-record cap; splitting partners [{}..{}] in two "
              + "(call {})", SecretRedaction.redact(sliceUrl), cap, lo, hi, calls);
          pending.push(new int[] {mid + 1, hi});
          pending.push(new int[] {lo, mid});
          continue;
        }
        current = new RecordIterator(fetch);
      }
    }

    @Override public boolean hasNext() {
      return current != null && current.hasNext();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = current.next();
      if (!current.hasNext()) {
        try {
          advance();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return row;
    }
  }

  /**
   * One Comtrade request. The whole response body is first spooled to a temp file, and only then
   * parsed, so a connection reset or truncated body is always seen before any row reaches the
   * caller. That makes a re-fetch safe at any point in the body (there is nothing to duplicate):
   * the fetch (open + body download) is re-run up to the source's {@code rateLimit.maxRetries}
   * times. Streaming straight off the socket could only retry before the first row, which missed
   * every reset that arrived a few hundred rows in. The spool is on disk, not in memory, because
   * a big-reporter response runs to hundreds of thousands of records.
   */
  private static final class Fetch {
    private final RequestContext context;
    private final String url;
    private final String safeUrl;
    private final Map<String, String> reqHeaders;
    private final int maxRetries;
    private int attempt;
    JsonParser parser;
    /** The response's {@code count}: records returned, which equals the cap when cut off. */
    long count = -1;
    private Path spool;

    Fetch(RequestContext context, String url) {
      this.context = context;
      this.url = url;
      this.safeUrl = SecretRedaction.redact(url);
      this.reqHeaders = new LinkedHashMap<String, String>(context.getHeaders());
      this.reqHeaders.put("Accept", "application/json");
      this.maxRetries = context.getRateLimit().getMaxRetries();
    }

    /**
     * Opens the request, retrying transport faults while locating the {@code data} array.
     *
     * @return true with {@link #parser} inside the array; false when the response has no rows
     */
    boolean open() throws IOException {
      while (true) {
        try {
          return openOnce();
        } catch (IOException e) {
          retryOrThrow(e);
        }
      }
    }

    /** Prepares a re-run after a mid-body fault, or throws once retries are exhausted. */
    void retryOrThrow(IOException cause) throws IOException {
      closeQuietly();
      if (attempt >= maxRetries) {
        throw new IOException("Comtrade: read failed for " + safeUrl + " after "
            + (attempt + 1) + " attempts", cause);
      }
      LOGGER.warn("Comtrade: {} while reading response (attempt {}/{}), retrying whole fetch of {}",
          cause.toString(), attempt + 1, maxRetries + 1, safeUrl);
      RetryableHttp.backoffBeforeRetry(attempt, context.getRateLimit());
      attempt++;
    }

    void closeQuietly() {
      if (parser != null) {
        try {
          parser.close();
        } catch (IOException ignored) {
          // best-effort
        }
        parser = null;
      }
      if (spool != null) {
        try {
          Files.deleteIfExists(spool);
        } catch (IOException e) {
          LOGGER.warn("Comtrade: could not delete spool file {}: {}", spool, e.toString());
        }
        spool = null;
      }
    }

    private boolean openOnce() throws IOException {
      SharedHttpClient.Response conn =
          RetryableHttp.openWithRetry(url, reqHeaders, context.getRateLimit(), false);
      try {
        spool = Files.createTempFile("comtrade-", ".json");
        try (InputStream in = conn.getInputStream()) {
          Files.copy(in, spool, StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        conn.disconnect();
      }
      parser = JSON_FACTORY.createParser(Files.newInputStream(spool));
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        closeQuietly();
        throw new IllegalStateException("Comtrade: expected top-level object for " + safeUrl);
      }
      // Walk top-level fields until the "data" array; capture any error message on the way.
      String errorMessage = null;
      JsonToken t;
      while ((t = parser.nextToken()) != null && t != JsonToken.END_OBJECT) {
        if (t != JsonToken.FIELD_NAME) {
          continue;
        }
        String field = parser.currentName();
        if ("data".equals(field)) {
          if (count < 0) {
            closeQuietly();
            throw new IllegalStateException("Comtrade: response for " + safeUrl + " has no count "
                + "before its data array, so a cut-off response cannot be detected");
          }
          JsonToken arr = parser.nextToken();
          if (arr == JsonToken.START_ARRAY) {
            return true;
          }
          // data present but null/empty object — no rows.
          if (arr == JsonToken.VALUE_NULL) {
            closeQuietly();
            return false;
          }
          parser.skipChildren();
        } else if ("count".equals(field)) {
          parser.nextToken();
          count = parser.getLongValue();
        } else {
          parser.nextToken();
          if (("statusCode".equals(field) || "message".equals(field) || "error".equals(field))
              && parser.currentToken().isScalarValue()) {
            String v = parser.getValueAsString();
            if (v != null && !v.isEmpty() && !"200".equals(v)) {
              errorMessage = field + "=" + v;
            }
          } else if (parser.currentToken().isStructStart()) {
            parser.skipChildren();
          }
        }
      }
      closeQuietly();
      String dims = context.getDimensionValues().isEmpty()
          ? "" : " [dimensions: " + context.getDimensionValues() + "]";
      if (errorMessage != null) {
        throw new RuntimeException("Comtrade API error: " + errorMessage + dims);
      }
      LOGGER.debug("Comtrade: no data array for {}{}", safeUrl, dims);
      return false;
    }
  }

  private static Double toDouble(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) v).doubleValue();
    }
    String s = v.toString().trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(s);
    // fallback-guard: allow per-field value parser while mapping a single row's columns; null marks that one value unparseable
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String toStr(Object v) {
    if (v == null) {
      return null;
    }
    String s = v.toString().trim();
    return s.isEmpty() ? null : s;
  }

  /** Lazy iterator over the {@code data} array; maps each Comtrade record to a clean row. */
  private static final class RecordIterator implements Iterator<Map<String, Object>> {
    private final Fetch fetch;
    private Map<String, Object> pending;
    private boolean closed;
    private long rowCount;

    RecordIterator(Fetch fetch) {
      this.fetch = fetch;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      while (true) {
        try {
          JsonToken t = fetch.parser.nextToken();
          if (t == JsonToken.START_OBJECT) {
            Map<String, Object> rec = MAPPER.readValue(fetch.parser, Map.class);
            pending = map(rec);
          } else {
            pending = null;
            finish();
          }
          return;
        } catch (IOException e) {
          // The body is already complete on local disk, so this is malformed JSON, not a
          // transport fault: retrying would fetch the same bytes.
          finish();
          throw new RuntimeException("Comtrade: unreadable response for "
              + SecretRedaction.redact(fetch.url) + " after " + rowCount + " rows", e);
        }
      }
    }

    private Map<String, Object> map(Map<String, Object> rec) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("reporter_code", toStr(rec.get("reporterISO")));

      String partnerCode = toStr(rec.get("partnerCode"));
      String partnerIso = toStr(rec.get("partnerISO"));
      boolean world = "0".equals(partnerCode);
      row.put("partner_code", world ? "WLD" : partnerIso);

      String flowDesc = toStr(rec.get("flowDesc"));
      if (flowDesc == null) {
        flowDesc = flowFromCode(toStr(rec.get("flowCode")));
      }
      row.put("flow_desc", flowDesc);

      String cmd = toStr(rec.get("cmdCode"));
      row.put("hs6", cmd);
      row.put("hs2", cmd != null && cmd.length() >= 2 ? cmd.substring(0, 2) : null);
      row.put("hs_description", toStr(rec.get("cmdDesc")));
      row.put("hs_revision", toStr(rec.get("classificationCode")));

      row.put("trade_value_usd", toDouble(rec.get("primaryValue")));
      row.put("net_weight_kg", toDouble(rec.get("netWgt")));
      row.put("quantity", toDouble(rec.get("qty")));
      row.put("quantity_unit", toStr(rec.get("qtyUnitAbbr")));

      Object agg = rec.get("isAggregate");
      boolean isAggregate = world
          || (agg instanceof Boolean && (Boolean) agg)
          || partnerIso == null;
      row.put("is_aggregate", isAggregate);
      return row;
    }

    private static String flowFromCode(String code) {
      if (code == null) {
        return null;
      }
      if ("M".equals(code)) {
        return "Import";
      }
      if ("X".equals(code)) {
        return "Export";
      }
      if ("RM".equals(code)) {
        return "Re-import";
      }
      if ("RX".equals(code)) {
        return "Re-export";
      }
      return code;
    }

    private void finish() {
      if (closed) {
        return;
      }
      closed = true;
      fetch.closeQuietly();
      LOGGER.debug("Comtrade: streamed {} records from {}", rowCount,
          SecretRedaction.redact(fetch.url));
    }

    @Override public boolean hasNext() {
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      rowCount++;
      advance();
      return row;
    }
  }
}
