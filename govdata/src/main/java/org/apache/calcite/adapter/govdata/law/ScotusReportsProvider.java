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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

/**
 * DataProvider for {@code scotus_reports_cases}: every case in the bound volumes of the United
 * States Reports on GovInfo (volumes 2 through 583, terms 1781 through 2017).
 *
 * <p>The {@code year} dimension is the Court term. A term's volumes are found from GovInfo's
 * {@code courtTerm} for each volume (see {@link ScotusVolumeTerms}); the volume-to-term map is
 * built once from the collection listing and one package summary per volume, and kept in the
 * cache so later runs fetch summaries only for volumes that are new.
 *
 * <p>Each case is one row built from two GovInfo resources: the granule's MODS record, whose
 * case-level fields are read by {@link ScotusModsParser}, and its PDF, read a page at a time
 * through {@link PdfPageTexts}. The docket numbers come from the opinion's first pages
 * ({@link ScotusDocketNumbers}). The disposition is GovInfo's curated value where the MODS
 * carries one (volumes through 582); otherwise it is derived from the opinion text by
 * {@link ScotusDispositionParser}, and {@code disposition_source} says which.
 *
 * <p>Streaming: volumes, granule pages and cases are walked lazily, one case at a time. A case
 * holds its own opinion text as the row's {@code opinion_text}, and nothing else is retained.
 *
 * <p>The API key is read from the {@code X-Api-Key} header of the YAML source block.
 */
public class ScotusReportsProvider implements StorageAwareDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScotusReportsProvider.class);

  static final String TABLE = "scotus_reports_cases";

  private static final String API = "https://api.govinfo.gov";
  private static final String CONTENT = "https://www.govinfo.gov/content/pkg";
  private static final String API_KEY_HEADER = "X-Api-Key";
  private static final String TERM_CACHE = "scotus/usreports/volume_terms.json";
  private static final int GRANULE_PAGE_SIZE = 100;
  private static final int COLLECTION_PAGE_SIZE = 1000;

  private static final Pattern PACKAGE_VOLUME = Pattern.compile("^USREPORTS-(\\d+)$");

  private static final ObjectMapper JSON = new ObjectMapper();

  private StorageProvider storageProvider;
  private String cacheBaseDir;
  private ScotusVolumeTerms terms;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("ScotusReportsProvider does not serve table '"
          + config.getName() + "'");
    }
    String yearValue = variables.get("year");
    if (yearValue == null || yearValue.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' (Court term) dimension is required");
    }
    int term = Integer.parseInt(yearValue);
    Map<String, String> headers = config.getSource() == null ? null
        : config.getSource().getHeaders();
    if (headers == null || !headers.containsKey(API_KEY_HEADER)) {
      throw new IOException(TABLE + ": source.headers must set " + API_KEY_HEADER);
    }

    List<Integer> volumes = volumeTerms(headers).volumesFor(term);
    LOGGER.info("{}: term {} has {} volume(s) {}", TABLE, term, volumes.size(), volumes);
    return new CaseIterator(term, volumes, headers);
  }

  // ---------------------------------------------------------------------------------------
  // Volume-to-term map
  // ---------------------------------------------------------------------------------------

  private ScotusVolumeTerms volumeTerms(Map<String, String> headers) throws IOException {
    if (terms != null) {
      return terms;
    }
    StorageProvider sp = storageProvider();
    String cachePath = sp.resolvePath(cacheBaseDir, TERM_CACHE);

    Map<Integer, String> courtTerms = new TreeMap<Integer, String>();
    if (sp.exists(cachePath)) {
      try (InputStream in = sp.openInputStream(cachePath)) {
        JsonNode cached = JSON.readTree(in);
        Iterator<String> volumeNumbers = cached.fieldNames();
        while (volumeNumbers.hasNext()) {
          String volumeNumber = volumeNumbers.next();
          courtTerms.put(Integer.valueOf(volumeNumber), cached.get(volumeNumber).asText());
        }
      }
    }

    boolean changed = false;
    for (int volume : listVolumes(headers)) {
      if (!courtTerms.containsKey(volume)) {
        JsonNode summary = getJson(API + "/packages/USREPORTS-" + volume + "/summary", headers);
        JsonNode term = summary.get("courtTerm");
        if (term == null || term.isNull() || term.asText().isEmpty()) {
          throw new IOException("GovInfo package USREPORTS-" + volume + " has no courtTerm");
        }
        courtTerms.put(volume, term.asText());
        changed = true;
      }
    }
    if (changed) {
      Map<String, String> out = new LinkedHashMap<String, String>();
      for (Map.Entry<Integer, String> e : courtTerms.entrySet()) {
        out.put(String.valueOf(e.getKey()), e.getValue());
      }
      byte[] bytes = JSON.writeValueAsBytes(out);
      sp.writeFile(cachePath, new ByteArrayInputStream(bytes));
      LOGGER.info("{}: volume-to-term map now covers {} volumes", TABLE, courtTerms.size());
    }
    terms = new ScotusVolumeTerms(courtTerms);
    return terms;
  }

  /** Every USREPORTS package volume, from the collection listing. */
  private List<Integer> listVolumes(Map<String, String> headers) throws IOException {
    List<Integer> volumes = new ArrayList<Integer>();
    String url = API + "/collections/USREPORTS/1700-01-01T00:00:00Z?offsetMark=*&pageSize="
        + COLLECTION_PAGE_SIZE;
    while (url != null) {
      JsonNode page = getJson(url, headers);
      for (JsonNode pkg : page.path("packages")) {
        Matcher m = PACKAGE_VOLUME.matcher(pkg.path("packageId").asText());
        if (!m.matches()) {
          throw new IOException("Unexpected USREPORTS package id: " + pkg.path("packageId"));
        }
        volumes.add(Integer.valueOf(m.group(1)));
      }
      url = nextPage(page);
    }
    return volumes;
  }

  private static String nextPage(JsonNode page) {
    JsonNode next = page.get("nextPage");
    return next == null || next.isNull() || next.asText().isEmpty() ? null : next.asText();
  }

  // ---------------------------------------------------------------------------------------
  // HTTP
  // ---------------------------------------------------------------------------------------

  private static JsonNode getJson(String url, Map<String, String> headers) throws IOException {
    File file = File.createTempFile("scotus-json-", ".json");
    try {
      ZipDownloadUtils.downloadToFile(url, headers, file);
      try (InputStream in = new FileInputStream(file)) {
        return JSON.readTree(in);
      }
    } finally {
      Files.deleteIfExists(file.toPath());
    }
  }

  // ---------------------------------------------------------------------------------------
  // Rows
  // ---------------------------------------------------------------------------------------

  /** Walks the cases of the given volumes, granule page by granule page. */
  private final class CaseIterator implements Iterator<Map<String, Object>> {
    private final int term;
    private final Deque<Integer> volumes;
    private final Map<String, String> headers;
    private final Deque<String> granules = new ArrayDeque<String>();
    private int volume = -1;
    private String nextGranulePage;

    CaseIterator(int term, List<Integer> volumes, Map<String, String> headers) {
      this.term = term;
      this.volumes = new ArrayDeque<Integer>(volumes);
      this.headers = headers;
    }

    @Override public boolean hasNext() {
      try {
        while (granules.isEmpty()) {
          if (nextGranulePage == null) {
            if (volumes.isEmpty()) {
              return false;
            }
            volume = volumes.removeFirst();
            nextGranulePage = API + "/packages/USREPORTS-" + volume
                + "/granules?offsetMark=*&pageSize=" + GRANULE_PAGE_SIZE;
            LOGGER.info("{}: term {} volume {}", TABLE, term, volume);
          }
          JsonNode page = getJson(nextGranulePage, headers);
          for (JsonNode granule : page.path("granules")) {
            if ("CASE".equals(granule.path("granuleClass").asText())) {
              granules.addLast(granule.path("granuleId").asText());
            }
          }
          nextGranulePage = nextPage(page);
        }
        return true;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      String granuleId = granules.removeFirst();
      try {
        return buildRow(volume, granuleId, headers);
      } catch (IOException | XMLStreamException e) {
        throw new GovDataException("Failed to read " + granuleId + " (volume " + volume + ")", e);
      }
    }
  }

  private static Map<String, Object> buildRow(int volume, String granuleId,
      Map<String, String> headers) throws IOException, XMLStreamException {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("volume", Integer.valueOf(volume));

    File modsFile = File.createTempFile("scotus-mods-", ".xml");
    try {
      ZipDownloadUtils.downloadToFile(
          API + "/packages/USREPORTS-" + volume + "/granules/" + granuleId + "/mods",
          headers, modsFile);
      try (InputStream in = new FileInputStream(modsFile)) {
        row.putAll(ScotusModsParser.parse(in));
      }
    } finally {
      Files.deleteIfExists(modsFile.toPath());
    }

    String pdfUrl = CONTENT + "/USREPORTS-" + volume + "/pdf/" + granuleId + ".pdf";
    final List<String> firstPages = new ArrayList<String>(ScotusDispositionParser.MAX_PAGES);
    final StringBuilder text = new StringBuilder();
    File pdf = File.createTempFile("scotus-pdf-", ".pdf");
    int pageCount;
    try {
      // The PDF is served from the content host, which needs no key; the key is not sent to it.
      ZipDownloadUtils.downloadToFile(pdfUrl, null, pdf);
      pageCount = PdfPageTexts.forEachPage(pdf, new PdfPageTexts.PageSink() {
        @Override public void page(int pageNumber, String pageText) {
          if (firstPages.size() < ScotusDispositionParser.MAX_PAGES) {
            firstPages.add(pageText);
          }
          text.append(pageText);
        }
      });
    } finally {
      Files.deleteIfExists(pdf.toPath());
    }

    row.put("docket_numbers", ScotusDocketNumbers.parse(firstPages));
    outcome(row, ScotusDispositionParser.parse(firstPages));
    row.put("pdf_url", pdfUrl);
    row.put("page_count", Integer.valueOf(pageCount));
    row.put("opinion_text", text.toString());
    return row;
  }

  /**
   * GovInfo's curated disposition wins where the MODS carries one; otherwise the value derived
   * from the opinion text is used and labelled as such. A case with neither has no disposition.
   */
  private static void outcome(Map<String, Object> row, ScotusDispositionParser.Result derived) {
    if (row.containsKey("disposition")) {
      row.put("disposition_source", "govinfo_mods");
      return;
    }
    if (derived.kind != ScotusDispositionParser.Kind.NONE) {
      row.put("disposition", derived.disposition);
      row.put("disposition_source", "opinion_text");
      if (!row.containsKey("decision_type") && derived.decisionType != null) {
        row.put("decision_type", derived.decisionType);
      }
    }
  }
}
