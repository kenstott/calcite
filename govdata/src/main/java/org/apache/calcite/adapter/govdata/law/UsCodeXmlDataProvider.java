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
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * DataProvider for the United States Code, sourced from the Office of the Law Revision
 * Counsel's (OLRC) USLM XML release points at uscode.house.gov.
 *
 * <p>One fetch is one title: it downloads {@code xml_usc{title}@{congress}-{release}.zip} for
 * the {@code title} dimension value (e.g. {@code 05}, or {@code 05a} for Title 5 Appendix) and
 * streams the single XML file inside it with StAX. Rows are produced lazily, one top-level
 * {@code <section>} per {@link Iterator#next()}; neither the XML nor the rows are ever held
 * whole in memory.
 *
 * <p>The current release point ({@code congress}-{@code release}, e.g. {@code 119-111}) is
 * scraped once per provider instance from the {@code xml_uscAll} link on the OLRC download
 * page and then pinned, so every title fetched by one run comes from the same release point
 * even if OLRC publishes a new one mid-run.
 *
 * <p>Statutory notes ({@code <notes>}/{@code <note>}) and tables of contents are not part of
 * a section's operative text and are skipped; the section's enactment history
 * ({@code <sourceCredit>}) is captured separately.
 */
public class UsCodeXmlDataProvider implements StorageAwareDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsCodeXmlDataProvider.class);

  private static final String RELEASE_PAGE_URL = "https://uscode.house.gov/download/download.shtml";

  @SuppressWarnings("InlineFormatString")
  private static final String ZIP_URL_TEMPLATE =
      "https://uscode.house.gov/download/releasepoints/us/pl/%s/%s/xml_usc%s@%s-%s.zip";

  private static final Pattern RELEASE_POINT_PATTERN =
      Pattern.compile("releasepoints/us/pl/(\\d+)/([^/\"]+)/xml_uscAll@");

  private static final String USLM_NS = "http://xml.house.gov/schemas/uslm/1.0";

  /** Elements whose whole subtree is excluded from section text. */
  private static final Set<String> SKIP_ELEMENTS =
      new HashSet<String>(Arrays.asList("notes", "note", "toc"));

  /** Structural containers of sections; tracked to derive each section's hierarchy path. The
   *  outermost one is the title (or title appendix) itself. */
  private static final Set<String> HIERARCHY_ELEMENTS =
      new HashSet<String>(
          Arrays.asList("title", "appendix", "subtitle", "part", "subpart", "chapter",
              "subchapter"));

  /** Elements that start a new line in section text. */
  private static final Set<String> BLOCK_ELEMENTS =
      new HashSet<String>(
          Arrays.asList("subsection", "paragraph", "subparagraph", "clause", "subclause",
              "item", "subitem", "chapeau", "continuation", "p", "tr", "li"));

  /** Inline label elements followed by a space so "(a)" does not fuse with the text after it. */
  private static final Set<String> INLINE_LABEL_ELEMENTS =
      new HashSet<String>(Arrays.asList("num", "heading", "td", "th"));

  private static final Pattern WHITESPACE = Pattern.compile("[\\s\\u00a0\\u2007\\u2009\\u202f]+");

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  private String congress;
  private String release;

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
    String title = variables.get("title");
    if (title == null || title.isEmpty()) {
      throw new IOException("usc_sections: the 'title' dimension is required");
    }
    resolveReleasePoint();

    String releasePoint = congress + "-" + release;
    String url = String.format(Locale.US, ZIP_URL_TEMPLATE, congress, release, title, congress,
        release);
    String cachePath = storageProvider().resolvePath(cacheBaseDir,
        "law/usc/release=" + releasePoint + "/title=" + title);

    File tempDir = ZipDownloadUtils.downloadZipToTempDirCached(url, null, "us-code", cachePath,
        storageProvider());
    File[] xmlFiles = tempDir.listFiles((d, n) -> n.toLowerCase(Locale.US).endsWith(".xml"));
    if (xmlFiles == null || xmlFiles.length != 1) {
      ZipDownloadUtils.deleteDirectory(tempDir);
      throw new IOException("usc_sections: expected exactly one XML file in " + url + " but found "
          + (xmlFiles == null ? 0 : xmlFiles.length));
    }

    LOGGER.info("usc_sections: streaming title {} from release point {}", title, releasePoint);
    return streamSections(xmlFiles[0], tempDir, releasePoint);
  }

  /** Streams the sections of one title's USLM XML file; deletes {@code tempDir} once the
   *  stream is exhausted or fails. Package-private so tests can drive the parser directly. */
  static Iterator<Map<String, Object>> streamSections(File xmlFile, File tempDir,
      String releasePoint) throws IOException {
    return new SectionIterator(xmlFile, tempDir, releasePoint);
  }

  /** Reads the OLRC download page line by line straight off the HTTP response and pins the
   *  current release point. Not routed through {@link ZipDownloadUtils#downloadToFile}: that
   *  helper rejects text/html responses by design (WAF-block detection), and this page is HTML. */
  private synchronized void resolveReleasePoint() throws IOException {
    if (release != null) {
      return;
    }
    HttpURLConnection conn = (HttpURLConnection) URI.create(RELEASE_PAGE_URL).toURL()
        .openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData/1.0");
    try {
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("usc_sections: HTTP " + status + " from " + RELEASE_PAGE_URL);
      }
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          Matcher m = RELEASE_POINT_PATTERN.matcher(line);
          if (m.find()) {
            congress = m.group(1);
            release = m.group(2);
            LOGGER.info("usc_sections: current release point is {}-{}", congress, release);
            return;
          }
        }
      }
    } finally {
      conn.disconnect();
    }
    throw new IOException("usc_sections: no xml_uscAll release point link found on "
        + RELEASE_PAGE_URL);
  }

  /** Which text buffer element characters are currently routed to. */
  private enum Capture {
    NONE, DOC_NUMBER, POSITIVE_LAW, FRAME_NUM, FRAME_HEADING, SECTION_HEADING, SECTION_NUM,
    SOURCE_CREDIT
  }

  /** One open structural container (title/subtitle/part/subpart/chapter/subchapter). */
  private static final class Frame {
    final String type;
    final int depth;
    String numValue;
    final StringBuilder numText = new StringBuilder();
    final StringBuilder heading = new StringBuilder();

    Frame(String type, int depth) {
      this.type = type;
      this.depth = depth;
    }

    String label() {
      if (numValue != null && !numValue.isEmpty()) {
        return numValue;
      }
      return collapse(numText);
    }
  }

  /**
   * Lazily walks one title's XML and emits one row per top-level section. Closes the parser,
   * the file stream and deletes the extracted temp directory as soon as the stream is
   * exhausted or fails.
   */
  private static final class SectionIterator implements Iterator<Map<String, Object>> {
    private final File tempDir;
    private final String releasePoint;
    private final InputStream in;
    private final XMLStreamReader reader;

    private Map<String, Object> pending;
    private boolean done;

    private int depth;
    private int skipUntilDepth;

    private String titleNumber;
    private Boolean positiveLaw;
    private final StringBuilder docNumberText = new StringBuilder();
    private final StringBuilder positiveLawText = new StringBuilder();

    private final Deque<Frame> frames = new ArrayDeque<Frame>();
    private Frame titleFrame;

    private Capture capture = Capture.NONE;
    private int captureDepth;
    private Frame captureFrame;

    private boolean inSection;
    private int sectionDepth;
    private String sectionIdentifier;
    private String sectionStatus;
    private String sectionNumber;
    private final StringBuilder sectionNumText = new StringBuilder();
    private final StringBuilder sectionHeading = new StringBuilder();
    private final StringBuilder sectionBody = new StringBuilder();
    private final StringBuilder sourceCredit = new StringBuilder();

    /** Occurrences seen per section number within this title -- disambiguates the handful of
     *  sections the Code itself numbers twice (e.g. 5 U.S.C. 3598). */
    private final Map<String, Integer> sectionSeq = new HashMap<String, Integer>();

    SectionIterator(File xmlFile, File tempDir, String releasePoint) throws IOException {
      this.tempDir = tempDir;
      this.releasePoint = releasePoint;
      try {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        this.in = new FileInputStream(xmlFile);
        this.reader = factory.createXMLStreamReader(in, "UTF-8");
      } catch (XMLStreamException e) {
        ZipDownloadUtils.deleteDirectory(tempDir);
        throw new IOException("usc_sections: cannot open XML stream for " + xmlFile, e);
      }
    }

    @Override public boolean hasNext() {
      if (pending == null && !done) {
        try {
          pending = advance();
        } catch (XMLStreamException e) {
          close();
          throw new UncheckedIOException(new IOException("usc_sections: XML parse failure", e));
        } catch (IOException e) {
          close();
          throw new UncheckedIOException(e);
        }
        if (pending == null) {
          close();
        }
      }
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      pending = null;
      return row;
    }

    private void close() {
      done = true;
      try {
        reader.close();
      } catch (XMLStreamException e) {
        LOGGER.warn("usc_sections: error closing XML reader: {}", e.getMessage());
      }
      try {
        in.close();
      } catch (IOException e) {
        LOGGER.warn("usc_sections: error closing XML stream: {}", e.getMessage());
      }
      ZipDownloadUtils.deleteDirectory(tempDir);
    }

    /** Parses forward to the end of the next top-level section; null when the stream ends. */
    private Map<String, Object> advance() throws XMLStreamException, IOException {
      while (reader.hasNext()) {
        int event = reader.next();
        if (event == XMLStreamConstants.START_ELEMENT) {
          onStart();
        } else if (event == XMLStreamConstants.CHARACTERS) {
          onText(reader.getText());
        } else if (event == XMLStreamConstants.END_ELEMENT) {
          Map<String, Object> row = onEnd();
          if (row != null) {
            return row;
          }
        }
      }
      return null;
    }

    private void onStart() {
      depth++;
      if (skipUntilDepth != 0) {
        return;
      }
      String name = reader.getLocalName();
      boolean uslm = USLM_NS.equals(reader.getNamespaceURI());

      if (uslm && SKIP_ELEMENTS.contains(name)) {
        skipUntilDepth = depth;
        return;
      }

      if (!inSection) {
        if (uslm && "docNumber".equals(name)) {
          startCapture(Capture.DOC_NUMBER, null);
        } else if (uslm && "property".equals(name)
            && "is-positive-law".equals(reader.getAttributeValue(null, "role"))) {
          startCapture(Capture.POSITIVE_LAW, null);
        } else if (uslm && HIERARCHY_ELEMENTS.contains(name)) {
          Frame frame = new Frame(name, depth);
          frames.push(frame);
          // The outermost container is the title itself (<title>, or <appendix> for the
          // Title N Appendix files); anything nested below it is hierarchy.
          if (frames.size() == 1) {
            titleFrame = frame;
          }
        } else if (uslm && !frames.isEmpty() && frames.peek().depth + 1 == depth) {
          if ("num".equals(name)) {
            frames.peek().numValue = reader.getAttributeValue(null, "value");
            startCapture(Capture.FRAME_NUM, frames.peek());
          } else if ("heading".equals(name)) {
            startCapture(Capture.FRAME_HEADING, frames.peek());
          }
        }
        if (uslm && "section".equals(name)) {
          inSection = true;
          sectionDepth = depth;
          sectionIdentifier = reader.getAttributeValue(null, "identifier");
          sectionStatus = reader.getAttributeValue(null, "status");
          sectionNumber = null;
          sectionNumText.setLength(0);
          sectionHeading.setLength(0);
          sectionBody.setLength(0);
          sourceCredit.setLength(0);
        }
        return;
      }

      // Inside a top-level section.
      if (uslm && depth == sectionDepth + 1) {
        if ("num".equals(name)) {
          sectionNumber = reader.getAttributeValue(null, "value");
          startCapture(Capture.SECTION_NUM, null);
          return;
        }
        if ("heading".equals(name)) {
          startCapture(Capture.SECTION_HEADING, null);
          return;
        }
        if ("sourceCredit".equals(name)) {
          startCapture(Capture.SOURCE_CREDIT, null);
          return;
        }
      }
      if (capture == Capture.NONE && BLOCK_ELEMENTS.contains(name)) {
        sectionBody.append('\n');
      }
    }

    private void startCapture(Capture target, Frame frame) {
      capture = target;
      captureDepth = depth;
      captureFrame = frame;
    }

    private void onText(String text) {
      if (skipUntilDepth != 0) {
        return;
      }
      switch (capture) {
      case DOC_NUMBER:
        docNumberText.append(text);
        return;
      case POSITIVE_LAW:
        positiveLawText.append(text);
        return;
      case FRAME_NUM:
        captureFrame.numText.append(text);
        return;
      case FRAME_HEADING:
        captureFrame.heading.append(text);
        return;
      case SECTION_NUM:
        sectionNumText.append(text);
        return;
      case SECTION_HEADING:
        sectionHeading.append(text);
        return;
      case SOURCE_CREDIT:
        sourceCredit.append(text);
        return;
      default:
        if (inSection) {
          sectionBody.append(text);
        }
      }
    }

    private Map<String, Object> onEnd() throws IOException {
      try {
        if (skipUntilDepth != 0) {
          if (depth == skipUntilDepth) {
            skipUntilDepth = 0;
          }
          return null;
        }
        String name = reader.getLocalName();

        if (capture != Capture.NONE && depth == captureDepth) {
          finishCapture();
          return null;
        }

        if (inSection) {
          if (depth == sectionDepth) {
            inSection = false;
            return buildRow();
          }
          if (INLINE_LABEL_ELEMENTS.contains(name)) {
            sectionBody.append(' ');
          }
          return null;
        }

        if (!frames.isEmpty() && frames.peek().depth == depth) {
          Frame closed = frames.pop();
          if (closed == titleFrame) {
            // Title container closed: nothing further to emit for this file.
            titleFrame = null;
          }
        }
        return null;
      } finally {
        depth--;
      }
    }

    private void finishCapture() throws IOException {
      if (capture == Capture.DOC_NUMBER) {
        titleNumber = collapse(docNumberText);
      } else if (capture == Capture.POSITIVE_LAW) {
        String value = collapse(positiveLawText);
        if ("yes".equals(value)) {
          positiveLaw = Boolean.TRUE;
        } else if ("no".equals(value)) {
          positiveLaw = Boolean.FALSE;
        } else {
          throw new IOException("usc_sections: unexpected is-positive-law value '" + value + "'");
        }
      }
      capture = Capture.NONE;
      captureFrame = null;
    }

    private Map<String, Object> buildRow() throws IOException {
      if (titleNumber == null || positiveLaw == null || titleName() == null) {
        throw new IOException("usc_sections: section reached before title metadata "
            + "(docNumber/is-positive-law/title heading) was read");
      }
      if (sectionNumber == null || sectionNumber.isEmpty()) {
        throw new IOException("usc_sections: section without a num value in title " + titleNumber
            + " (identifier=" + sectionIdentifier + ")");
      }

      Integer seen = sectionSeq.get(sectionNumber);
      int seq = seen == null ? 1 : seen + 1;
      sectionSeq.put(sectionNumber, seq);

      String heading = collapse(sectionHeading);
      boolean appendix = Character.isLetter(titleNumber.charAt(titleNumber.length() - 1));
      String citation = titleNumber.toUpperCase(Locale.US) + " U.S.C. "
          + (appendix ? "App. " : "") + "§ " + sectionNumber;
      String body = normalizeBody(sectionBody);

      String headerLine = heading.isEmpty() ? citation : citation + " — " + heading;

      Frame chapter = null;
      StringBuilder hierarchy = new StringBuilder();
      // frames is a stack (head = innermost); walk outermost-first, skipping the title itself.
      Iterator<Frame> outerToInner = frames.descendingIterator();
      while (outerToInner.hasNext()) {
        Frame f = outerToInner.next();
        if (f == titleFrame) {
          continue;
        }
        if ("chapter".equals(f.type)) {
          chapter = f;
        }
        if (hierarchy.length() > 0) {
          hierarchy.append(" > ");
        }
        hierarchy.append(capitalize(f.type)).append(' ').append(f.label());
        String h = collapse(f.heading);
        if (!h.isEmpty()) {
          hierarchy.append(" — ").append(h);
        }
      }

      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("title_number", titleNumber);
      row.put("title_name", titleName());
      row.put("is_positive_law", positiveLaw);
      row.put("release_point", releasePoint);
      row.put("section_number", sectionNumber);
      row.put("section_seq", seq);
      row.put("usc_identifier", sectionIdentifier);
      row.put("citation", citation);
      row.put("heading", heading.isEmpty() ? null : heading);
      row.put("status", sectionStatus);
      row.put("hierarchy", hierarchy.length() == 0 ? null : hierarchy.toString());
      row.put("chapter_number", chapter == null ? null : chapter.label());
      row.put("chapter_heading", chapter == null ? null : emptyToNull(collapse(chapter.heading)));
      // A section with no operative text (repealed/omitted/transferred) carries only its
      // header line; leave the blob null so it is not chunked and embedded as a stub.
      row.put("section_text", body.isEmpty() ? null : headerLine + "\n" + body);
      row.put("source_credit", emptyToNull(collapse(sourceCredit)));
      return row;
    }

    private String titleName() {
      return titleFrame == null ? null : emptyToNull(collapse(titleFrame.heading));
    }
  }

  /** Collapses every whitespace run (including the non-breaking spaces OLRC uses) to one space. */
  private static String collapse(CharSequence text) {
    return WHITESPACE.matcher(text).replaceAll(" ").trim();
  }

  /** Per-line whitespace collapse; drops blank lines and keeps the block-level line breaks. */
  private static String normalizeBody(CharSequence raw) {
    StringBuilder out = new StringBuilder(raw.length());
    for (String line : raw.toString().split("\n", -1)) {
      String collapsed = collapse(line);
      if (collapsed.isEmpty()) {
        continue;
      }
      if (out.length() > 0) {
        out.append('\n');
      }
      out.append(collapsed);
    }
    return out.toString();
  }

  private static String capitalize(String s) {
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  private static String emptyToNull(String s) {
    return s.isEmpty() ? null : s;
  }
}
