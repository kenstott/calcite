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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * DataProvider for the full text of Federal Register documents, sourced from the same govinfo.gov
 * monthly bulk XML archives as {@link FedRegisterBulkXmlDataProvider}.
 *
 * <p>Emits one row per document: {@code document_number}, {@code summary} (the preamble
 * {@code SUM} element) and {@code body_text} (the {@code SUPLINF} supplementary information of a
 * rule, proposed rule or notice; the whole document text of a presidential document, which has no
 * {@code SUPLINF}). Each daily XML file is read with a StAX stream and the rows of one file at a
 * time are held, so a month's text is never resident at once. The monthly ZIP is shared with
 * fr_documents through the same raw cache path.
 */
public class FedRegisterDocumentTextDataProvider implements StorageAwareDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FedRegisterDocumentTextDataProvider.class);

  // Elements whose end closes a paragraph-level block; a blank line keeps adjacent blocks from
  // fusing into one run of words once the markup is dropped.
  private static final Set<String> BLOCK_ELEMENTS = new HashSet<String>(Arrays.asList(
      "P", "FP", "HD", "SJ", "SJDENT", "AMDPAR", "SECTNO", "SUBJECT", "ROW", "TITLE", "LI"));

  private StorageProvider storageProvider;
  private String cacheBaseDir;

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
    int year = Integer.parseInt(variables.get("year"));
    int month = Integer.parseInt(variables.get("month"));

    String url = String.format(Locale.US, FedRegisterBulkXmlDataProvider.GOVINFO_URL_TEMPLATE,
        year, month, year, month);
    String cachePath = storageProvider().resolvePath(cacheBaseDir,
        String.format(Locale.US, "fedregister/year=%d/month=%02d", year, month));

    File tempDir;
    try {
      tempDir = ZipDownloadUtils.downloadZipToTempDirCached(
          url, null, "fr-bulk-text", cachePath, storageProvider());
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("HTTP 404")) {
        LOGGER.info("FR bulk ZIP not yet available (404): {}", url);
        return Collections.emptyIterator();
      }
      throw e;
    }

    File[] xmlFiles = tempDir.listFiles(
        (d, n) -> FedRegisterBulkXmlDataProvider.FILENAME_DATE_PATTERN.matcher(n).find());
    if (xmlFiles == null || xmlFiles.length == 0) {
      ZipDownloadUtils.deleteDirectory(tempDir);
      return Collections.emptyIterator();
    }
    Arrays.sort(xmlFiles);
    return new DailyFileIterator(tempDir, xmlFiles);
  }

  /** Lazily parses one daily file at a time and removes the extracted directory once drained. */
  private static final class DailyFileIterator implements Iterator<Map<String, Object>> {
    private final File tempDir;
    private final File[] xmlFiles;
    private final Deque<Map<String, Object>> buffer = new ArrayDeque<Map<String, Object>>();
    // GPO reprints an identical document (same document_number) in a later issue of the same
    // month; document_number is the primary key, so only the first occurrence is kept.
    private final Set<String> seenDocNumbers = new HashSet<String>();
    private int nextFile;
    private boolean cleaned;

    DailyFileIterator(File tempDir, File[] xmlFiles) {
      this.tempDir = tempDir;
      this.xmlFiles = xmlFiles;
    }

    @Override public boolean hasNext() {
      while (buffer.isEmpty() && nextFile < xmlFiles.length) {
        File file = xmlFiles[nextFile++];
        try {
          parseDailyFile(file, buffer, seenDocNumbers);
        } catch (IOException | XMLStreamException e) {
          cleanup();
          throw new IllegalStateException("fr_document_text: cannot parse " + file.getName(), e);
        }
      }
      if (buffer.isEmpty()) {
        cleanup();
        return false;
      }
      return true;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return buffer.poll();
    }

    private void cleanup() {
      if (!cleaned) {
        cleaned = true;
        ZipDownloadUtils.deleteDirectory(tempDir);
      }
    }
  }

  static void parseDailyFile(File file, Deque<Map<String, Object>> out, Set<String> seen)
      throws IOException, XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

    // justification: file is an entry of the ZIP already extracted to a local temp directory
    try (InputStream in = new FileInputStream(file)) {
      XMLStreamReader xml = factory.createXMLStreamReader(in);
      try {
        readDocuments(xml, out, seen);
      } finally {
        xml.close();
      }
    }
  }

  private static String docTagFor(String containerTag) {
    for (String[] c : FedRegisterBulkXmlDataProvider.CONTAINER_TYPES) {
      if (c[0].equals(containerTag)) {
        return c[1];
      }
    }
    return null;
  }

  private static void readDocuments(XMLStreamReader xml, Deque<Map<String, Object>> out,
      Set<String> seen) throws XMLStreamException {
    String docTag = null;
    boolean presDoc = false;
    int docDepth = 0;
    StringBuilder summary = null;
    StringBuilder body = null;
    StringBuilder whole = null;
    StringBuilder frdoc = null;
    int summaryDepth = 0;
    int bodyDepth = 0;
    int frdocDepth = 0;

    while (xml.hasNext()) {
      int event = xml.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        String name = xml.getLocalName();
        if (docDepth == 0) {
          String candidate = docTagFor(name);
          if (candidate != null) {
            docTag = candidate;
            presDoc = "PRESDOCU".equals(candidate);
          } else if (docTag != null && name.equals(docTag)) {
            docDepth = 1;
            summary = new StringBuilder();
            body = new StringBuilder();
            whole = new StringBuilder();
            frdoc = new StringBuilder();
            summaryDepth = 0;
            bodyDepth = 0;
            frdocDepth = 0;
          }
          continue;
        }
        docDepth++;
        if (summaryDepth > 0) {
          summaryDepth++;
        } else if ("SUM".equals(name)) {
          summaryDepth = 1;
        }
        if (bodyDepth > 0) {
          bodyDepth++;
        } else if ("SUPLINF".equals(name)) {
          bodyDepth = 1;
        }
        if (frdocDepth > 0) {
          frdocDepth++;
        } else if ("FRDOC".equals(name) && frdoc.length() == 0) {
          frdocDepth = 1;
        }
      } else if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA
          || event == XMLStreamConstants.SPACE) {
        if (docDepth == 0) {
          continue;
        }
        String text = xml.getText();
        if (summaryDepth > 0) {
          summary.append(text);
        }
        if (bodyDepth > 0) {
          body.append(text);
        }
        if (frdocDepth > 0) {
          frdoc.append(text);
        }
        if (presDoc) {
          whole.append(text);
        }
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        if (docDepth == 0) {
          continue;
        }
        String name = xml.getLocalName();
        boolean block = BLOCK_ELEMENTS.contains(name);
        if (block) {
          if (summaryDepth > 0) {
            summary.append("\n\n");
          }
          if (bodyDepth > 0) {
            body.append("\n\n");
          }
          if (presDoc) {
            whole.append("\n\n");
          }
        }
        if (summaryDepth > 0) {
          summaryDepth--;
        }
        if (bodyDepth > 0) {
          bodyDepth--;
        }
        if (frdocDepth > 0) {
          frdocDepth--;
        }
        docDepth--;
        if (docDepth == 0) {
          addRow(out, seen, frdoc.toString(), summary.toString(),
              presDoc ? stripFrdoc(whole.toString(), frdoc.toString()) : body.toString());
        }
      }
    }
  }

  private static String stripFrdoc(String text, String frdoc) {
    int i = frdoc.isEmpty() ? -1 : text.indexOf(frdoc);
    return i < 0 ? text : text.substring(0, i) + text.substring(i + frdoc.length());
  }

  private static void addRow(Deque<Map<String, Object>> out, Set<String> seen, String frdocText,
      String summary, String body) {
    String docNumber = FedRegisterBulkXmlDataProvider.extractDocNumber(frdocText);
    if (docNumber == null) {
      return;
    }
    if (!seen.add(docNumber)) {
      LOGGER.warn("fr_document_text: dropping reprint of document_number={}", docNumber);
      return;
    }
    String summaryText = normalize(summary).replaceFirst("(?i)^SUMMARY:\\s*", "");
    String bodyText = normalize(body).replaceFirst("(?i)^SUPPLEMENTARY INFORMATION:\\s*", "");
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("document_number", docNumber);
    row.put("summary", summaryText.isEmpty() ? null : summaryText);
    row.put("body_text", bodyText.isEmpty() ? null : bodyText);
    out.add(row);
  }

  // Collapses the whitespace the pretty-printed XML leaves inside a run of text, keeping the
  // blank line that separates blocks.
  static String normalize(String text) {
    String t = text.replace(' ', ' ').replaceAll("[ \\t\\x0B\\f\\r]+", " ");
    t = t.replaceAll(" ?\\n ?", "\n").replaceAll("\\n{3,}", "\n\n");
    // single line breaks inside a block are source pretty-printing, not structure
    t = t.replaceAll("(?<!\\n)\\n(?!\\n)", " ");
    return t.trim();
  }
}
