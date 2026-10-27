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
package org.apache.calcite.adapter.govdata.sec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One complete submission from EDGAR's daily feed: the {@code .nc} entry of a day's tar.
 *
 * <p>The entry starts with an SGML header ({@code <ACCESSION-NUMBER>}, {@code <TYPE>},
 * {@code <FILING-DATE>} and the filer blocks), followed by one {@code <DOCUMENT>} per file:
 * {@code <TYPE>}, {@code <SEQUENCE>}, {@code <FILENAME>}, {@code <DESCRIPTION>} and the content
 * between {@code <TEXT>} and {@code </TEXT>}. Text documents sit inline (XML wrapped in
 * {@code <XML>}); binaries (images, spreadsheets, zips, PDFs) are uuencoded and are reported as
 * binary with no content, because nothing downstream reads them.
 *
 * <p>The bytes are decoded as ISO-8859-1, one char per byte, so a document's content is returned
 * byte-for-byte as it was filed whatever its charset.
 */
final class NcSubmission {

  /** One file of the submission. {@code content} is null for a binary document. */
  static final class Doc {
    final String type;
    final int sequence;
    final String filename;
    final byte[] content;

    Doc(String type, int sequence, String filename, byte[] content) {
      this.type = type;
      this.sequence = sequence;
      this.filename = filename;
      this.content = content;
    }

    boolean isBinary() {
      return content == null;
    }
  }

  private static final Pattern ACCESSION = Pattern.compile("<ACCESSION-NUMBER>\\s*([0-9-]+)");
  private static final Pattern HEADER_TYPE = Pattern.compile("<TYPE>\\s*([^\\r\\n<]+)");
  private static final Pattern FILING_DATE = Pattern.compile("<FILING-DATE>\\s*(\\d{8})");
  private static final Pattern DOC_FIELD = Pattern.compile("<(TYPE|SEQUENCE|FILENAME)>\\s*([^\\r\\n<]*)");
  private static final Pattern UUENCODED = Pattern.compile("^\\s*begin\\s+\\d{3}\\s+\\S+");

  final String accession;
  final String formType;
  final String filingDate;
  /** The SGML header, everything before the first document: the {@code .hdr.sgml} of the filing. */
  final byte[] header;
  final List<Doc> docs;

  private NcSubmission(String accession, String formType, String filingDate, byte[] header,
      List<Doc> docs) {
    this.accession = accession;
    this.formType = formType;
    this.filingDate = filingDate;
    this.header = header;
    this.docs = docs;
  }

  /** The accession without dashes, as it appears in an EDGAR archive path. */
  String accessionNoDash() {
    return accession.replace("-", "");
  }

  static NcSubmission parse(byte[] nc) {
    String text = new String(nc, StandardCharsets.ISO_8859_1);
    int firstDoc = text.indexOf("<DOCUMENT>");
    String header = firstDoc < 0 ? text : text.substring(0, firstDoc);
    String accession = first(ACCESSION, header);
    String formType = first(HEADER_TYPE, header);
    if (accession == null || formType == null) {
      throw new IllegalArgumentException("not an EDGAR feed submission: no accession or type");
    }
    List<Doc> docs = new ArrayList<Doc>();
    int pos = firstDoc;
    while (pos >= 0) {
      int end = text.indexOf("</DOCUMENT>", pos);
      if (end < 0) {
        break;
      }
      docs.add(parseDoc(text.substring(pos + "<DOCUMENT>".length(), end)));
      pos = text.indexOf("<DOCUMENT>", end);
    }
    return new NcSubmission(accession, formType.trim(), first(FILING_DATE, header),
        header.getBytes(StandardCharsets.ISO_8859_1), docs);
  }

  private static Doc parseDoc(String doc) {
    int textStart = doc.indexOf("<TEXT>");
    String meta = textStart < 0 ? doc : doc.substring(0, textStart);
    String type = null;
    String filename = null;
    int sequence = 0;
    Matcher m = DOC_FIELD.matcher(meta);
    while (m.find()) {
      String value = m.group(2).trim();
      if ("TYPE".equals(m.group(1))) {
        type = value;
      } else if ("SEQUENCE".equals(m.group(1))) {
        sequence = value.isEmpty() ? 0 : Integer.parseInt(value);
      } else {
        filename = value;
      }
    }
    if (textStart < 0) {
      return new Doc(type, sequence, filename, new byte[0]);
    }
    int textEnd = doc.lastIndexOf("</TEXT>");
    String body = doc.substring(textStart + "<TEXT>".length(), textEnd < 0 ? doc.length() : textEnd);
    String stripped = stripWrapper(body);
    if (stripped == null || UUENCODED.matcher(stripped).find()) {
      return new Doc(type, sequence, filename, null);
    }
    return new Doc(type, sequence, filename, stripped.getBytes(StandardCharsets.ISO_8859_1));
  }

  /** The envelopes a submission puts around a text document's content. */
  private static final String[] ENVELOPES = {"XML", "XBRL"};

  /**
   * The document content without its {@code <XML>}/{@code <XBRL>}/{@code <PDF>} envelope; null for
   * a PDF. {@code <XBRL>} wraps the linkbases and the instance of many filings, and a submission's
   * line ending is {@code \r\n}, {@code \n} or a bare {@code \r}; sec.gov serves the content
   * alone, with the XML declaration first, and an XML parser rejects anything before it.
   */
  static String stripWrapper(String body) {
    String t = dropLineEnding(body);
    if (t.startsWith("<PDF>")) {
      return null;
    }
    for (String tag : ENVELOPES) {
      String open = "<" + tag + ">";
      if (t.startsWith(open)) {
        t = t.substring(open.length());
        int close = t.lastIndexOf("</" + tag + ">");
        if (close >= 0) {
          t = t.substring(0, close);
        }
        return dropLineEnding(t);
      }
    }
    return t;
  }

  private static String dropLineEnding(String s) {
    if (s.startsWith("\r\n")) {
      return s.substring(2);
    }
    if (s.startsWith("\n") || s.startsWith("\r")) {
      return s.substring(1);
    }
    return s;
  }

  /**
   * {@code content} without a leading envelope, for a document unpacked before the envelope was
   * stripped correctly (the tree on disk holds years of them); the same bytes when it has none.
   */
  static byte[] unwrapEnvelope(byte[] content) {
    int i = 0;
    while (i < content.length && i < 2 && (content[i] == '\r' || content[i] == '\n')) {
      i++;
    }
    if (!startsWith(content, i, "<XML>") && !startsWith(content, i, "<XBRL>")) {
      return content;
    }
    String stripped = stripWrapper(new String(content, StandardCharsets.ISO_8859_1));
    return stripped == null ? content : stripped.getBytes(StandardCharsets.ISO_8859_1);
  }

  private static boolean startsWith(byte[] content, int offset, String prefix) {
    if (content.length - offset < prefix.length()) {
      return false;
    }
    for (int k = 0; k < prefix.length(); k++) {
      if (content[offset + k] != prefix.charAt(k)) {
        return false;
      }
    }
    return true;
  }

  private static String first(Pattern p, String s) {
    Matcher m = p.matcher(s);
    return m.find() ? m.group(1) : null;
  }
}
