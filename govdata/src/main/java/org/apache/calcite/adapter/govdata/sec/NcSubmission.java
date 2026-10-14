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

  /** The document content without its {@code <XML>}/{@code <PDF>} wrapper; null for a PDF. */
  private static String stripWrapper(String body) {
    String t = body;
    if (t.startsWith("\r\n")) {
      t = t.substring(2);
    } else if (t.startsWith("\n")) {
      t = t.substring(1);
    }
    if (t.startsWith("<PDF>")) {
      return null;
    }
    if (t.startsWith("<XML>")) {
      t = t.substring("<XML>".length());
      int close = t.lastIndexOf("</XML>");
      if (close >= 0) {
        t = t.substring(0, close);
      }
      if (t.startsWith("\r\n")) {
        t = t.substring(2);
      } else if (t.startsWith("\n")) {
        t = t.substring(1);
      }
    }
    return t;
  }

  private static String first(Pattern p, String s) {
    Matcher m = p.matcher(s);
    return m.find() ? m.group(1) : null;
  }
}
