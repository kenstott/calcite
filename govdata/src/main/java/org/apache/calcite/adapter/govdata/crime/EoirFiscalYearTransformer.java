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
package org.apache.calcite.adapter.govdata.crime;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base for the EOIR one-page fiscal-year tables. The configured source URL is the EOIR "Workload
 * and Adjudication Statistics" page; the PDF is located by its link title there, so a re-issued
 * file under a new media id is still found.
 *
 * <p>Each data row is the fiscal year, an optional "(Third Quarter)" marker on the unfinished
 * year, then the counts. The last-but-N column relationships printed in the table are checked
 * on every row, so a changed column layout fails the load.
 */
public abstract class EoirFiscalYearTransformer extends EoirPdfTransformer {

  private static final Pattern GENERATED =
      Pattern.compile("Data Generated: ([A-Z][a-z]+ \\d{1,2}, \\d{4})");
  private static final DateTimeFormatter GENERATED_FORMAT =
      DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH);

  /** The link title of the PDF on the statistics page. */
  protected abstract String linkTitle();

  /** The table's column header, whitespace-collapsed, exactly as printed. */
  protected abstract String header();

  /** Output column names for the counts, in printed order. */
  protected abstract String[] countColumns();

  /** Throws if the counts of one row do not satisfy the table's own arithmetic. */
  protected abstract void check(int fiscalYear, long[] counts);

  @Override protected String rows(String response, String sourceUrl) throws IOException {
    Matcher link = Pattern.compile(
        "<a href=\"([^\"]+)\"[^>]*>\\s*" + Pattern.quote(linkTitle()) + "\\s*</a>").matcher(response);
    if (!link.find()) {
      throw new IOException("no link titled '" + linkTitle() + "' on " + sourceUrl);
    }
    String pdfUrl = URI.create(sourceUrl).resolve(link.group(1)).toString();
    StringBuilder text = new StringBuilder();
    for (String page : pageTexts(download(pdfUrl))) {
      text.append(page).append('\n');
    }
    return parse(text.toString()).toString();
  }

  /** Parses the PDF text into rows. Package-visible for tests. */
  ArrayNode parse(String rawText) {
    String text = rawText.replaceAll("\\s+", " ");
    int start = text.indexOf(header());
    if (start < 0) {
      throw new IllegalStateException("EOIR '" + linkTitle() + "': column header not found: "
          + header());
    }
    Matcher generated = GENERATED.matcher(text);
    if (!generated.find(start)) {
      throw new IllegalStateException("EOIR '" + linkTitle() + "': no 'Data Generated' date");
    }
    String dataGenerated =
        LocalDate.parse(generated.group(1), GENERATED_FORMAT).toString();
    String body = text.substring(start + header().length(), generated.start()).trim();

    String[] columns = countColumns();
    StringBuilder counts = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      counts.append(" ([\\d,]+)");
    }
    Pattern row = Pattern.compile(
        "\\G\\s*(\\d{4})(?: \\( ?(First|Second|Third) Quarter ?\\))?" + counts);
    ArrayNode out = MAPPER.createArrayNode();
    Matcher m = row.matcher(body);
    int end = 0;
    while (m.find()) {
      int fiscalYear = Integer.parseInt(m.group(1));
      long[] values = new long[columns.length];
      for (int i = 0; i < columns.length; i++) {
        values[i] = Long.parseLong(m.group(i + 3).replace(",", ""));
      }
      check(fiscalYear, values);
      ObjectNode node = out.addObject();
      node.put("fiscal_year", fiscalYear);
      node.put("quarters_reported", quarters(m.group(2)));
      for (int i = 0; i < columns.length; i++) {
        node.put(columns[i], values[i]);
      }
      node.put("data_generated", dataGenerated);
      end = m.end();
    }
    if (out.size() == 0 || !body.substring(end).trim().isEmpty()) {
      throw new IllegalStateException("EOIR '" + linkTitle() + "': unparsed table text: "
          + body.substring(end));
    }
    return out;
  }

  private static int quarters(String marker) {
    if (marker == null) {
      return 4;
    }
    return "First".equals(marker) ? 1 : "Second".equals(marker) ? 2 : 3;
  }
}
