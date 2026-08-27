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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Appendix A-4 (Mode of transportation codes) of the 2017 CFS PUF Data Users Guide into
 * {@code cfs_mode_ref} rows: one row per {@code cfs_shipments.mode_code} value and its
 * description (e.g. "04" &mdash; "For-hire truck"). Stops before the appendix's second table
 * (the "Mode Collapsing Pattern" confidentiality-recoding diagram), which is not a code list.
 *
 * <p>Four codes (12, 19, 14, 20) sit in a merged/stacked cell in the source PDF, grouped under
 * their parent category ("Single mode", "Multiple mode"). PDFBox's text stripper (unlike a
 * layout-aware extractor) emits that cell as bare code lines followed by bare description lines
 * — e.g. {@code "12\n19\nPipeline\nOther mode"} &mdash; instead of one code+description per
 * line. A FIFO queue re-pairs each bare code with the next bare description in appendix order,
 * which is stable because the PDF's own visual layout is top-to-bottom for both the code column
 * and the description column of that cell (verified against the live PDF: 12=Pipeline,
 * 19=Other mode, 14=Parcel/USPS/courier, 20=Non-parcel multiple mode).
 */
public class CfsModeRefTransformer extends CfsPufCodeListTransformer {

  // "04 For-hire truck"  or  "101 Multiple Waterways" (code and description on one line)
  private static final Pattern INLINE_ROW = Pattern.compile("^(\\d{2,3})\\s+(.+)$");
  // A code with no description on its own line (stacked-cell case)
  private static final Pattern BARE_CODE = Pattern.compile("^(\\d{2,3})$");

  @Override
  protected String appendixStartMarker() {
    return "Appendix A-4\nMode";
  }

  @Override
  protected void parseRows(String appendixText, ArrayNode rows) {
    Deque<String> pendingCodes = new ArrayDeque<>();
    for (String rawLine : appendixText.split("\n")) {
      String line = rawLine.trim();
      if (line.isEmpty()) {
        continue;
      }
      Matcher bare = BARE_CODE.matcher(line);
      if (bare.matches()) {
        pendingCodes.addLast(bare.group(1));
        continue;
      }
      Matcher inline = INLINE_ROW.matcher(line);
      if (inline.matches()) {
        addRow(rows, inline.group(1), inline.group(2).trim());
        continue;
      }
      if (!pendingCodes.isEmpty()) {
        addRow(rows, pendingCodes.pollFirst(), line);
      }
    }
  }

  private void addRow(ArrayNode rows, String code, String description) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("mode_code", code);
    row.put("description", description);
    rows.add(row);
  }
}
