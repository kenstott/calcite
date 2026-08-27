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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Appendix A-3 (SCTG commodity codes) of the 2017 CFS PUF Data Users Guide into
 * {@code cfs_sctg_ref} rows: one row per 2-digit SCTG code, its description, and (where the
 * appendix gives one) the confidentiality-collapsed SCTG group range it belongs to.
 */
public class CfsSctgRefTransformer extends CfsPufCodeListTransformer {

  // "01 Animals and Fish (live) 01-05"  or  "02 Cereal Grains (includes seed)" (no group)
  private static final Pattern ROW_PATTERN =
      Pattern.compile("^(\\d{2})\\s+(.+?)(?:\\s+(\\d{2}-\\d{2}))?$");

  @Override
  protected String appendixStartMarker() {
    return "Appendix A-3\nSCTG";
  }

  @Override
  protected void parseRows(String appendixText, ArrayNode rows) {
    for (String line : appendixText.split("\n")) {
      Matcher m = ROW_PATTERN.matcher(line.trim());
      if (!m.matches()) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("sctg_code", m.group(1));
      row.put("description", m.group(2).trim());
      if (m.group(3) != null) {
        row.put("sctg_group", m.group(3));
      } else {
        row.putNull("sctg_group");
      }
      rows.add(row);
    }
  }
}
