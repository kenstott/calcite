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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class OshaEnforcementTransformerTest {

  @TempDir Path tmp;

  private File zip(String... nameAndBody) throws IOException {
    File f = new File(tmp.toFile(), "t.zip");
    try (ZipOutputStream z = new ZipOutputStream(new FileOutputStream(f))) {
      for (int i = 0; i < nameAndBody.length; i += 2) {
        z.putNextEntry(new ZipEntry(nameAndBody[i]));
        z.write(nameAndBody[i + 1].getBytes(StandardCharsets.UTF_8));
        z.closeEntry();
      }
    }
    return f;
  }

  private static final String VIOL_HEADER = "﻿ACTIVITY_NR,CITATION_ID,DELETE_FLAG,STANDARD,"
      + "VIOL_TYPE,ISSUANCE_DATE,ABATE_DATE,ABATE_COMPLETE,CURRENT_PENALTY,INITIAL_PENALTY,"
      + "CONTEST_DATE,FINAL_ORDER_DATE,NR_INSTANCES,NR_EXPOSED,REC,GRAVITY,EMPHASIS,HAZCAT,"
      + "FTA_INSP_NR,FTA_ISSUANCE_DATE,FTA_PENALTY,FTA_CONTEST_DATE,FTA_FINAL_ORDER_DATE,"
      + "HAZSUB1,HAZSUB2,HAZSUB3,HAZSUB4,HAZSUB5,LOAD_DT\n";

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  @Test void streamsAllChunksAndTypesValues() throws Exception {
    String r1 = "308260132,01001,X,19100036 B01,S,2005-08-09 00:00:00+00:00,"
        + "2005-09-26 00:00:00+00:00,X,,4500.00,NaT,NaT,1,5241.0,,10,,,nan,NaT,,NaT,NaT,,,,,,"
        + "2011-10-14 16:45:21+00:00\n";
    String r2 = "3031036,01001A,,\"19100132 A\",O,1989-09-05 00:00:00+00:00,NaT,,200.5,,NaT,NaT,"
        + "2,3.0,,,,,nan,NaT,,NaT,NaT,,,,,,2015-01-05 01:01:08+00:00\n";
    File z = zip("c_chunk_1.csv", VIOL_HEADER + r1, "c_chunk_2.csv", VIOL_HEADER + r2);
    List<Map<String, Object>> rows = drain(OshaEnforcementTransformer.stream(
        z, "u", "violation", new OshaViolationsTransformer().columns()));
    assertEquals(2, rows.size());
    Map<String, Object> a = rows.get(0);
    assertEquals("308260132", a.get("activity_nr"));
    assertEquals("01001", a.get("citation_id"));
    assertEquals("S", a.get("viol_type"));
    assertEquals("2005-08-09", a.get("issuance_date"));
    assertEquals(Integer.valueOf(2005), a.get("issuance_year"));
    assertEquals(Double.valueOf(4500.0), a.get("initial_penalty"));
    assertNull(a.get("current_penalty"));
    assertNull(a.get("contest_date"));
    assertNull(a.get("fta_insp_nr"));
    assertEquals(Integer.valueOf(5241), a.get("nr_exposed"));
    assertEquals("19100132 A", rows.get(1).get("standard"));
    assertEquals(Double.valueOf(200.5), rows.get(1).get("current_penalty"));
    assertFalse(z.exists(), "staging zip is removed once exhausted");
  }

  @Test void malformedDateFailsLoudly() throws Exception {
    String bad = "1,01001,,STD,S,not-a-date,NaT,,,,NaT,NaT,1,1.0,,,,,nan,NaT,,NaT,NaT,,,,,,x\n";
    File z = zip("c_chunk_1.csv", VIOL_HEADER + bad);
    Iterator<Map<String, Object>> it = OshaEnforcementTransformer.stream(
        z, "u", "violation", new OshaViolationsTransformer().columns());
    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void missingColumnFailsLoudly() throws Exception {
    File z = zip("c_chunk_1.csv", "ACTIVITY_NR,CITATION_ID\n1,2\n");
    Iterator<Map<String, Object>> it = OshaEnforcementTransformer.stream(
        z, "u", "violation", new OshaViolationsTransformer().columns());
    assertThrows(IllegalStateException.class, it::hasNext);
  }
}
