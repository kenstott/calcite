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

import org.apache.calcite.adapter.govdata.GovDataException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests {@link GovInfoHearingProvider#rowsOf} against the shape of a CHRG MODS record. */
@Tag("unit")
class GovInfoHearingProviderTest {

  private static final byte[] MODS = ("<mods xmlns=\"http://www.loc.gov/mods/v3\">"
      + "<titleInfo><title>ARCTIC FROST ACCOUNTABILITY</title></titleInfo>"
      + "<titleInfo type=\"alternative\"/>"
      + "<extension><collectionCode>CHRG</collectionCode></extension>"
      + "<extension><accessId>CHRG-119shrg65582</accessId><chamber>SENATE</chamber>"
      + "<congress years=\"2025-2026\">119</congress><session>2</session><number>547</number>"
      + "<heldDate>2026-02-12</heldDate><heldDate>2026-02-10</heldDate>"
      + "<preferredCitation>S. Hrg. 119-547</preferredCitation></extension>"
      + "<relatedItem type=\"constituent\"><extension><witness>McAtee, David R</witness>"
      + "<witness>Miller, Chris</witness><jacketId>65-582</jacketId></extension></relatedItem>"
      + "<extension><witness>Miller, Chris</witness><jacketId>65-582</jacketId>"
      + "<congCommittee authorityId=\"ssju00\"><name type=\"authority-standard\">Committee on "
      + "the Judiciary</name><subCommittee><name type=\"parsed\">Subcommittee on Privacy</name>"
      + "</subCommittee></congCommittee></extension></mods>")
      .getBytes(StandardCharsets.UTF_8);

  @Test void hearingRow() throws Exception {
    Map<String, Object> row = GovInfoHearingProvider
        .rowsOf("committee_hearings", "2026-11-24T00:00:00Z", MODS).get(0);
    assertEquals("CHRG-119shrg65582", row.get("package_id"));
    assertEquals("Senate", row.get("chamber"));
    assertEquals(65582, row.get("jacket_number"));
    assertEquals("ARCTIC FROST ACCOUNTABILITY", row.get("title"));
    assertEquals("2026-02-10", row.get("first_held_date"));
    assertEquals("2026-02-12", row.get("last_held_date"));
    assertEquals("ssju00", row.get("committee_system_code"));
    assertEquals("Committee on the Judiciary", row.get("committee_name"));
    assertEquals("Subcommittee on Privacy", row.get("subcommittee_name"));
    assertEquals(2, row.get("witness_count"));
  }

  @Test void witnessesOfThePackageAndItsPartsCountOnce() throws Exception {
    List<Map<String, Object>> rows = GovInfoHearingProvider
        .rowsOf("committee_hearing_witnesses", "x", MODS);
    assertEquals(2, rows.size());
    assertEquals("Miller, Chris", rows.get(0).get("witness"));
    assertEquals("McAtee, David R", rows.get(1).get("witness"));
  }

  @Test void absentFieldsAreNull() throws Exception {
    byte[] bare = ("<mods xmlns=\"http://www.loc.gov/mods/v3\"><extension>"
        + "<accessId>CHRG-111jhrg1</accessId><chamber>JOINT</chamber><congress>111</congress>"
        + "</extension></mods>").getBytes(StandardCharsets.UTF_8);
    Map<String, Object> row =
        GovInfoHearingProvider.rowsOf("committee_hearings", "x", bare).get(0);
    assertNull(row.get("first_held_date"));
    assertNull(row.get("committee_system_code"));
    assertNull(row.get("jacket_number"));
    assertEquals(0, row.get("witness_count"));
  }

  @Test void nonModsDocumentFails() {
    assertThrows(GovDataException.class, () -> GovInfoHearingProvider
        .rowsOf("committee_hearings", "x", "<html/>".getBytes(StandardCharsets.UTF_8)));
  }
}
