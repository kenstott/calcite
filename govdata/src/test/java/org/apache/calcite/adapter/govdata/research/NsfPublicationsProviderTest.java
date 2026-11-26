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
package org.apache.calcite.adapter.govdata.research;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests how an NSF award's publication entries become {@code nsf_publications} rows. */
@Tag("unit")
class NsfPublicationsProviderTest {

  private static JsonNode award(String jrnl) throws IOException {
    return new ObjectMapper().readTree(
        "{\"id\":\"1902717\",\"dirAbbr\":\"MPS\",\"divAbbr\":\"DMS\"" + jrnl + "}");
  }

  @Test void doiAcceptsResolverFormsOnly() {
    assertEquals("10.3842/sigma.2022.078",
        NsfPublicationsProvider.doi("https://doi.org/10.3842/SIGMA.2022.078"));
    assertEquals("10.3390/gels7020061",
        NsfPublicationsProvider.doi("https://doi.org/https://doi.org/10.3390/gels7020061"));
    assertEquals("10.1002/eco.1721", NsfPublicationsProvider.doi("DOI: 10.1002/eco.1721."));
    assertEquals("10.1086/674526", NsfPublicationsProvider.doi("DOI 10.1086/674526"));
    assertNull(NsfPublicationsProvider.doi("https://doi.org/"));
    assertNull(NsfPublicationsProvider.doi("0024-9297"));
    assertNull(NsfPublicationsProvider.doi("10.1002/ 2016JC012331"));
    assertNull(NsfPublicationsProvider.doi("10.1175/JHM-D-12- 0189.1"));
    assertNull(NsfPublicationsProvider.doi("10.1002/grl.50196, 2013"));
    assertNull(NsfPublicationsProvider.doi(null));
  }

  @Test void awardWithoutPublicationsYieldsNoRows() throws IOException {
    assertTrue(NsfPublicationsProvider.toRows(award(""), 2019).isEmpty());
  }

  @Test void repeatedEntriesCollapseToTheMostCompleteOne() throws IOException {
    String bare = "{\"artTitl\":\"K-Theoretic Series.\",\"jrnlYr\":\"2021\"}";
    String full = "{\"artTitl\":\"K-theoretic series\",\"jrnlYr\":\"2022\","
        + "\"dgtlObjId\":\"https://doi.org/10.3842/SIGMA.2022.078\",\"parPblcId\":\"10487127\"}";
    for (String order : new String[] {bare + "," + full, full + "," + bare}) {
      List<Map<String, Object>> rows =
          NsfPublicationsProvider.toRows(award(",\"jrnl\":[" + order + "]"), 2019);
      assertEquals(1, rows.size());
      Map<String, Object> row = rows.get(0);
      assertEquals("doi:10.3842/sigma.2022.078", row.get("publication_key"));
      assertEquals("10487127", row.get("par_id"));
      assertEquals(2022, row.get("pub_year"));
      assertEquals(2, row.get("source_record_count"));
      assertEquals("MPS", row.get("directorate"));
      assertEquals(2019, row.get("fiscal_year"));
    }
  }

  @Test void titleSharedByTwoDoisStaysSeparate() throws IOException {
    List<Map<String, Object>> rows = NsfPublicationsProvider.toRows(award(",\"jrnl\":["
        + "{\"artTitl\":\"T\",\"dgtlObjId\":\"10.1000/a\"},"
        + "{\"artTitl\":\"T\",\"dgtlObjId\":\"10.1000/b\"},"
        + "{\"artTitl\":\"t.\"}]"), 2019);
    assertEquals(3, rows.size());
    assertEquals("title:t", rows.get(2).get("publication_key"));
  }

  @Test void untitledEntriesWithoutDoiAreKeyedByTheirFields() throws IOException {
    List<Map<String, Object>> rows = NsfPublicationsProvider.toRows(award(",\"jrnl\":["
        + "{\"artTitl\":\"--\",\"auth\":\"See report 1\"},"
        + "{\"artTitl\":\"--\",\"auth\":\"See report 2\"},"
        + "{\"artTitl\":\"--\",\"auth\":\"See report 2\"}]"), 2010);
    assertEquals(2, rows.size());
    assertNull(rows.get(0).get("doi"));
  }
}
