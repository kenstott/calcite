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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A filing found in the local SEC Form 13F data set must yield exactly the rows its information
 * table would, so the per-filing fetch can be skipped without changing the table.
 */
@Tag("unit")
class Sec13fBulkDataTest {

  private static final String XML =
      "<?xml version='1.0' encoding='UTF-8'?>"
      + "<ns1:informationTable xmlns:ns1='http://www.sec.gov/edgar/document/thirteenf/informationtable'>"
      + "<ns1:infoTable><ns1:nameOfIssuer>M &amp; T BANK CORP</ns1:nameOfIssuer>"
      + "<ns1:titleOfClass>COM</ns1:titleOfClass><ns1:cusip>55261F104</ns1:cusip>"
      + "<ns1:value>39808037</ns1:value><ns1:shrsOrPrnAmt><ns1:sshPrnamt>314812</ns1:sshPrnamt>"
      + "<ns1:sshPrnamtType>SH</ns1:sshPrnamtType></ns1:shrsOrPrnAmt>"
      + "<ns1:investmentDiscretion>DFND</ns1:investmentDiscretion>"
      + "<ns1:otherManager>59</ns1:otherManager>"
      + "<ns1:votingAuthority><ns1:Sole>314481</ns1:Sole><ns1:Shared>0</ns1:Shared>"
      + "<ns1:None>332</ns1:None></ns1:votingAuthority></ns1:infoTable>"
      + "<ns1:infoTable><ns1:nameOfIssuer>APPLE INC</ns1:nameOfIssuer>"
      + "<ns1:titleOfClass>COM</ns1:titleOfClass><ns1:cusip>037833100</ns1:cusip>"
      + "<ns1:value>1515058</ns1:value><ns1:shrsOrPrnAmt><ns1:sshPrnamt>15643</ns1:sshPrnamt>"
      + "<ns1:sshPrnamtType>SH</ns1:sshPrnamtType></ns1:shrsOrPrnAmt>"
      + "<ns1:putCall>Put</ns1:putCall>"
      + "<ns1:investmentDiscretion>SOLE</ns1:investmentDiscretion>"
      + "<ns1:votingAuthority><ns1:Sole>15253</ns1:Sole><ns1:Shared>0</ns1:Shared>"
      + "<ns1:None>390</ns1:None></ns1:votingAuthority></ns1:infoTable>"
      + "</ns1:informationTable>";

  /** The same two positions as the data set carries them: fixed-width padded, blanks for absent. */
  private static void writeDataSet(File dir) throws Exception {
    File out = new File(dir, "infotable_test.parquet");
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("COPY (SELECT * FROM (VALUES "
          + "('0000019617-23-000542','M & T BANK CORP                    ','COM','55261F104',"
          + "'39808037','314812','SH',NULL,'DFND','59','314481','0','332'),"
          + "('0000019617-23-000542','APPLE INC','COM','037833100','1515058','15643','SH','Put',"
          + "'SOLE',NULL,'15253','0','390'),"
          + "('0000895421-23-000493','OTHER CO','COM','000000000','1','1','SH',NULL,'SOLE',NULL,"
          + "'1','0','0')"
          + ") t(ACCESSION_NUMBER, NAMEOFISSUER, TITLEOFCLASS, CUSIP, VALUE, SSHPRNAMT, "
          + "SSHPRNAMTTYPE, PUTCALL, INVESTMENTDISCRETION, OTHERMANAGER, VOTING_AUTH_SOLE, "
          + "VOTING_AUTH_SHARED, VOTING_AUTH_NONE) ORDER BY ACCESSION_NUMBER) TO '"
          + out.getAbsolutePath().replace("'", "''") + "' (FORMAT PARQUET)");
    }
  }

  private static Document parse(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    return factory.newDocumentBuilder().parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test void rowsOfAnAccessionAreTrimmedAndBlanksAreNull(@TempDir File dir) throws Exception {
    writeDataSet(dir);
    try (Sec13fBulkData bulk = Sec13fBulkData.open(dir)) {
      List<Sec13fBulkData.Row> rows = bulk.rows("0000019617-23-000542");
      assertNotNull(rows);
      assertEquals(2, rows.size());
      Sec13fBulkData.Row mt = rows.get(0).issuer.startsWith("M") ? rows.get(0) : rows.get(1);
      assertEquals("M & T BANK CORP", mt.issuer);
      assertNull(mt.putCall);
      assertEquals("59", mt.otherManager);
    }
  }

  @Test void anAccessionNotInTheDataSetIsNullSoTheCallerFetchesIt(@TempDir File dir)
      throws Exception {
    writeDataSet(dir);
    try (Sec13fBulkData bulk = Sec13fBulkData.open(dir)) {
      assertNull(bulk.rows("0000000000-26-000001"));
    }
  }

  @Test void aDirectoryWithNoDataSetIsAnErrorNotASilentFallback(@TempDir File dir) {
    assertThrows(IllegalStateException.class, () -> Sec13fBulkData.open(dir));
  }

  @Test void bulkRowsEqualTheRowsOfTheInformationTable(@TempDir File dir) throws Exception {
    writeDataSet(dir);
    XbrlToParquetConverter converter = new XbrlToParquetConverter(null);
    // One filing before and one after the 2023-01-03 change from thousands to whole dollars.
    for (String filingDate : new String[] {"2022-11-14", "2023-11-14"}) {
      List<Map<String, Object>> fromXml = converter.extract13FHoldings(parse(XML), "0000019617",
          "13F-HR", filingDate, "0000019617-23-000542", 2023, "2023-09-30");
      try (Sec13fBulkData bulk = Sec13fBulkData.open(dir)) {
        List<Map<String, Object>> fromBulk = converter.holdingsFromBulk(
            bulk.rows("0000019617-23-000542"), "0000019617", "13F-HR", filingDate,
            "0000019617-23-000542", 2023, "2023-09-30");
        assertEquals(sorted(fromXml), sorted(fromBulk), "filing date " + filingDate);
      }
    }
  }

  /** Orders by cusip so two sources that list positions in different orders compare equal. */
  private static List<Map<String, Object>> sorted(List<Map<String, Object>> rows) {
    java.util.List<Map<String, Object>> copy = new java.util.ArrayList<Map<String, Object>>(rows);
    java.util.Collections.sort(copy, (a, b) ->
        String.valueOf(a.get("cusip")).compareTo(String.valueOf(b.get("cusip"))));
    return copy;
  }
}
