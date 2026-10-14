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

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A feed entry splits into its documents byte for byte, and binaries are not carried. */
@Tag("unit")
class NcSubmissionTest {

  static final String FORM4 =
      "<SUBMISSION>\n<ACCESSION-NUMBER>0000805676-23-000155\n<TYPE>4\n"
      + "<PUBLIC-DOCUMENT-COUNT>1\n<PERIOD>20231129\n<FILING-DATE>20231214\n"
      + "<REPORTING-OWNER>\n<OWNER-DATA>\n<CONFORMED-NAME>ENGLEFIELD F W IV\n<CIK>0001311245\n"
      + "</OWNER-DATA>\n</REPORTING-OWNER>\n<ISSUER>\n<COMPANY-DATA>\n"
      + "<CONFORMED-NAME>PARK NATIONAL CORP /OH/\n<CIK>0000805676\n</COMPANY-DATA>\n</ISSUER>\n"
      + "<DOCUMENT>\n<TYPE>4\n<SEQUENCE>1\n<FILENAME>wk-form4_1702587176.xml\n"
      + "<DESCRIPTION>FORM 4\n<TEXT>\n<XML>\n<?xml version=\"1.0\"?>\n"
      + "<ownershipDocument><issuer>PARK</issuer></ownershipDocument>\n</XML>\n</TEXT>\n"
      + "</DOCUMENT>\n</SUBMISSION>\n";

  static final String EIGHT_K =
      "<SUBMISSION>\n<ACCESSION-NUMBER>0001234567-23-000009\n<TYPE>8-K\n"
      + "<FILING-DATE>20231214\n<FILER>\n<COMPANY-DATA>\n<CONFORMED-NAME>ACME\n<CIK>0001234567\n"
      + "</COMPANY-DATA>\n</FILER>\n"
      + "<DOCUMENT>\n<TYPE>8-K\n<SEQUENCE>1\n<FILENAME>acme-8k.htm\n<DESCRIPTION>8-K\n<TEXT>\n"
      + "<html><body>Café &amp; Co</body></html>\n</TEXT>\n</DOCUMENT>\n"
      + "<DOCUMENT>\n<TYPE>GRAPHIC\n<SEQUENCE>2\n<FILENAME>logo.jpg\n<DESCRIPTION>logo\n<TEXT>\n"
      + "begin 644 logo.jpg\nM_]C_X``02D9)1@`!`0\nend\n</TEXT>\n</DOCUMENT>\n"
      + "<DOCUMENT>\n<TYPE>XML\n<SEQUENCE>3\n<FILENAME>FilingSummary.xml\n<DESCRIPTION>FS\n<TEXT>\n"
      + "<XML>\n<FilingSummary><Version>1</Version></FilingSummary>\n</XML>\n</TEXT>\n"
      + "</DOCUMENT>\n</SUBMISSION>\n";

  @Test void headerFieldsAreRead() {
    NcSubmission s = NcSubmission.parse(FORM4.getBytes(StandardCharsets.ISO_8859_1));
    assertEquals("0000805676-23-000155", s.accession);
    assertEquals("000080567623000155", s.accessionNoDash());
    assertEquals("4", s.formType);
    assertEquals("20231214", s.filingDate);
    assertEquals(1, s.docs.size());
  }

  @Test void anXmlDocumentIsReturnedWithoutItsWrapper() {
    NcSubmission s = NcSubmission.parse(FORM4.getBytes(StandardCharsets.ISO_8859_1));
    NcSubmission.Doc d = s.docs.get(0);
    assertEquals("wk-form4_1702587176.xml", d.filename);
    assertEquals("<?xml version=\"1.0\"?>\n"
        + "<ownershipDocument><issuer>PARK</issuer></ownershipDocument>\n",
        new String(d.content, StandardCharsets.ISO_8859_1));
  }

  @Test void textBinaryAndXmlDocumentsAreSeparated() {
    NcSubmission s = NcSubmission.parse(EIGHT_K.getBytes(StandardCharsets.ISO_8859_1));
    assertEquals("8-K", s.formType);
    assertEquals(3, s.docs.size());
    assertEquals("acme-8k.htm", s.docs.get(0).filename);
    assertTrue(!s.docs.get(0).isBinary());
    assertEquals("logo.jpg", s.docs.get(1).filename);
    assertTrue(s.docs.get(1).isBinary());
    assertNull(s.docs.get(1).content);
    assertEquals("FilingSummary.xml", s.docs.get(2).filename);
    assertEquals("<FilingSummary><Version>1</Version></FilingSummary>\n",
        new String(s.docs.get(2).content, StandardCharsets.ISO_8859_1));
  }

  @Test void documentBytesAreReturnedExactlyAsFiled() {
    // UTF-8 "Café" is read and written as raw bytes; nothing is re-encoded.
    byte[] nc = EIGHT_K.replace("Café", "CafÃ©").getBytes(StandardCharsets.ISO_8859_1);
    NcSubmission s = NcSubmission.parse(nc);
    byte[] expected =
        "<html><body>CafÃ© &amp; Co</body></html>\n".getBytes(StandardCharsets.ISO_8859_1);
    assertArrayEquals(expected, s.docs.get(0).content);
  }

  @Test void somethingThatIsNotASubmissionIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> NcSubmission.parse("not a submission".getBytes(StandardCharsets.ISO_8859_1)));
  }
}
