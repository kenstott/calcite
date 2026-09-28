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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Parses the House Clerk's annual financial disclosure index
 * ({@code disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip}, the {@code
 * {year}FD.xml} entry) into one {@link Entry} per filing.
 *
 * <p>Every House member and candidate filing of any kind for the year is listed — annual reports,
 * candidate reports, amendments, extensions, terminations, withdrawals, and periodic transaction
 * reports ({@link Entry#filingType} {@code "P"}, the STOCK Act disclosures {@link
 * HouseStockTransactionsProvider} reads). This index carries filer identity and a document ID
 * only; the transaction detail is in the PDF at {@code
 * public_disc/ptr-pdfs/{year}/{docId}.pdf}.
 */
final class HouseFinancialDisclosureIndex {

  /** One row of the index: a single filing by a single filer. */
  static final class Entry {
    final String lastName;
    final String firstName;
    final String filingType;
    final String stateDistrict;
    final String filingDate;
    final String docId;

    Entry(String lastName, String firstName, String filingType, String stateDistrict,
        String filingDate, String docId) {
      this.lastName = lastName;
      this.firstName = firstName;
      this.filingType = filingType;
      this.stateDistrict = stateDistrict;
      this.filingDate = filingDate;
      this.docId = docId;
    }
  }

  private HouseFinancialDisclosureIndex() {
  }

  /** Parses every {@code <Member>} record in the index; malformed dates/IDs are skipped. */
  static List<Entry> parse(InputStream in) throws XMLStreamException {
    List<Entry> entries = new ArrayList<Entry>();
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    XMLStreamReader r = factory.createXMLStreamReader(in);
    try {
      String last = null;
      String first = null;
      String filingType = null;
      String stateDistrict = null;
      String filingDate = null;
      String docId = null;
      String currentTag = null;
      boolean inMember = false;
      while (r.hasNext()) {
        int event = r.next();
        if (event == XMLStreamConstants.START_ELEMENT) {
          String name = r.getLocalName();
          if ("Member".equals(name)) {
            inMember = true;
            last = null;
            first = null;
            filingType = null;
            stateDistrict = null;
            filingDate = null;
            docId = null;
          }
          currentTag = name;
        } else if (event == XMLStreamConstants.CHARACTERS && inMember && currentTag != null) {
          String text = r.getText().trim();
          if (!text.isEmpty()) {
            if ("Last".equals(currentTag)) {
              last = text;
            } else if ("First".equals(currentTag)) {
              first = text;
            } else if ("FilingType".equals(currentTag)) {
              filingType = text;
            } else if ("StateDst".equals(currentTag)) {
              stateDistrict = text;
            } else if ("FilingDate".equals(currentTag)) {
              filingDate = text;
            } else if ("DocID".equals(currentTag)) {
              docId = text;
            }
          }
        } else if (event == XMLStreamConstants.END_ELEMENT) {
          if ("Member".equals(r.getLocalName())) {
            if (last != null && filingType != null && docId != null) {
              entries.add(new Entry(last, first == null ? "" : first, filingType,
                  stateDistrict == null ? "" : stateDistrict, filingDate, docId));
            }
            inMember = false;
          }
          currentTag = null;
        }
      }
    } finally {
      r.close();
    }
    return entries;
  }
}
