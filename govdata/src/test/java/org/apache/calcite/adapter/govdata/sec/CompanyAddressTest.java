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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Confirms {@link SecDataFetcher#getCompanyInfoForCik} returns full postal addresses
 * (street, city, state, ZIP) rather than only the street line, per D-030.
 */
@Tag("integration")
class CompanyAddressTest {

  @Test
  void appleAddressIncludesCityStateZip() {
    Map<String, String> info = SecDataFetcher.getCompanyInfoForCik("0000320193");

    String mailing = info.get("mailing_address");
    String business = info.get("business_address");

    assertNotNull(mailing, "mailing_address should be populated for Apple");
    assertNotNull(business, "business_address should be populated for Apple");

    assertTrue(mailing.contains("CUPERTINO"), "mailing_address missing city: " + mailing);
    assertTrue(mailing.contains("CA"), "mailing_address missing state: " + mailing);
    assertTrue(mailing.contains("95014"), "mailing_address missing ZIP: " + mailing);

    assertTrue(business.contains("CUPERTINO"), "business_address missing city: " + business);
    assertTrue(business.contains("CA"), "business_address missing state: " + business);
    assertTrue(business.contains("95014"), "business_address missing ZIP: " + business);
  }

  @Test
  void microsoftAddressIncludesCityStateZip() {
    Map<String, String> info = SecDataFetcher.getCompanyInfoForCik("0000789019");

    String mailing = info.get("mailing_address");
    String business = info.get("business_address");

    assertNotNull(mailing, "mailing_address should be populated for Microsoft");
    assertNotNull(business, "business_address should be populated for Microsoft");

    assertTrue(mailing.contains("REDMOND"), "mailing_address missing city: " + mailing);
    assertTrue(mailing.contains("WA"), "mailing_address missing state: " + mailing);

    assertTrue(business.contains("REDMOND"), "business_address missing city: " + business);
    assertTrue(business.contains("WA"), "business_address missing state: " + business);
  }
}
