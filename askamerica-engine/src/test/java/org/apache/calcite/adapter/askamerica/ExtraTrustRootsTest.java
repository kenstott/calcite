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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ExtraTrustRootsTest {
  @Test void bundledRootsParseAndFactoryBuilds() throws Exception {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    String[] expected = {
        "CN=GlobalSign Root R46, O=GlobalSign nv-sa, C=BE",
        "CN=Sectigo Public Server Authentication Root R46, O=Sectigo Limited, C=GB",
        "CN=CFCA EV ROOT, O=China Financial Certification Authority, C=CN"};
    String[] files = {"globalsign-root-r46.pem",
        "sectigo-public-server-authentication-root-r46.pem", "cfca-ev-root.pem"};
    for (int i = 0; i < files.length; i++) {
      try (InputStream in = getClass().getClassLoader()
          .getResourceAsStream("extra-trust-roots/" + files[i])) {
        assertNotNull(in, files[i]);
        X509Certificate c = (X509Certificate) cf.generateCertificate(in);
        assertEquals(expected[i], c.getSubjectX500Principal().getName("RFC1779"));
        c.checkValidity();
      }
    }
    assertNotNull(ExtraTrustRoots.socketFactory());
  }
}
