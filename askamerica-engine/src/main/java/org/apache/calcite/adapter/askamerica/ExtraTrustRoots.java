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

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS socket factory for outbound fetches that trusts the running JRE's own CA store plus the
 * public roots listed in {@code extra-trust-roots/index.txt}. The engine ships a jlink runtime
 * whose cacerts is frozen at build time, so a public root issued after that JDK build is absent
 * and a site chaining to it fails with PKIX path building. The extra roots are public CA
 * certificates, added alongside the JRE's roots, never in place of them.
 */
final class ExtraTrustRoots {
  private static final String DIR = "extra-trust-roots/";

  private ExtraTrustRoots() {
  }

  /** Lazily built, process-wide factory; holder idiom keeps construction thread-safe. */
  private static final class Holder {
    static final SSLSocketFactory FACTORY = build();
  }

  static SSLSocketFactory socketFactory() {
    return Holder.FACTORY;
  }

  private static SSLSocketFactory build() {
    try {
      TrustManagerFactory jre = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      jre.init((KeyStore) null);
      KeyStore merged = KeyStore.getInstance(KeyStore.getDefaultType());
      merged.load(null, null);
      int n = 0;
      for (javax.net.ssl.TrustManager tm : jre.getTrustManagers()) {
        if (tm instanceof javax.net.ssl.X509TrustManager) {
          for (X509Certificate c : ((javax.net.ssl.X509TrustManager) tm).getAcceptedIssuers()) {
            merged.setCertificateEntry("jre-" + n++, c);
          }
        }
      }
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      for (String name : readIndex()) {
        try (InputStream in = resource(DIR + name)) {
          for (Certificate c : cf.generateCertificates(in)) {
            merged.setCertificateEntry("extra-" + name + "-" + n++, c);
          }
        }
      }
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(merged);
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, tmf.getTrustManagers(), null);
      return ctx.getSocketFactory();
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Cannot build the web_fetch TLS trust store", e);
    }
  }

  private static java.util.List<String> readIndex() throws IOException {
    java.util.List<String> names = new java.util.ArrayList<>();
    try (InputStream in = resource(DIR + "index.txt")) {
      for (String line : new String(in.readAllBytes(),
          java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
        String t = line.trim();
        if (!t.isEmpty() && !t.startsWith("#")) {
          names.add(t);
        }
      }
    }
    return names;
  }

  private static InputStream resource(String path) throws IOException {
    InputStream in = ExtraTrustRoots.class.getClassLoader().getResourceAsStream(path);
    if (in == null) {
      throw new IOException("Missing bundled resource " + path);
    }
    return in;
  }
}
