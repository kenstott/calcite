/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.sharepoint.auth;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * CERTIFICATE auth with and without a PFX password.
 *
 * <p>A PKCS#12 file exported without a password is protected by the empty password. The factory
 * refused CERTIFICATE auth whenever certificatePassword was absent, so a password-less PFX could
 * never authenticate. The fixtures are generated per run (keytool, then re-saved under the empty
 * password), so no key material is checked in.
 */
class CertificateAuthPasswordTest {
  private static final String PASSWORD = "test-password";

  @TempDir static Path dir;
  private static Path withPassword;
  private static Path passwordless;

  @BeforeAll static void makeFixtures() throws Exception {
    withPassword = dir.resolve("with-password.pfx");
    String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
    Process p = new ProcessBuilder(keytool, "-genkeypair", "-alias", "test", "-keyalg", "RSA",
        "-keysize", "2048", "-dname", "CN=calcite-sharepoint-test", "-validity", "2",
        "-storetype", "PKCS12", "-keystore", withPassword.toString(),
        "-storepass", PASSWORD, "-keypass", PASSWORD)
        .inheritIO().start();
    assertEquals(0, p.waitFor(), "keytool -genkeypair");

    KeyStore source = KeyStore.getInstance("PKCS12");
    try (FileInputStream in = new FileInputStream(withPassword.toFile())) {
      source.load(in, PASSWORD.toCharArray());
    }
    Key key = source.getKey("test", PASSWORD.toCharArray());
    Certificate[] chain = source.getCertificateChain("test");
    KeyStore empty = KeyStore.getInstance("PKCS12");
    empty.load(null, null);
    empty.setKeyEntry("test", key, new char[0], chain);
    passwordless = dir.resolve("passwordless.pfx");
    try (FileOutputStream out = new FileOutputStream(passwordless.toFile())) {
      empty.store(out, new char[0]);
    }
  }

  private static Map<String, Object> config(Path pfx) {
    Map<String, Object> config = new HashMap<>();
    config.put("authType", "CERTIFICATE");
    config.put("clientId", "00000000-0000-0000-0000-000000000001");
    config.put("tenantId", "00000000-0000-0000-0000-000000000002");
    config.put("certificatePath", pfx.toString());
    return config;
  }

  @Test void passwordlessPfxLoadsWithNoPasswordGiven() {
    assertInstanceOf(CertificateAuth.class, SharePointAuthFactory.createAuth(config(passwordless)));
  }

  @Test void passwordProtectedPfxLoadsWithItsPassword() {
    Map<String, Object> config = config(withPassword);
    config.put("certificatePassword", PASSWORD);
    assertInstanceOf(CertificateAuth.class, SharePointAuthFactory.createAuth(config));
  }

  @Test void passwordProtectedPfxIsRefusedWithoutItsPassword() {
    Map<String, Object> config = config(withPassword);
    assertThrows(RuntimeException.class, () -> SharePointAuthFactory.createAuth(config));
  }
}
