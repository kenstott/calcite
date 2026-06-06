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
package org.apache.calcite.adapter.file.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage tests for {@link SharePointCertificateTokenManager}.
 * Tests constructor, client assertion generation, and error paths.
 *
 * <p>Note: Tests that require a valid PFX certificate file use keytool to
 * generate a self-signed certificate for testing purposes.
 */
@Tag("unit")
public class SharePointCertificateTokenManagerCoverageTest {

  /**
   * Creates a self-signed PFX certificate file using keytool.
   */
  private String createTestPfxFile(Path tempDir, String password) throws Exception {
    String pfxPath = tempDir.resolve("test.pfx").toString();

    // Use keytool to create a self-signed certificate
    ProcessBuilder pb = new ProcessBuilder(
        "keytool",
        "-genkeypair",
        "-alias", "test",
        "-keyalg", "RSA",
        "-keysize", "2048",
        "-storetype", "PKCS12",
        "-keystore", pfxPath,
        "-storepass", password,
        "-dname", "CN=Test, OU=Test, O=Test, L=Test, ST=Test, C=US",
        "-validity", "365"
    );
    pb.redirectErrorStream(true);
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      byte[] output = process.getInputStream().readAllBytes();
      throw new RuntimeException("keytool failed with exit code " + exitCode
          + ": " + new String(output));
    }

    assertTrue(new File(pfxPath).exists(), "PFX file should exist");
    return pfxPath;
  }

  @Test public void testConstructorWithValidPfx(@TempDir Path tempDir) throws Exception {
    String password = "testpass";
    String pfxPath = createTestPfxFile(tempDir, password);

    SharePointCertificateTokenManager mgr = new SharePointCertificateTokenManager(
        "tenant-id", "client-id", pfxPath, password,
        "https://mysite.sharepoint.com/sites/test");

    assertNotNull(mgr);
    assertNotNull(mgr.getSiteUrl());

    // Verify internal fields via reflection
    Field privateKeyField = SharePointCertificateTokenManager.class
        .getDeclaredField("privateKey");
    privateKeyField.setAccessible(true);
    assertNotNull(privateKeyField.get(mgr));

    Field certField = SharePointCertificateTokenManager.class
        .getDeclaredField("certificate");
    certField.setAccessible(true);
    assertNotNull(certField.get(mgr));

    Field thumbprintField = SharePointCertificateTokenManager.class
        .getDeclaredField("thumbprint");
    thumbprintField.setAccessible(true);
    String thumbprint = (String) thumbprintField.get(mgr);
    assertNotNull(thumbprint);
    assertTrue(thumbprint.length() > 0, "Thumbprint should not be empty");
  }

  @Test public void testConstructorWithNonExistentFile() {
    try {
      new SharePointCertificateTokenManager(
          "tenant-id", "client-id", "/nonexistent/path/cert.pfx", "pass",
          "https://mysite.sharepoint.com/sites/test");
      fail("Expected exception for non-existent certificate file");
    } catch (Exception e) {
      // Expected - file not found
      assertNotNull(e);
    }
  }

  @Test public void testCreateClientAssertion(@TempDir Path tempDir) throws Exception {
    String password = "testpass";
    String pfxPath = createTestPfxFile(tempDir, password);

    SharePointCertificateTokenManager mgr = new SharePointCertificateTokenManager(
        "tenant-id", "client-id", pfxPath, password,
        "https://mysite.sharepoint.com/sites/test");

    // Access createClientAssertion via reflection
    Method method = SharePointCertificateTokenManager.class.getDeclaredMethod(
        "createClientAssertion", String.class);
    method.setAccessible(true);

    String jwt = (String) method.invoke(mgr,
        "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token");
    assertNotNull(jwt);

    // JWT should have 3 parts separated by dots
    String[] parts = jwt.split("\\.");
    assertTrue(parts.length == 3, "JWT should have 3 parts, got " + parts.length);
  }

  @Test public void testRefreshWithClientCredentialsFails(@TempDir Path tempDir) throws Exception {
    String password = "testpass";
    String pfxPath = createTestPfxFile(tempDir, password);

    SharePointCertificateTokenManager mgr = new SharePointCertificateTokenManager(
        "tenant-id", "client-id", pfxPath, password,
        "https://mysite.sharepoint.com/sites/test");

    // Call refreshWithClientCredentials - will fail because Azure AD is not reachable
    Method method = SharePointCertificateTokenManager.class.getDeclaredMethod(
        "refreshWithClientCredentials");
    method.setAccessible(true);

    try {
      method.invoke(mgr);
      fail("Expected IOException for unreachable Azure AD");
    } catch (Exception e) {
      // Expected - cannot reach Azure AD in unit test
      assertNotNull(e);
    }
  }

  @Test public void testMakeTokenRequestFails(@TempDir Path tempDir) throws Exception {
    String password = "testpass";
    String pfxPath = createTestPfxFile(tempDir, password);

    SharePointCertificateTokenManager mgr = new SharePointCertificateTokenManager(
        "tenant-id", "client-id", pfxPath, password,
        "https://mysite.sharepoint.com/sites/test");

    // Access makeTokenRequest via reflection
    Method method = SharePointCertificateTokenManager.class.getDeclaredMethod(
        "makeTokenRequest", String.class, String.class);
    method.setAccessible(true);

    try {
      // Use a URL that will fail - unreachable host
      method.invoke(mgr, "https://localhost:1/token", "grant_type=client_credentials");
      fail("Expected IOException for unreachable endpoint");
    } catch (Exception e) {
      assertNotNull(e);
    }
  }

  @Test public void testGetSiteUrl(@TempDir Path tempDir) throws Exception {
    String password = "testpass";
    String pfxPath = createTestPfxFile(tempDir, password);

    SharePointCertificateTokenManager mgr = new SharePointCertificateTokenManager(
        "tenant-id", "client-id", pfxPath, password,
        "https://mysite.sharepoint.com/sites/test");

    assertTrue(mgr.getSiteUrl().contains("sharepoint.com"));
  }
}
