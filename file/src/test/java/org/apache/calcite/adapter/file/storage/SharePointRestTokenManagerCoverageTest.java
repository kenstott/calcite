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

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage tests for {@link SharePointRestTokenManager}.
 * Tests constructors and method coverage.
 */
@Tag("unit")
public class SharePointRestTokenManagerCoverageTest {

  @Test public void testConstructorClientCredentials() {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "tenant-id", "client-id", "client-secret",
        "https://mysite.sharepoint.com/sites/test");
    assertNotNull(mgr);
    assertEquals("https://mysite.sharepoint.com/sites/test", mgr.getSiteUrl());
  }

  @Test public void testConstructorRefreshToken() {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "tenant-id", "client-id", "refresh-token",
        "https://mysite.sharepoint.com/sites/test", true);
    assertNotNull(mgr);
    assertEquals("https://mysite.sharepoint.com/sites/test", mgr.getSiteUrl());
  }

  @Test public void testConstructorStaticToken() {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "static-access-token", "https://mysite.sharepoint.com/sites/test");
    assertNotNull(mgr);
    assertEquals("https://mysite.sharepoint.com/sites/test", mgr.getSiteUrl());
  }

  @Test public void testRefreshWithClientCredentialsFails() throws Exception {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "tenant-id", "client-id", "client-secret",
        "https://mysite.sharepoint.com/sites/test");

    Method method = SharePointRestTokenManager.class.getDeclaredMethod(
        "refreshWithClientCredentials");
    method.setAccessible(true);

    try {
      method.invoke(mgr);
      fail("Expected exception for unreachable Azure AD");
    } catch (Exception e) {
      assertNotNull(e);
    }
  }

  @Test public void testMakeTokenRequestViaReflectionFails() throws Exception {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "tenant-id", "client-id", "client-secret",
        "https://mysite.sharepoint.com/sites/test");

    // Access the private makeTokenRequest in SharePointRestTokenManager
    Method method = SharePointRestTokenManager.class.getDeclaredMethod(
        "makeTokenRequest", String.class, String.class);
    method.setAccessible(true);

    try {
      method.invoke(mgr, "https://localhost:1/token", "body");
      fail("Expected exception");
    } catch (Exception e) {
      assertNotNull(e);
    }
  }

  @Test public void testUpdateTokenFromResponseViaReflection() throws Exception {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "tenant-id", "client-id", "client-secret",
        "https://mysite.sharepoint.com/sites/test");

    // The updateTokenFromResponse calls parent's private method via reflection
    // We test that the method exists and is accessible
    Method method = SharePointRestTokenManager.class.getDeclaredMethod(
        "updateTokenFromResponse", java.util.Map.class);
    method.setAccessible(true);

    java.util.Map<String, Object> response = new java.util.HashMap<String, Object>();
    response.put("access_token", "test-token");
    response.put("expires_in", 3600);

    // This should successfully update the token via reflection
    method.invoke(mgr, response);

    // Verify token was set
    String token = mgr.getAccessToken();
    assertEquals("test-token", token);
  }

  @Test public void testStaticTokenAccess() throws Exception {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "my-static-token", "https://mysite.sharepoint.com/sites/test");

    String token = mgr.getAccessToken();
    assertEquals("my-static-token", token);
  }

  @Test public void testInvalidateToken() {
    SharePointRestTokenManager mgr = new SharePointRestTokenManager(
        "my-static-token", "https://mysite.sharepoint.com/sites/test");
    mgr.invalidateToken();
    // After invalidation, the next getAccessToken call would try to refresh
    // but since it's a static token, it may succeed or fail depending on expiry
  }
}
