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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage tests for {@link SharePointLegacyTokenManager}.
 * Tests constructor variants, token refresh, and realm discovery.
 */
@Tag("unit")
public class SharePointLegacyTokenManagerCoverageTest {

  @Test public void testConstructorWithRealm() {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    assertNotNull(mgr);
    assertEquals("https://mysite.sharepoint.com/sites/test", mgr.getSiteUrl());
  }

  @Test public void testConstructorWithoutRealm() {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test");

    assertNotNull(mgr);
    assertEquals("https://mysite.sharepoint.com/sites/test", mgr.getSiteUrl());
  }

  @Test public void testRealmFieldWithRealm() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    Field realmField = SharePointLegacyTokenManager.class.getDeclaredField("realm");
    realmField.setAccessible(true);
    assertEquals("my-realm", realmField.get(mgr));
  }

  @Test public void testRealmFieldWithoutRealm() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test");

    Field realmField = SharePointLegacyTokenManager.class.getDeclaredField("realm");
    realmField.setAccessible(true);
    assertNull(realmField.get(mgr));
  }

  @Test public void testRefreshWithClientCredentialsWithRealmFails() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    Method method = SharePointLegacyTokenManager.class.getDeclaredMethod(
        "refreshWithClientCredentials");
    method.setAccessible(true);

    try {
      method.invoke(mgr);
      fail("Expected exception for unreachable Azure ACS");
    } catch (Exception e) {
      // Expected - cannot reach Azure ACS endpoint in unit test
      assertNotNull(e);
    }
  }

  @Test public void testRefreshWithClientCredentialsWithoutRealmFails() throws Exception {
    // Use a non-resolvable host to ensure the request fails
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret",
        "https://this-host-does-not-exist-99999.sharepoint.invalid/sites/test");

    Method method = SharePointLegacyTokenManager.class.getDeclaredMethod(
        "refreshWithClientCredentials");
    method.setAccessible(true);

    try {
      method.invoke(mgr);
      fail("Expected exception for unreachable SharePoint (realm discovery)");
    } catch (Exception e) {
      // Expected - cannot reach SharePoint for realm discovery in unit test
      assertNotNull(e);
    }
  }

  @Test public void testDiscoverRealmWithUnresolvableHost() throws Exception {
    // Use a host that definitely won't resolve to test the error path
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret",
        "https://this-host-does-not-exist-99999.sharepoint.invalid/sites/test");

    Method method = SharePointLegacyTokenManager.class.getDeclaredMethod("discoverRealm");
    method.setAccessible(true);

    try {
      method.invoke(mgr);
      fail("Expected exception for unreachable SharePoint");
    } catch (Exception e) {
      assertNotNull(e);
    }
  }

  @Test public void testDiscoverRealmMethodExists() throws Exception {
    // Verify the discoverRealm method exists and is accessible via reflection
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test");

    Method method = SharePointLegacyTokenManager.class.getDeclaredMethod("discoverRealm");
    method.setAccessible(true);
    assertNotNull(method);
  }

  @Test public void testMakeLegacyTokenRequestFails() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    Method method = SharePointLegacyTokenManager.class.getDeclaredMethod(
        "makeLegacyTokenRequest", String.class, String.class);
    method.setAccessible(true);

    try {
      method.invoke(mgr, "https://localhost:1/token", "grant_type=client_credentials");
      fail("Expected exception for unreachable endpoint");
    } catch (Exception e) {
      assertNotNull(e);
    }
  }

  @Test public void testInvalidateToken() {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    // Should not throw
    mgr.invalidateToken();
  }

  @Test public void testGetAccessTokenNoCredentials() {
    // Create a static token manager via parent constructor
    SharePointTokenManager mgr = new SharePointTokenManager(
        "static-token", "https://mysite.sharepoint.com/sites/test");

    try {
      String token = mgr.getAccessToken();
      assertEquals("static-token", token);
    } catch (Exception e) {
      fail("Static token should be returned without error: " + e.getMessage());
    }
  }

  @Test public void testMapperFieldExists() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    Field mapperField = SharePointLegacyTokenManager.class.getDeclaredField("mapper");
    mapperField.setAccessible(true);
    assertNotNull(mapperField.get(mgr));
  }

  @Test public void testParentClassFields() throws Exception {
    SharePointLegacyTokenManager mgr = new SharePointLegacyTokenManager(
        "client-id", "client-secret", "https://mysite.sharepoint.com/sites/test", "my-realm");

    // Verify parent fields are set correctly
    Field clientIdField = SharePointTokenManager.class.getDeclaredField("clientId");
    clientIdField.setAccessible(true);
    assertTrue(clientIdField.get(mgr) != null);

    Field clientSecretField = SharePointTokenManager.class.getDeclaredField("clientSecret");
    clientSecretField.setAccessible(true);
    assertTrue(clientSecretField.get(mgr) != null);
  }
}
