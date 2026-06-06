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

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Token manager specifically for Microsoft Graph API access.
 * Uses Graph API scopes instead of SharePoint-specific scopes.
 */
public class MicrosoftGraphTokenManager extends SharePointTokenManager {

  /**
   * Creates a token manager for client credentials flow (app-only) with Graph API.
   */
  public MicrosoftGraphTokenManager(String tenantId, String clientId, String clientSecret, String siteUrl) {
    super(tenantId, clientId, clientSecret, siteUrl);
  }

  /**
   * Creates a token manager for user delegated flow with refresh token for Graph API.
   */
  public MicrosoftGraphTokenManager(String tenantId, String clientId, String refreshToken,
                                   String siteUrl, boolean isRefreshToken) {
    super(tenantId, clientId, refreshToken, siteUrl, isRefreshToken);
  }

  /**
   * Creates a token manager with a static token for Graph API.
   */
  public MicrosoftGraphTokenManager(String accessToken, String siteUrl) {
    super(accessToken, siteUrl);
  }

  /**
   * Refreshes token using client credentials (app-only) flow with Microsoft Graph scope.
   */
  @Override protected void refreshWithClientCredentials() throws IOException {
    String tokenUrl =
                                   String.format(Locale.ROOT, "https://login.microsoftonline.com/%s/oauth2/v2.0/token", super.tenantId);

    // Use Microsoft Graph API scope
    String scope = "https://graph.microsoft.com/.default";

    // Build request body
    String requestBody =
        String.format(Locale.ROOT, "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
        URLEncoder.encode(super.clientId, StandardCharsets.UTF_8),
        URLEncoder.encode(super.clientSecret, StandardCharsets.UTF_8),
        URLEncoder.encode(scope, StandardCharsets.UTF_8));

    Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
    updateTokenFromResponse(response);
  }

  /**
   * Makes a token request to Azure AD.
   */
  private Map<String, Object> makeTokenRequest(String tokenUrl, String requestBody)
      throws IOException {
    // Use reflection to call the private method from parent class
    try {
      java.lang.reflect.Method method = SharePointTokenManager.class
          .getDeclaredMethod("makeTokenRequest", String.class, String.class);
      method.setAccessible(true);
      return (Map<String, Object>) method.invoke(this, tokenUrl, requestBody);
    } catch (Exception e) {
      throw new IOException("Failed to make token request", e);
    }
  }

  /**
   * Updates token and expiry from Azure AD response.
   */
  private void updateTokenFromResponse(Map<String, Object> response) {
    // Use reflection to call the private method from parent class
    try {
      java.lang.reflect.Method method = SharePointTokenManager.class
          .getDeclaredMethod("updateTokenFromResponse", Map.class);
      method.setAccessible(true);
      method.invoke(this, response);
    } catch (Exception e) {
      throw new RuntimeException("Failed to update token from response", e);
    }
  }
}
