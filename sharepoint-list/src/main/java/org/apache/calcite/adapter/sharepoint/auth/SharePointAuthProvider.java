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
package org.apache.calcite.adapter.sharepoint.auth;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for SharePoint authentication providers.
 * Allows pluggable authentication mechanisms including direct OAuth2,
 * auth proxy services, and custom enterprise implementations.
 *
 * <p>Follows the same phased approach as the file adapter's HTTP auth:
 * <ul>
 *   <li>Phase 1: Simple static authentication (client credentials, certificates)</li>
 *   <li>Phase 2: External token management (commands, files, endpoints)</li>
 *   <li>Phase 3: Full proxy delegation for complex auth protocols</li>
 * </ul>
 */
public interface SharePointAuthProvider {

  /**
   * Gets a valid access token for SharePoint API access.
   * Implementations should handle token refresh internally.
   *
   * @return Valid access token
   * @throws IOException if authentication fails
   */
  String getAccessToken() throws IOException;

  /**
   * Gets additional headers to include in API requests.
   * For example, custom correlation IDs or proxy headers.
   *
   * @return Map of header names to values, or empty map
   */
  Map<String, String> getAdditionalHeaders();

  /**
   * Invalidates the current token, forcing refresh on next request.
   */
  void invalidateToken();

  /**
   * Gets the SharePoint site URL this provider is configured for.
   *
   * @return SharePoint site URL
   */
  String getSiteUrl();

  /**
   * Determines if this provider supports the given API type.
   *
   * @param apiType Either "graph" or "rest"
   * @return true if the provider supports the API type
   */
  boolean supportsApiType(String apiType);

  /**
   * Gets the tenant ID if available.
   *
   * @return Tenant ID or null if not applicable
   */
  String getTenantId();
}
