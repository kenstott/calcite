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

import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Factory for creating SharePoint authentication instances.
 */
public final class SharePointAuthFactory {

  private SharePointAuthFactory() {
    // Utility class
  }

  public static SharePointAuth createAuth(Map<String, Object> config) {
    String authType = (String) config.get("authType");
    if (authType == null) {
      authType = "CLIENT_CREDENTIALS"; // Default
    }

    SharePointAuth.AuthType type;
    try {
      type = SharePointAuth.AuthType.valueOf(authType.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Invalid authType: " + authType + ". Valid types are: "
          + String.join(", ", getAuthTypes()));
    }

    try {
      switch (type) {
      case CLIENT_CREDENTIALS:
        return createClientCredentialsAuth(config);

      case USERNAME_PASSWORD:
        return createUsernamePasswordAuth(config);

      case CERTIFICATE:
        return createCertificateAuth(config);

      case DEVICE_CODE:
        return createDeviceCodeAuth(config);

      case MANAGED_IDENTITY:
        return createManagedIdentityAuth(config);

      default:
        throw new RuntimeException("Unsupported auth type: " + type);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create authentication: " + e.getMessage(), e);
    }
  }

  private static SharePointAuth createClientCredentialsAuth(Map<String, Object> config) {
    String clientId = (String) config.get("clientId");
    String clientSecret = (String) config.get("clientSecret");
    String tenantId = (String) config.get("tenantId");

    if (clientId == null || clientSecret == null || tenantId == null) {
      throw new RuntimeException(
          "CLIENT_CREDENTIALS auth requires clientId, clientSecret, and tenantId");
    }

    return new ClientCredentialsAuth(clientId, clientSecret, tenantId);
  }

  private static SharePointAuth createUsernamePasswordAuth(Map<String, Object> config) {
    String clientId = (String) config.get("clientId");
    String tenantId = (String) config.get("tenantId");
    String username = (String) config.get("username");
    String password = (String) config.get("password");

    if (clientId == null || tenantId == null || username == null || password == null) {
      throw new RuntimeException(
          "USERNAME_PASSWORD auth requires clientId, tenantId, username, and password");
    }

    return new UsernamePasswordAuth(clientId, tenantId, username, password);
  }

  private static SharePointAuth createCertificateAuth(Map<String, Object> config) throws Exception {
    String clientId = (String) config.get("clientId");
    String tenantId = (String) config.get("tenantId");
    String certificatePath = (String) config.get("certificatePath");
    String certificatePassword = (String) config.get("certificatePassword");
    String thumbprint = (String) config.get("thumbprint");

    if (clientId == null || tenantId == null || certificatePath == null
        || certificatePassword == null) {
      throw new RuntimeException("CERTIFICATE auth requires clientId, tenantId, certificatePath, "
          + "and certificatePassword");
    }

    return new CertificateAuth(clientId, tenantId, certificatePath,
        certificatePassword, thumbprint);
  }

  private static SharePointAuth createDeviceCodeAuth(Map<String, Object> config) {
    String clientId = (String) config.get("clientId");
    String tenantId = (String) config.get("tenantId");

    if (clientId == null || tenantId == null) {
      throw new RuntimeException(
          "DEVICE_CODE auth requires clientId and tenantId");
    }

    // Allow custom message handler if provided
    Consumer<String> messageHandler = null;
    if (config.containsKey("messageHandler")) {
      messageHandler = (Consumer<String>) config.get("messageHandler");
    }

    return new DeviceCodeAuth(clientId, tenantId, messageHandler);
  }

  private static SharePointAuth createManagedIdentityAuth(Map<String, Object> config) {
    String clientId = (String) config.get("clientId"); // Optional for user-assigned identity

    if (clientId != null && !clientId.isEmpty()) {
      return new ManagedIdentityAuth(clientId);
    } else {
      return new ManagedIdentityAuth();
    }
  }

  private static String[] getAuthTypes() {
    SharePointAuth.AuthType[] types = SharePointAuth.AuthType.values();
    String[] names = new String[types.length];
    for (int i = 0; i < types.length; i++) {
      names[i] = types[i].name();
    }
    return names;
  }
}
