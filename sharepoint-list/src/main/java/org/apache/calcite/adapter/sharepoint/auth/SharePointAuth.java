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

/**
 * Interface for SharePoint authentication methods.
 */
public interface SharePointAuth {
  String getAccessToken() throws IOException, InterruptedException;

  /**
   * Authentication types supported by the SharePoint adapter.
   */
  enum AuthType {
    CLIENT_CREDENTIALS,
    USERNAME_PASSWORD,
    CERTIFICATE,
    DEVICE_CODE,
    MANAGED_IDENTITY
  }
}
