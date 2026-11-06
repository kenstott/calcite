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
package org.apache.calcite.adapter.servicenow;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * How requests to the instance are authenticated.
 *
 * <p>This is the seam for OAuth client credentials (a token request to {@code /oauth_token.do},
 * then a bearer header, re-authenticating once on 401). Only HTTP Basic is implemented in this
 * release; {@link ServiceNowSchemaFactory} rejects any other {@code authType} by name instead of
 * guessing from which operands are present.
 */
interface ServiceNowAuth {

  /** Adds the credentials to a request. */
  void authorize(HttpRequest.Builder request);

  /**
   * Who the requests run as. Part of the catalog cache scope, because the tables and columns a
   * user can read depend on that user's roles and ACLs.
   */
  String identity();

  /** HTTP Basic authentication with a user name and password. */
  static ServiceNowAuth basic(String username, String password) {
    if (username == null || username.isEmpty()) {
      throw new IllegalArgumentException("Basic authentication needs a non-empty 'username'");
    }
    if (password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Basic authentication needs a non-empty 'password'");
    }
    final String header = "Basic " + Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    return new ServiceNowAuth() {
      @Override public void authorize(HttpRequest.Builder request) {
        request.header("Authorization", header);
      }

      @Override public String identity() {
        return "basic:" + username;
      }
    };
  }
}
