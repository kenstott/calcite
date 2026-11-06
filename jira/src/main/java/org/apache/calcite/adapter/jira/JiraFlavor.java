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
package org.apache.calcite.adapter.jira;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * What differs between Jira deployments: the REST API version, how a request is authorized, and
 * whether rich-text fields travel as Atlassian Document Format. Cloud is the only flavour so far;
 * Data Center (REST v2, a personal access token as a bearer, rich text as wiki markup) is a second
 * instance of this class, not a second code path.
 */
final class JiraFlavor {

  /** Jira Cloud: REST v3, basic auth with the account's email and an API token, ADF text. */
  static JiraFlavor cloud(String email, String apiToken) {
    final String credentials = Base64.getEncoder()
        .encodeToString((email + ":" + apiToken).getBytes(StandardCharsets.UTF_8));
    return new JiraFlavor("/rest/api/3", "Basic " + credentials, email, true);
  }

  /** Path prefix of every REST call, e.g. {@code /rest/api/3}. */
  final String apiBase;
  private final String authorization;
  /** Who the requests are made as, for messages; never the secret. */
  final String identity;
  /** Whether rich-text fields are read and written as Atlassian Document Format. */
  final boolean documentFormat;

  private JiraFlavor(String apiBase, String authorization, String identity,
      boolean documentFormat) {
    this.apiBase = apiBase;
    this.authorization = authorization;
    this.identity = identity;
    this.documentFormat = documentFormat;
  }

  void authorize(HttpRequest.Builder request) {
    request.header("Authorization", authorization);
  }
}
