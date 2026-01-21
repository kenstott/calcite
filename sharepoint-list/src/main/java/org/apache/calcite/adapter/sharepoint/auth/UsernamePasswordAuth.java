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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Username/password authentication for SharePoint.
 */
public class UsernamePasswordAuth implements SharePointAuth {
  private static final String TOKEN_ENDPOINT =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://graph.microsoft.com/.default";

  private final String clientId;
  private final String tenantId;
  private final String username;
  private final String password;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ReentrantLock tokenLock;

  private String accessToken;
  private Instant tokenExpiry;

  public UsernamePasswordAuth(String clientId, String tenantId, String username, String password) {
    this.clientId = clientId;
    this.tenantId = tenantId;
    this.username = username;
    this.password = password;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.objectMapper = new ObjectMapper();
    this.tokenLock = new ReentrantLock();
  }

  @Override public String getAccessToken() throws IOException, InterruptedException {
    tokenLock.lock();
    try {
      if (accessToken == null || isTokenExpired()) {
        refreshToken();
      }
      return accessToken;
    } finally {
      tokenLock.unlock();
    }
  }

  private boolean isTokenExpired() {
    return tokenExpiry == null || Instant.now().isAfter(tokenExpiry);
  }

  private void refreshToken() throws IOException, InterruptedException {
    String tokenUrl = TOKEN_ENDPOINT.replace("%s", tenantId);

    String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8)
        + "&username=" + URLEncoder.encode(username, StandardCharsets.UTF_8)
        + "&password=" + URLEncoder.encode(password, StandardCharsets.UTF_8)
        + "&grant_type=password";

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(tokenUrl))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(formData))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to authenticate with username/password: " + response.body());
    }

    JsonNode json = objectMapper.readTree(response.body());
    accessToken = json.get("access_token").asText();
    int expiresIn = json.get("expires_in").asInt();

    tokenExpiry = Instant.now().plusSeconds(expiresIn - 300);
  }
}
