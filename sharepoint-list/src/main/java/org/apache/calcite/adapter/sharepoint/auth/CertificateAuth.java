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

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * Certificate-based authentication for SharePoint.
 */
public class CertificateAuth implements SharePointAuth {
  private static final String TOKEN_ENDPOINT =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://graph.microsoft.com/.default";

  private final String clientId;
  private final String tenantId;
  private final String certificatePath;
  private final String certificatePassword;
  private final String thumbprint;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ReentrantLock tokenLock;

  private String accessToken;
  private Instant tokenExpiry;
  private KeyStore keyStore;

  public CertificateAuth(String clientId, String tenantId, String certificatePath,
                        String certificatePassword, String thumbprint) throws Exception {
    this.clientId = clientId;
    this.tenantId = tenantId;
    this.certificatePath = certificatePath;
    this.certificatePassword = certificatePassword;
    this.objectMapper = new ObjectMapper();
    this.tokenLock = new ReentrantLock();

    // Load certificate
    this.keyStore = KeyStore.getInstance("PKCS12");
    try (FileInputStream fis = new FileInputStream(certificatePath)) {
      keyStore.load(fis, certificatePassword.toCharArray());
    }

    // Auto-calculate thumbprint if not provided
    if (thumbprint == null || "auto".equals(thumbprint)) {
      this.thumbprint = calculateThumbprint();
    } else {
      this.thumbprint = thumbprint;
    }

    // Create SSL context with client certificate
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, certificatePassword.toCharArray());

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), null, null);

    this.httpClient = HttpClient.newBuilder()
        .sslContext(sslContext)
        .connectTimeout(Duration.ofSeconds(30))
        .build();
  }

  private String calculateThumbprint() throws Exception {
    // Get the first alias (there should only be one)
    String alias = keyStore.aliases().nextElement();

    // Get certificate
    java.security.cert.X509Certificate certificate =
        (java.security.cert.X509Certificate) keyStore.getCertificate(alias);

    // Calculate thumbprint (SHA-1 hash of certificate)
    java.security.MessageDigest sha1 = java.security.MessageDigest.getInstance("SHA-1");
    byte[] certHash = sha1.digest(certificate.getEncoded());
    return Base64.getUrlEncoder().withoutPadding().encodeToString(certHash);
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
    try {
      String tokenUrl = TOKEN_ENDPOINT.replace("%s", tenantId);

      // Create client assertion JWT
      String clientAssertion = createClientAssertion();

      String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
          + "&client_assertion_type="
          + URLEncoder.encode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
              StandardCharsets.UTF_8)
          + "&client_assertion=" + URLEncoder.encode(clientAssertion, StandardCharsets.UTF_8)
          + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8)
          + "&grant_type=client_credentials";

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(tokenUrl))
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(formData))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new IOException("Failed to authenticate with certificate: "
            + response.body());
      }

      JsonNode json = objectMapper.readTree(response.body());
      accessToken = json.get("access_token").asText();
      int expiresIn = json.get("expires_in").asInt();

      tokenExpiry = Instant.now().plusSeconds(expiresIn - 300);
    } catch (Exception e) {
      throw new IOException("Certificate authentication failed", e);
    }
  }

  private String createClientAssertion() throws Exception {
    long now = System.currentTimeMillis() / 1000;

    java.security.Key key =
        keyStore.getKey(keyStore.aliases().nextElement(),
        certificatePassword.toCharArray());

    return Jwts.builder()
        .setId(UUID.randomUUID().toString())
        .setIssuer(clientId)
        .setSubject(clientId)
        .setAudience("https://login.microsoftonline.com/" + tenantId
            + "/oauth2/v2.0/token")
        .setIssuedAt(new java.util.Date(now * 1000))
        .setNotBefore(new java.util.Date(now * 1000))
        .setExpiration(new java.util.Date((now + 600) * 1000))
        .claim("x5t", thumbprint)
        .signWith(key, SignatureAlgorithm.RS256)
        .compact();
  }
}
