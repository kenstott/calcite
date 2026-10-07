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
package org.apache.calcite.adapter.salesforce;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Connection to Salesforce REST API.
 */
public class SalesforceConnection implements Closeable {

  private static final String DESCRIBE_PATH = "/services/data/%s/sobjects/%s/describe";
  private static final String COLLECTIONS_PATH = "/services/data/%s/composite/sobjects";

  /** Maximum records per sObject Collections request. */
  static final int COLLECTION_BATCH_SIZE = 200;

  private final String loginUrl;
  private final AuthConfig authConfig;
  private final String apiVersion;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;

  private String accessToken;
  private String instanceUrl;

  public SalesforceConnection(String loginUrl, AuthConfig authConfig, String apiVersion)
      throws IOException {
    this.loginUrl = loginUrl;
    this.authConfig = authConfig;
    this.apiVersion = apiVersion;
    this.httpClient = HttpClients.createDefault();
    this.mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    authenticate();
  }

  private void authenticate() throws IOException {
    if (authConfig.type == AuthType.ACCESS_TOKEN) {
      this.accessToken = authConfig.accessToken;
      this.instanceUrl = authConfig.instanceUrl;
    } else {
      StringBuilder body = new StringBuilder();
      if (authConfig.type == AuthType.CLIENT_CREDENTIALS) {
        // Client credentials flow; loginUrl must be the org's My Domain URL
        body.append("grant_type=client_credentials");
        body.append("&client_id=").append(encode(authConfig.clientId));
        body.append("&client_secret=").append(encode(authConfig.clientSecret));
      } else {
        // Username/password OAuth flow
        body.append("grant_type=password");
        body.append("&client_id=").append(encode(authConfig.clientId));
        body.append("&client_secret=").append(encode(authConfig.clientSecret));
        body.append("&username=").append(encode(authConfig.username));
        body.append("&password=").append(encode(authConfig.password));
        if (authConfig.securityToken != null) {
          body.append(encode(authConfig.securityToken));
        }
      }
      requestToken(body.toString());
    }
  }

  private void requestToken(String body) throws IOException {
    HttpPost post = new HttpPost(loginUrl + "/services/oauth2/token");
    post.setHeader("Content-Type", "application/x-www-form-urlencoded");
    post.setEntity(new StringEntity(body));

    try (CloseableHttpResponse response = httpClient.execute(post)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("Authentication failed: " + responseBody);
      }

      JsonNode auth = mapper.readTree(responseBody);
      this.accessToken = auth.get("access_token").asText();
      this.instanceUrl = auth.get("instance_url").asText();
    }
  }

  /**
   * Execute a SOQL query.
   */
  public QueryResult query(String soql) throws IOException {
    URIBuilder builder = new URIBuilder(URI.create(instanceUrl))
        .setPath("/services/data/" + apiVersion + "/query")
        .addParameter("q", soql);

    HttpGet get = new HttpGet(builder.toString());
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("Query failed: " + responseBody);
      }

      return mapper.readValue(responseBody, QueryResult.class);
    } catch (Exception e) {
      throw new IOException("Query failed", e);
    }
  }

  /**
   * Continue a query using the nextRecordsUrl.
   */
  public QueryResult queryMore(String nextRecordsUrl) throws IOException {
    HttpGet get = new HttpGet(instanceUrl + nextRecordsUrl);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("QueryMore failed: " + responseBody);
      }

      return mapper.readValue(responseBody, QueryResult.class);
    } catch (Exception e) {
      throw new IOException("QueryMore failed", e);
    }
  }

  /**
   * Describe an sObject type.
   */
  public SObjectDescription describeSObject(String sObjectType) throws IOException {
    String path = String.format(Locale.ROOT, DESCRIBE_PATH, apiVersion, sObjectType);

    HttpGet get = new HttpGet(instanceUrl + path);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("Describe failed: " + responseBody);
      }

      return mapper.readValue(responseBody, SObjectDescription.class);
    } catch (Exception e) {
      throw new IOException("Describe failed", e);
    }
  }

  /**
   * Get list of all sObjects.
   */
  public List<SObjectBasicInfo> listSObjects() throws IOException {
    String path = String.format(Locale.ROOT, "/services/data/%s/sobjects", apiVersion);

    HttpGet get = new HttpGet(instanceUrl + path);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("List sObjects failed: " + responseBody);
      }

      JsonNode root = mapper.readTree(responseBody);
      JsonNode sobjects = root.get("sobjects");

      List<SObjectBasicInfo> result = new ArrayList<>();
      for (JsonNode node : sobjects) {
        SObjectBasicInfo info = mapper.treeToValue(node, SObjectBasicInfo.class);
        if (info.queryable) {
          result.add(info);
        }
      }
      return result;
    } catch (Exception e) {
      throw new IOException("List sObjects failed", e);
    }
  }

  /**
   * Creates records with the sObject Collections API. Each call is atomic
   * (allOrNone); at most {@link #COLLECTION_BATCH_SIZE} records per call.
   *
   * @return number of records created
   */
  public int createRecords(String sObjectType, List<Map<String, Object>> records)
      throws IOException {
    HttpPost post = new HttpPost(instanceUrl + collectionsPath());
    post.setEntity(collectionBody(sObjectType, records));
    return countSuccesses(execute(post, "Create"), "Create");
  }

  /**
   * Updates records with the sObject Collections API. Each record must carry
   * its {@code Id}. Each call is atomic (allOrNone); at most
   * {@link #COLLECTION_BATCH_SIZE} records per call.
   *
   * @return number of records updated
   */
  public int updateRecords(String sObjectType, List<Map<String, Object>> records)
      throws IOException {
    HttpPatch patch = new HttpPatch(instanceUrl + collectionsPath());
    patch.setEntity(collectionBody(sObjectType, records));
    return countSuccesses(execute(patch, "Update"), "Update");
  }

  /**
   * Deletes records with the sObject Collections API. Each call is atomic
   * (allOrNone); at most {@link #COLLECTION_BATCH_SIZE} ids per call.
   *
   * @return number of records deleted
   */
  public int deleteRecords(List<String> ids) throws IOException {
    checkBatchSize(ids.size());
    URIBuilder builder;
    try {
      builder = new URIBuilder(instanceUrl + collectionsPath())
          .addParameter("ids", String.join(",", ids))
          .addParameter("allOrNone", "true");
      return countSuccesses(execute(new HttpDelete(builder.build()), "Delete"), "Delete");
    } catch (URISyntaxException e) {
      throw new IOException("Delete failed", e);
    }
  }

  private String collectionsPath() {
    return String.format(Locale.ROOT, COLLECTIONS_PATH, apiVersion);
  }

  private static void checkBatchSize(int size) {
    if (size > COLLECTION_BATCH_SIZE) {
      throw new IllegalArgumentException("At most " + COLLECTION_BATCH_SIZE
          + " records per sObject Collections request, got " + size);
    }
  }

  private StringEntity collectionBody(String sObjectType,
      List<Map<String, Object>> records) throws IOException {
    checkBatchSize(records.size());
    List<Map<String, Object>> typed = new ArrayList<>();
    for (Map<String, Object> record : records) {
      Map<String, Object> attributes = new LinkedHashMap<>();
      attributes.put("type", sObjectType);
      Map<String, Object> withType = new LinkedHashMap<>();
      withType.put("attributes", attributes);
      withType.putAll(record);
      typed.add(withType);
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("allOrNone", true);
    body.put("records", typed);
    return new StringEntity(mapper.writeValueAsString(body), ContentType.APPLICATION_JSON);
  }

  private JsonNode execute(HttpRequestBase request, String action) throws IOException {
    request.setHeader("Authorization", "Bearer " + accessToken);
    request.setHeader("Accept", "application/json");
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException(action + " failed: " + responseBody);
      }
      return mapper.readTree(responseBody);
    }
  }

  /** Counts per-record successes; any failure fails the whole statement. */
  private static int countSuccesses(JsonNode results, String action) throws IOException {
    int count = 0;
    List<String> errors = new ArrayList<>();
    for (JsonNode result : results) {
      if (result.path("success").asBoolean()) {
        count++;
      } else {
        for (JsonNode error : result.path("errors")) {
          String code = error.path("statusCode").asText();
          // Under allOrNone, records that would have succeeded report this code
          if (!"ALL_OR_NONE_OPERATION_ROLLED_BACK".equals(code)) {
            errors.add(code + ": " + error.path("message").asText()
                + " " + error.path("fields"));
          }
        }
      }
    }
    if (count != results.size()) {
      throw new IOException(action + " failed: " + errors);
    }
    return count;
  }

  private String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override public void close() throws IOException {
    httpClient.close();
  }

  /**
   * Authentication configuration.
   */
  public static class AuthConfig {
    final AuthType type;
    final String username;
    final String password;
    final String securityToken;
    final String clientId;
    final String clientSecret;
    final String accessToken;
    final String instanceUrl;

    private AuthConfig(AuthType type, String username, String password,
        String securityToken, String clientId, String clientSecret,
        String accessToken, String instanceUrl) {
      this.type = type;
      this.username = username;
      this.password = password;
      this.securityToken = securityToken;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.accessToken = accessToken;
      this.instanceUrl = instanceUrl;
    }

    public static AuthConfig usernamePassword(String username, String password,
        String securityToken, String clientId, String clientSecret) {
      return new AuthConfig(AuthType.USERNAME_PASSWORD, username, password,
          securityToken, clientId, clientSecret, null, null);
    }

    /**
     * OAuth 2.0 client credentials flow. The connected app must have the
     * flow enabled with a "Run As" user, and the login URL must be the
     * org's My Domain URL, not login.salesforce.com.
     */
    public static AuthConfig clientCredentials(String clientId, String clientSecret) {
      return new AuthConfig(AuthType.CLIENT_CREDENTIALS, null, null, null,
          clientId, clientSecret, null, null);
    }

    public static AuthConfig accessToken(String accessToken, String instanceUrl) {
      return new AuthConfig(AuthType.ACCESS_TOKEN, null, null, null, null, null,
          accessToken, instanceUrl);
    }
  }

  /**
   * Authentication type for Salesforce connection.
   */
  private enum AuthType {
    USERNAME_PASSWORD,
    CLIENT_CREDENTIALS,
    ACCESS_TOKEN
  }

  /**
   * SOQL query result.
   */
  public static class QueryResult {
    public int totalSize;
    public boolean done;
    public String nextRecordsUrl;
    public List<Map<String, Object>> records;
  }

  /**
   * Basic sObject information.
   */
  public static class SObjectBasicInfo {
    public String name;
    public String label;
    public boolean queryable;
    public boolean custom;
  }

  /**
   * Full sObject description.
   */
  public static class SObjectDescription {
    public String name;
    public String label;
    public List<FieldDescription> fields;
    public boolean queryable;
    public boolean custom;
  }

  /**
   * Field description.
   */
  public static class FieldDescription {
    public String name;
    public String label;
    public String type;
    public int length;
    public boolean nillable;
    public boolean createable;
    public boolean updateable;
    public boolean defaultedOnCreate;
    public boolean custom;
    public boolean sortable;
    public boolean filterable;
    public String relationshipName;
    public List<String> referenceTo;
  }
}
