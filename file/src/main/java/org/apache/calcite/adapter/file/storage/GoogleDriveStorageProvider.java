/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read-only storage provider over Google Drive (Drive API v3).
 *
 * <p>Paths are resolved by name, segment by segment, below a root folder given by
 * {@code folderId} (a My Drive folder shared with the caller, or a shared drive / folder in
 * one). The root folder itself is the empty path.
 *
 * <p>Configuration ({@code storageType: "gdrive"}):
 * <ul>
 *   <li>{@code folderId} (required) - id of the root folder.</li>
 *   <li>Exactly one sign-in:
 *     <ul>
 *       <li>{@code serviceAccountKey} - the service account JSON key, as a string or an object;</li>
 *       <li>{@code serviceAccountKeyPath} - path to that JSON key file;</li>
 *       <li>{@code clientId} + {@code clientSecret} + {@code refreshToken} - OAuth user
 *           sign-in;</li>
 *       <li>{@code accessToken} - a static bearer token.</li>
 *     </ul>
 *   </li>
 *   <li>With a service account key only:
 *     <ul>
 *       <li>{@code subject} - a Workspace user's email; the service account acts as that user
 *           through domain-wide delegation (the JWT {@code sub} claim);</li>
 *       <li>{@code scope} - the OAuth scope(s) to request, as a space-separated string or a list
 *           (default {@code https://www.googleapis.com/auth/drive.readonly}). With delegation,
 *           only the scopes the Workspace admin granted the client are issued.</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>Google Workspace native files (Docs, Sheets, Slides) have no binary content; they are
 * listed, but reading one fails, naming the file.
 */
public class GoogleDriveStorageProvider implements StorageProvider {

  static final String DRIVE_API_BASE = "https://www.googleapis.com/drive/v3";
  static final String GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
  static final String DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly";

  private static final String FOLDER_MIME = "application/vnd.google-apps.folder";
  private static final String NATIVE_MIME_PREFIX = "application/vnd.google-apps.";
  private static final String ITEM_FIELDS = "id,name,mimeType,size,modifiedTime,version";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Supplies a current bearer token. */
  interface TokenSource {
    String getAccessToken() throws IOException;
  }

  /** One Drive item. */
  static final class DriveItem {
    final String id;
    final String name;
    final String mimeType;
    final long size;
    final long modifiedTime;
    final String version;

    DriveItem(JsonNode node) {
      this.id = node.get("id").asText();
      this.name = node.get("name").asText();
      this.mimeType = node.get("mimeType").asText();
      // Drive omits size for folders and native Workspace files, which have no byte content.
      this.size = node.has("size") ? node.get("size").asLong() : 0L;
      this.modifiedTime = parseTime(node.get("modifiedTime").asText());
      this.version = node.get("version").asText();
    }

    boolean isFolder() {
      return FOLDER_MIME.equals(mimeType);
    }
  }

  private final String rootFolderId;
  private final TokenSource tokens;
  private final String apiBase;
  /** Resolved folder ids by normalized path; folders are re-resolved only on a miss. */
  private final Map<String, String> folderIds = new ConcurrentHashMap<>();

  GoogleDriveStorageProvider(String rootFolderId, TokenSource tokens, String apiBase) {
    this.rootFolderId = rootFolderId;
    this.tokens = tokens;
    this.apiBase = apiBase;
    folderIds.put("", rootFolderId);
  }

  /** Builds a provider from a {@code storageConfig} map (keys in the class comment). */
  public static GoogleDriveStorageProvider fromConfig(Map<String, Object> config) {
    if (config == null) {
      throw new IllegalArgumentException(
          "Google Drive storage requires storageConfig with 'folderId' and credentials");
    }
    Object folderId = config.get("folderId");
    if (!(folderId instanceof String) || ((String) folderId).isEmpty()) {
      throw new IllegalArgumentException("Google Drive storage requires 'folderId' in config");
    }
    return new GoogleDriveStorageProvider((String) folderId, tokenSourceFrom(config),
        DRIVE_API_BASE);
  }

  static TokenSource tokenSourceFrom(Map<String, Object> config) {
    boolean serviceAccount =
        config.containsKey("serviceAccountKey") || config.containsKey("serviceAccountKeyPath");
    if (!serviceAccount && (config.containsKey("subject") || config.containsKey("scope"))) {
      throw new IllegalArgumentException("Google Drive 'subject' and 'scope' apply only to "
          + "'serviceAccountKey' or 'serviceAccountKeyPath' sign-in");
    }
    String subject = serviceAccount ? subjectFrom(config) : null;
    String scope = serviceAccount ? scopeFrom(config) : null;
    if (config.containsKey("serviceAccountKey")) {
      Object key = config.get("serviceAccountKey");
      JsonNode keyJson;
      try {
        keyJson = key instanceof String ? MAPPER.readTree((String) key) : MAPPER.valueToTree(key);
      } catch (IOException e) {
        throw new IllegalArgumentException("Google Drive 'serviceAccountKey' is not valid JSON", e);
      }
      return new ServiceAccountTokenSource(keyJson, scope, subject);
    }
    if (config.containsKey("serviceAccountKeyPath")) {
      String path = (String) config.get("serviceAccountKeyPath");
      try {
        // A credential file on the host running the adapter, not data storage.
        return new ServiceAccountTokenSource(MAPPER.readTree(Files.readAllBytes(Paths.get(path))),
            scope, subject);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Cannot read Google Drive service account key file '" + path + "'", e);
      }
    }
    if (config.containsKey("refreshToken")) {
      String clientId = (String) config.get("clientId");
      String clientSecret = (String) config.get("clientSecret");
      if (clientId == null || clientSecret == null) {
        throw new IllegalArgumentException(
            "Google Drive refresh-token sign-in requires 'clientId' and 'clientSecret'");
      }
      return new RefreshTokenSource(clientId, clientSecret, (String) config.get("refreshToken"),
          GOOGLE_TOKEN_URL);
    }
    if (config.containsKey("accessToken")) {
      String token = (String) config.get("accessToken");
      return () -> token;
    }
    throw new IllegalArgumentException("Google Drive storage requires one of: "
        + "'serviceAccountKey', 'serviceAccountKeyPath', 'clientId+clientSecret+refreshToken', "
        + "or 'accessToken'");
  }

  private static @Nullable String subjectFrom(Map<String, Object> config) {
    if (!config.containsKey("subject")) {
      return null;
    }
    Object subject = config.get("subject");
    if (!(subject instanceof String) || ((String) subject).trim().isEmpty()) {
      throw new IllegalArgumentException("Google Drive 'subject' must be a non-empty string");
    }
    return ((String) subject).trim();
  }

  private static String scopeFrom(Map<String, Object> config) {
    if (!config.containsKey("scope")) {
      return DRIVE_READONLY_SCOPE;
    }
    Object scope = config.get("scope");
    List<String> scopes = new ArrayList<>();
    if (scope instanceof String) {
      for (String s : ((String) scope).trim().split("\\s+")) {
        if (!s.isEmpty()) {
          scopes.add(s);
        }
      }
    } else if (scope instanceof List) {
      for (Object s : (List<?>) scope) {
        if (!(s instanceof String) || ((String) s).trim().isEmpty()) {
          throw new IllegalArgumentException(
              "Google Drive 'scope' list entries must be non-empty strings");
        }
        scopes.add(((String) s).trim());
      }
    } else {
      throw new IllegalArgumentException(
          "Google Drive 'scope' must be a space-separated string or a list of strings");
    }
    if (scopes.isEmpty()) {
      throw new IllegalArgumentException("Google Drive 'scope' is empty");
    }
    return String.join(" ", scopes);
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    String dir = normalize(path);
    DriveItem folder = resolve(dir);
    if (folder == null) {
      throw new IOException("Google Drive path not found: '" + path + "'");
    }
    if (!folder.isFolder()) {
      throw new IOException("Google Drive path is not a folder: '" + path + "'");
    }
    List<FileEntry> entries = new ArrayList<>();
    for (DriveItem child : children(folder.id, null)) {
      String childPath = dir.isEmpty() ? child.name : dir + "/" + child.name;
      entries.add(
          new FileEntry(childPath, child.name, child.isFolder(), child.size,
          child.modifiedTime));
      if (child.isFolder()) {
        folderIds.put(childPath, child.id);
        if (recursive) {
          entries.addAll(listFiles(childPath, true));
        }
      }
    }
    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    DriveItem item = require(path);
    return new FileMetadata(path, item.size, item.modifiedTime, item.mimeType, item.version);
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    DriveItem item = require(path);
    if (item.isFolder()) {
      throw new IOException("Google Drive path is a folder, not a file: '" + path + "'");
    }
    if (item.mimeType.startsWith(NATIVE_MIME_PREFIX)) {
      throw new IOException("Google Drive file '" + path + "' is a Google Workspace document ("
          + item.mimeType + ") with no binary content; save it as CSV, XLSX or another "
          + "file format to query it");
    }
    HttpURLConnection conn =
        open(apiBase + "/files/" + enc(item.id) + "?alt=media&supportsAllDrives=true");
    int code = conn.getResponseCode();
    if (code != HttpURLConnection.HTTP_OK) {
      String error = readError(conn);
      conn.disconnect();
      throw new IOException("Google Drive download of '" + path + "' failed: HTTP " + code
          + (error.isEmpty() ? "" : " - " + error));
    }
    InputStream staged =
        StorageProvider.stageToSafeStream(conn.getInputStream(), conn.getContentLengthLong());
    conn.disconnect();
    return staged;
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    return resolve(normalize(path)) != null;
  }

  @Override public boolean isDirectory(String path) throws IOException {
    DriveItem item = resolve(normalize(path));
    return item != null && item.isFolder();
  }

  @Override public String getStorageType() {
    return "gdrive";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath.startsWith("/")) {
      return relativePath;
    }
    String base = normalize(basePath);
    int lastSlash = base.lastIndexOf('/');
    String lastName = lastSlash >= 0 ? base.substring(lastSlash + 1) : base;
    if (!basePath.endsWith("/") && lastName.contains(".")) {
      base = lastSlash >= 0 ? base.substring(0, lastSlash) : "";
    }
    return base.isEmpty() ? relativePath : base + "/" + relativePath;
  }

  private DriveItem require(String path) throws IOException {
    DriveItem item = resolve(normalize(path));
    if (item == null) {
      throw new IOException("Google Drive path not found: '" + path + "'");
    }
    return item;
  }

  /** Resolves a normalized path to its item, or null when a segment does not exist. */
  @Nullable DriveItem resolve(String path) throws IOException {
    if (path.isEmpty()) {
      return getItem(rootFolderId);
    }
    int lastSlash = path.lastIndexOf('/');
    String parentPath = lastSlash >= 0 ? path.substring(0, lastSlash) : "";
    String name = path.substring(lastSlash + 1);
    String parentId = folderIds.get(parentPath);
    if (parentId == null) {
      DriveItem parent = resolve(parentPath);
      if (parent == null || !parent.isFolder()) {
        return null;
      }
      parentId = parent.id;
      folderIds.put(parentPath, parentId);
    }
    List<DriveItem> matches = children(parentId, name);
    if (matches.isEmpty()) {
      return null;
    }
    if (matches.size() > 1) {
      List<String> ids = new ArrayList<>();
      for (DriveItem m : matches) {
        ids.add(m.id);
      }
      throw new IOException("Google Drive path '" + path + "' is ambiguous: " + matches.size()
          + " items share that name in one folder (ids " + ids + ")");
    }
    return matches.get(0);
  }

  private DriveItem getItem(String id) throws IOException {
    return new DriveItem(
        getJson(apiBase + "/files/" + enc(id) + "?supportsAllDrives=true&fields="
        + enc(ITEM_FIELDS)));
  }

  /** Lists a folder's children, optionally only those with an exact name. */
  private List<DriveItem> children(String folderId, @Nullable String name) throws IOException {
    String q = "'" + escapeQuery(folderId) + "' in parents and trashed = false";
    if (name != null) {
      q += " and name = '" + escapeQuery(name) + "'";
    }
    List<DriveItem> items = new ArrayList<>();
    String pageToken = null;
    do {
      String url = apiBase + "/files?q=" + enc(q)
          + "&fields=" + enc("nextPageToken,files(" + ITEM_FIELDS + ")")
          + "&pageSize=1000&supportsAllDrives=true&includeItemsFromAllDrives=true"
          + (pageToken == null ? "" : "&pageToken=" + enc(pageToken));
      JsonNode page = getJson(url);
      for (JsonNode f : page.get("files")) {
        items.add(new DriveItem(f));
      }
      pageToken = page.has("nextPageToken") ? page.get("nextPageToken").asText() : null;
    } while (pageToken != null);
    return items;
  }

  private JsonNode getJson(String url) throws IOException {
    HttpURLConnection conn = open(url);
    try {
      int code = conn.getResponseCode();
      if (code != HttpURLConnection.HTTP_OK) {
        String error = readError(conn);
        throw new IOException("Google Drive API error: HTTP " + code
            + (error.isEmpty() ? "" : " - " + error));
      }
      try (InputStream in = conn.getInputStream()) {
        return MAPPER.readTree(in);
      }
    } finally {
      conn.disconnect();
    }
  }

  private HttpURLConnection open(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer " + tokens.getAccessToken());
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    return conn;
  }

  /** Reads an error body's message for the exception text; the HTTP code is reported anyway. */
  private static String readError(HttpURLConnection conn) throws IOException {
    try (InputStream err = conn.getErrorStream()) {
      if (err == null) {
        return "";
      }
      byte[] body = readAll(err);
      String text = new String(body, StandardCharsets.UTF_8);
      try {
        JsonNode json = MAPPER.readTree(body);
        if (json.has("error") && json.get("error").has("message")) {
          return json.get("error").get("message").asText();
        }
      } catch (IOException notJson) {
        return text;
      }
      return text;
    }
  }

  static String normalize(String path) {
    String p = path == null ? "" : path.trim();
    while (p.startsWith("/")) {
      p = p.substring(1);
    }
    while (p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  static String escapeQuery(String value) {
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static long parseTime(String time) {
    return Instant.parse(time).toEpochMilli();
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /** Base for token sources that cache a token until shortly before it expires. */
  private abstract static class CachingTokenSource implements TokenSource {
    private @Nullable String token;
    private Instant expiry = Instant.EPOCH;

    @Override public synchronized String getAccessToken() throws IOException {
      if (token == null || Instant.now().isAfter(expiry.minusSeconds(60))) {
        JsonNode response = requestToken();
        if (!response.has("access_token") || !response.has("expires_in")) {
          throw new IOException("Google token response has no access_token/expires_in: "
              + response);
        }
        token = response.get("access_token").asText();
        expiry = Instant.now().plusSeconds(response.get("expires_in").asLong());
      }
      return token;
    }

    abstract JsonNode requestToken() throws IOException;

    static JsonNode postForm(String tokenUrl, String form) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) URI.create(tokenUrl).toURL().openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(30000);
      try {
        try (OutputStream out = conn.getOutputStream()) {
          out.write(form.getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        if (code != HttpURLConnection.HTTP_OK) {
          throw new IOException("Google token request to " + tokenUrl + " failed: HTTP " + code
              + " - " + readError(conn));
        }
        try (InputStream in = conn.getInputStream()) {
          return MAPPER.readTree(in);
        }
      } finally {
        conn.disconnect();
      }
    }
  }

  /** Service account sign-in: a self-signed RS256 JWT exchanged at the key's token URI. */
  static final class ServiceAccountTokenSource extends CachingTokenSource {
    private final String clientEmail;
    private final String tokenUri;
    private final PrivateKey privateKey;
    private final String scope;
    private final @Nullable String subject;

    ServiceAccountTokenSource(JsonNode key, String scope, @Nullable String subject) {
      this.scope = scope;
      this.subject = subject;
      for (String field : new String[] {"client_email", "private_key", "token_uri"}) {
        if (!key.hasNonNull(field)) {
          throw new IllegalArgumentException(
              "Google service account key has no '" + field + "' field");
        }
      }
      this.clientEmail = key.get("client_email").asText();
      this.tokenUri = key.get("token_uri").asText();
      this.privateKey = parsePkcs8(key.get("private_key").asText());
    }

    @Override JsonNode requestToken() throws IOException {
      long now = Instant.now().getEpochSecond();
      Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
      String header = b64.encodeToString(
          "{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
      Map<String, Object> claims = new LinkedHashMap<>();
      claims.put("iss", clientEmail);
      claims.put("scope", scope);
      if (subject != null) {
        claims.put("sub", subject);
      }
      claims.put("aud", tokenUri);
      claims.put("iat", now);
      claims.put("exp", now + 3600);
      String payload = b64.encodeToString(MAPPER.writeValueAsBytes(claims));
      String signingInput = header + "." + payload;
      String signature;
      try {
        Signature signer = Signature.getInstance("SHA256withRSA");
        signer.initSign(privateKey);
        signer.update(signingInput.getBytes(StandardCharsets.UTF_8));
        signature = b64.encodeToString(signer.sign());
      } catch (GeneralSecurityException e) {
        throw new IOException("Cannot sign Google service account assertion", e);
      }
      String form = "grant_type=" + enc("urn:ietf:params:oauth:grant-type:jwt-bearer")
          + "&assertion=" + enc(signingInput + "." + signature);
      return postForm(tokenUri, form);
    }

    private static PrivateKey parsePkcs8(String pem) {
      String body = pem.replace("-----BEGIN PRIVATE KEY-----", "")
          .replace("-----END PRIVATE KEY-----", "")
          .replaceAll("\\s", "");
      try {
        return KeyFactory.getInstance("RSA")
            .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(body)));
      } catch (GeneralSecurityException | IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Google service account 'private_key' is not a PKCS#8 RSA key", e);
      }
    }
  }

  /** OAuth user sign-in: a refresh token exchanged for access tokens. */
  static final class RefreshTokenSource extends CachingTokenSource {
    private final String clientId;
    private final String clientSecret;
    private final String refreshToken;
    private final String tokenUrl;

    RefreshTokenSource(String clientId, String clientSecret, String refreshToken,
        String tokenUrl) {
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.refreshToken = refreshToken;
      this.tokenUrl = tokenUrl;
    }

    @Override JsonNode requestToken() throws IOException {
      String form = "grant_type=refresh_token&client_id=" + enc(clientId)
          + "&client_secret=" + enc(clientSecret) + "&refresh_token=" + enc(refreshToken);
      return postForm(tokenUrl, form);
    }
  }
}
