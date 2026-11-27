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

import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the OneDrive ({@code onedrive}) and Google Drive ({@code gdrive}) storage providers
 * against an in-process fake of the Microsoft Graph and Drive v3 HTTP APIs.
 */
@Tag("unit")
@Isolated("swaps the JVM-wide StorageCacheManager")
public class CloudDriveStorageProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern PARENT_Q = Pattern.compile("'([^']+)' in parents");
  private static final Pattern NAME_Q = Pattern.compile("name = '((?:[^'\\\\]|\\\\.)*)'");

  private HttpServer server;
  private String base;
  private final List<String> authHeaders = new CopyOnWriteArrayList<>();

  /** Fake Drive tree: id to item. */
  private final Map<String, ObjectNode> driveItems = new LinkedHashMap<>();
  private final Map<String, String> driveParents = new HashMap<>();
  private final Map<String, byte[]> driveContent = new HashMap<>();
  private int drivePageSize = 1000;

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  // ---------------------------------------------------------------- Google Drive

  private void driveItem(String id, String parent, String name, String mime, String content) {
    ObjectNode n = MAPPER.createObjectNode();
    n.put("id", id);
    n.put("name", name);
    n.put("mimeType", mime);
    n.put("modifiedTime", "2026-11-20T10:15:30.000Z");
    n.put("version", "7");
    if (content != null) {
      n.put("size", String.valueOf(content.length()));
      driveContent.put(id, content.getBytes(StandardCharsets.UTF_8));
    }
    driveItems.put(id, n);
    if (parent != null) {
      driveParents.put(id, parent);
    }
  }

  private void standardDriveTree() {
    driveItem("root", null, "Shared", "application/vnd.google-apps.folder", null);
    driveItem("f-sales", "root", "sales", "application/vnd.google-apps.folder", null);
    driveItem("f-2026", "f-sales", "2026", "application/vnd.google-apps.folder", null);
    driveItem("c-q1", "f-2026", "q1.csv", "text/csv", "id,amount\n1,10\n");
    driveItem("c-top", "root", "it's top.csv", "text/csv", "a\n1\n");
    driveItem("g-doc", "root", "notes", "application/vnd.google-apps.document", null);
    serveDrive();
  }

  private void serveDrive() {
    server.createContext("/drive/v3/files", this::handleDrive);
  }

  private void handleDrive(HttpExchange ex) throws IOException {
    authHeaders.add(ex.getRequestHeaders().getFirst("Authorization"));
    String path = ex.getRequestURI().getPath();
    Map<String, String> params = query(ex.getRequestURI().getRawQuery());
    String prefix = "/drive/v3/files";
    if (path.equals(prefix)) {
      String q = params.get("q");
      Matcher pm = PARENT_Q.matcher(q);
      assertTrue(pm.find(), q);
      String parent = pm.group(1);
      Matcher nm = NAME_Q.matcher(q);
      String name = nm.find()
          ? nm.group(1).replace("\\'", "'").replace("\\\\", "\\") : null;
      List<ObjectNode> kids = new ArrayList<>();
      for (Map.Entry<String, ObjectNode> e : driveItems.entrySet()) {
        if (parent.equals(driveParents.get(e.getKey()))
            && (name == null || name.equals(e.getValue().get("name").asText()))) {
          kids.add(e.getValue());
        }
      }
      int start = params.containsKey("pageToken") ? Integer.parseInt(params.get("pageToken")) : 0;
      int end = Math.min(kids.size(), start + drivePageSize);
      ObjectNode resp = MAPPER.createObjectNode();
      ArrayNode files = resp.putArray("files");
      for (ObjectNode k : kids.subList(start, end)) {
        files.add(k);
      }
      if (end < kids.size()) {
        resp.put("nextPageToken", String.valueOf(end));
      }
      send(ex, 200, MAPPER.writeValueAsBytes(resp));
      return;
    }
    String id = path.substring(prefix.length() + 1);
    ObjectNode item = driveItems.get(id);
    if (item == null) {
      send(ex, 404, "{\"error\":{\"message\":\"File not found\"}}".getBytes(StandardCharsets.UTF_8));
    } else if ("media".equals(params.get("alt"))) {
      send(ex, 200, driveContent.get(id));
    } else {
      send(ex, 200, MAPPER.writeValueAsBytes(item));
    }
  }

  private GoogleDriveStorageProvider drive() {
    return new GoogleDriveStorageProvider("root", () -> "test-token", base + "/drive/v3");
  }

  @Test void driveListsRootAndRecursiveByName() throws IOException {
    standardDriveTree();
    List<String> top = new ArrayList<>();
    for (StorageProvider.FileEntry e : drive().listFiles("", false)) {
      top.add(e.getPath() + (e.isDirectory() ? "/" : ""));
    }
    assertEquals(List.of("sales/", "it's top.csv", "notes"), top);

    List<String> all = new ArrayList<>();
    for (StorageProvider.FileEntry e : drive().listFiles("/sales", true)) {
      all.add(e.getPath());
    }
    assertEquals(List.of("sales/2026", "sales/2026/q1.csv"), all);
    assertTrue(authHeaders.stream().allMatch("Bearer test-token"::equals));
  }

  @Test void drivePagesThroughLargeFolders() throws IOException {
    standardDriveTree();
    for (int i = 0; i < 5; i++) {
      driveItem("p" + i, "f-2026", "part-" + i + ".csv", "text/csv", "x\n" + i + "\n");
    }
    drivePageSize = 2;
    assertEquals(6, drive().listFiles("sales/2026", false).size());
  }

  @Test void driveReadsFileAndMetadata() throws IOException {
    standardDriveTree();
    GoogleDriveStorageProvider p = drive();
    try (InputStream in = p.openInputStream("sales/2026/q1.csv")) {
      assertEquals("id,amount\n1,10\n", new String(readAll(in), StandardCharsets.UTF_8));
    }
    StorageProvider.FileMetadata md = p.getMetadata("sales/2026/q1.csv");
    assertEquals(15, md.getSize());
    assertEquals("text/csv", md.getContentType());
    assertEquals("7", md.getEtag());
    assertEquals(java.time.Instant.parse("2026-11-20T10:15:30Z").toEpochMilli(),
        md.getLastModified());
    // A name with a quote must be escaped in the Drive query.
    try (InputStream in = p.openInputStream("it's top.csv")) {
      assertEquals("a\n1\n", new String(readAll(in), StandardCharsets.UTF_8));
    }
  }

  @Test void driveExistsAndIsDirectory() throws IOException {
    standardDriveTree();
    GoogleDriveStorageProvider p = drive();
    assertTrue(p.exists("sales/2026/q1.csv"));
    assertFalse(p.exists("sales/2025/q1.csv"));
    assertFalse(p.exists("sales/2026/missing.csv"));
    assertTrue(p.isDirectory("sales"));
    assertTrue(p.isDirectory(""));
    assertFalse(p.isDirectory("sales/2026/q1.csv"));
    assertThrows(IOException.class, () -> p.openInputStream("nope.csv"));
  }

  @Test void driveRefusesAmbiguousNamesAndNativeDocs() {
    standardDriveTree();
    driveItem("c-q1-dup", "f-2026", "q1.csv", "text/csv", "dup\n");
    GoogleDriveStorageProvider p = drive();
    IOException ambiguous =
        assertThrows(IOException.class, () -> p.openInputStream("sales/2026/q1.csv"));
    assertTrue(ambiguous.getMessage().contains("ambiguous"), ambiguous.getMessage());
    IOException nativeDoc = assertThrows(IOException.class, () -> p.openInputStream("notes"));
    assertTrue(nativeDoc.getMessage().contains("Google Workspace"), nativeDoc.getMessage());
  }

  @Test void driveResolvePath() {
    GoogleDriveStorageProvider p = drive();
    assertEquals("sales/2026/q2.csv", p.resolvePath("sales/2026/q1.csv", "q2.csv"));
    assertEquals("sales/2026/q2.csv", p.resolvePath("sales/2026", "q2.csv"));
    assertEquals("q2.csv", p.resolvePath("", "q2.csv"));
  }

  /** Runs two listings through a service-account provider; returns the JWT claims sent. */
  private List<JsonNode> serviceAccountClaims(Map<String, Object> extraConfig) throws Exception {
    standardDriveTree();
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    KeyPair keys = gen.generateKeyPair();
    String pem = "-----BEGIN PRIVATE KEY-----\n"
        + Base64.getMimeEncoder().encodeToString(keys.getPrivate().getEncoded())
        + "\n-----END PRIVATE KEY-----\n";
    List<JsonNode> claimsSeen = new CopyOnWriteArrayList<>();
    server.createContext("/token", ex -> {
      Map<String, String> form =
          query(new String(readAll(ex.getRequestBody()), StandardCharsets.UTF_8));
      assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", form.get("grant_type"));
      String[] jwt = form.get("assertion").split("\\.");
      try {
        Signature verifier = Signature.getInstance("SHA256withRSA");
        verifier.initVerify(keys.getPublic());
        verifier.update((jwt[0] + "." + jwt[1]).getBytes(StandardCharsets.UTF_8));
        assertTrue(verifier.verify(Base64.getUrlDecoder().decode(jwt[2])));
      } catch (java.security.GeneralSecurityException e) {
        throw new IOException(e);
      }
      claimsSeen.add(MAPPER.readTree(Base64.getUrlDecoder().decode(jwt[1])));
      send(ex, 200, "{\"access_token\":\"sa-token\",\"expires_in\":3600}"
          .getBytes(StandardCharsets.UTF_8));
    });
    ObjectNode key = MAPPER.createObjectNode();
    key.put("type", "service_account");
    key.put("client_email", "reader@proj.iam.gserviceaccount.com");
    key.put("private_key", pem);
    key.put("token_uri", base + "/token");
    Map<String, Object> config = new HashMap<>(extraConfig);
    config.put("serviceAccountKey", MAPPER.writeValueAsString(key));
    GoogleDriveStorageProvider p = new GoogleDriveStorageProvider("root",
        GoogleDriveStorageProvider.tokenSourceFrom(config), base + "/drive/v3");

    assertEquals(3, p.listFiles("", false).size());
    p.listFiles("sales", false);
    assertEquals(1, claimsSeen.size(), "token is cached across calls");
    assertTrue(authHeaders.stream().allMatch("Bearer sa-token"::equals));
    return claimsSeen;
  }

  @Test void driveServiceAccountSignsJwtAndUsesToken() throws Exception {
    JsonNode claims = serviceAccountClaims(new HashMap<>()).get(0);
    assertEquals("reader@proj.iam.gserviceaccount.com", claims.get("iss").asText());
    assertEquals(GoogleDriveStorageProvider.DRIVE_READONLY_SCOPE, claims.get("scope").asText());
    assertEquals(base + "/token", claims.get("aud").asText());
    assertFalse(claims.has("sub"), "no subject configured, no delegation");
  }

  @Test void driveServiceAccountDelegatesToSubjectWithScope() throws Exception {
    Map<String, Object> extra = new HashMap<>();
    extra.put("subject", "analyst@example.com");
    extra.put("scope", "https://www.googleapis.com/auth/drive");
    JsonNode claims = serviceAccountClaims(extra).get(0);
    assertEquals("reader@proj.iam.gserviceaccount.com", claims.get("iss").asText());
    assertEquals("analyst@example.com", claims.get("sub").asText());
    assertEquals("https://www.googleapis.com/auth/drive", claims.get("scope").asText());
  }

  @Test void driveServiceAccountScopeList() throws Exception {
    Map<String, Object> extra = new HashMap<>();
    extra.put("subject", "analyst@example.com");
    extra.put("scope", Arrays.asList(GoogleDriveStorageProvider.DRIVE_READONLY_SCOPE,
        "https://www.googleapis.com/auth/drive.metadata.readonly"));
    JsonNode claims = serviceAccountClaims(extra).get(0);
    assertEquals(GoogleDriveStorageProvider.DRIVE_READONLY_SCOPE
        + " https://www.googleapis.com/auth/drive.metadata.readonly",
        claims.get("scope").asText());
  }

  @Test void driveRefreshTokenSignIn() throws IOException {
    standardDriveTree();
    server.createContext("/oauth", ex -> {
      Map<String, String> form =
          query(new String(readAll(ex.getRequestBody()), StandardCharsets.UTF_8));
      assertEquals("refresh_token", form.get("grant_type"));
      assertEquals("cid", form.get("client_id"));
      assertEquals("csecret", form.get("client_secret"));
      assertEquals("rtoken", form.get("refresh_token"));
      send(ex, 200, "{\"access_token\":\"user-token\",\"expires_in\":3600}"
          .getBytes(StandardCharsets.UTF_8));
    });
    GoogleDriveStorageProvider p = new GoogleDriveStorageProvider("root",
        new GoogleDriveStorageProvider.RefreshTokenSource("cid", "csecret", "rtoken",
            base + "/oauth"), base + "/drive/v3");
    assertTrue(p.exists("sales"));
    assertTrue(authHeaders.stream().allMatch("Bearer user-token"::equals));
  }

  @Test void driveConfigValidation() {
    Map<String, Object> noFolder = new HashMap<>();
    noFolder.put("accessToken", "t");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", noFolder));

    Map<String, Object> noAuth = new HashMap<>();
    noAuth.put("folderId", "abc");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", noAuth));

    Map<String, Object> refreshNoSecret = new HashMap<>(noAuth);
    refreshNoSecret.put("refreshToken", "r");
    refreshNoSecret.put("clientId", "c");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", refreshNoSecret));

    Map<String, Object> badKey = new HashMap<>(noAuth);
    badKey.put("serviceAccountKey", "{\"client_email\":\"x\"}");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", badKey));

    Map<String, Object> subjectWithoutServiceAccount = new HashMap<>(noAuth);
    subjectWithoutServiceAccount.put("accessToken", "t");
    subjectWithoutServiceAccount.put("subject", "analyst@example.com");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", subjectWithoutServiceAccount));

    Map<String, Object> scopeWithRefreshToken = new HashMap<>(refreshNoSecret);
    scopeWithRefreshToken.put("clientSecret", "s");
    scopeWithRefreshToken.put("scope", "https://www.googleapis.com/auth/drive");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", scopeWithRefreshToken));

    Map<String, Object> emptySubject = new HashMap<>(noAuth);
    emptySubject.put("serviceAccountKeyPath", "/nonexistent/key.json");
    emptySubject.put("subject", " ");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", emptySubject));
    assertTrue(e.getMessage().contains("subject"), e.getMessage());

    Map<String, Object> emptyScope = new HashMap<>(noAuth);
    emptyScope.put("serviceAccountKeyPath", "/nonexistent/key.json");
    emptyScope.put("scope", "  ");
    e = assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gdrive", emptyScope));
    assertTrue(e.getMessage().contains("scope"), e.getMessage());

    Map<String, Object> ok = new HashMap<>(noAuth);
    ok.put("accessToken", "t");
    StorageProvider p = StorageProviderFactory.createFromType("gdrive", ok);
    assertInstanceOf(GoogleDriveStorageProvider.class, p);
    assertEquals("gdrive", p.getStorageType());
  }

  // ---------------------------------------------------------------- OneDrive

  @Test void oneDriveResolvesUserDriveListsAndReads() throws IOException {
    String upn = "pat@contoso.com";
    server.createContext("/v1.0", ex -> {
      authHeaders.add(ex.getRequestHeaders().getFirst("Authorization"));
      // Decoded: the provider percent-encodes '/' inside a root:/...: item path.
      String raw = ex.getRequestURI().getPath();
      String body;
      if (raw.equals("/v1.0/users/pat@contoso.com/drive")) {
        body = "{\"id\":\"drv1\"}";
      } else if (raw.equals("/v1.0/drives/drv1/root/children")) {
        body = "{\"value\":[{\"name\":\"data\",\"folder\":{},"
            + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}]}";
      } else if (raw.equals("/v1.0/drives/drv1/root:/data:/children")) {
        body = "{\"value\":[{\"name\":\"a.csv\",\"size\":4,\"file\":{},"
            + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}]}";
      } else if (raw.equals("/v1.0/drives/drv1/root:/data/a.csv")) {
        // Metadata: read when another test has initialized the shared StorageCacheManager.
        body = "{\"name\":\"a.csv\",\"size\":4,\"file\":{},"
            + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}";
      } else if (raw.equals("/v1.0/drives/drv1/root:/data/a.csv:/content")) {
        send(ex, 200, "x\n1\n".getBytes(StandardCharsets.UTF_8));
        return;
      } else {
        send(ex, 404, "{\"error\":{\"message\":\"not found\"}}".getBytes(StandardCharsets.UTF_8));
        return;
      }
      send(ex, 200, body.getBytes(StandardCharsets.UTF_8));
    });
    MicrosoftGraphTokenManager tokens =
        new MicrosoftGraphTokenManager("graph-token", "https://graph.microsoft.com");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokens, upn,
        base + "/v1.0");

    assertEquals("onedrive", p.getStorageType());
    List<StorageProvider.FileEntry> entries = p.listFiles("", true);
    assertEquals(2, entries.size());
    assertEquals("data", entries.get(0).getPath());
    assertTrue(entries.get(0).isDirectory());
    assertEquals("data/a.csv", entries.get(1).getPath());
    try (InputStream in = p.openInputStream("data/a.csv")) {
      assertEquals("x\n1\n", new String(readAll(in), StandardCharsets.UTF_8));
    }
    assertTrue(authHeaders.stream().allMatch("Bearer graph-token"::equals));
  }

  @Test void oneDriveCachesDownloadsPerDrive(@TempDir File cacheDir) throws Exception {
    // Two drives holding the same path, with item metadata that cannot tell them apart.
    List<String> downloads = new CopyOnWriteArrayList<>();
    server.createContext("/v1.0", ex -> {
      String raw = ex.getRequestURI().getPath();
      Matcher user = Pattern.compile("/v1.0/users/(\\w+)/drive").matcher(raw);
      Matcher item = Pattern.compile("/v1.0/drives/(\\w+)/root:/data/a.csv(:/content)?")
          .matcher(raw);
      if (user.matches()) {
        send(ex, 200, ("{\"id\":\"drv" + user.group(1) + "\"}")
            .getBytes(StandardCharsets.UTF_8));
      } else if (item.matches() && item.group(2) != null) {
        downloads.add(item.group(1));
        send(ex, 200, item.group(1).getBytes(StandardCharsets.UTF_8));
      } else if (item.matches()) {
        send(ex, 200, "{\"size\":4,\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}"
            .getBytes(StandardCharsets.UTF_8));
      } else {
        send(ex, 404, "{\"error\":{\"message\":\"not found\"}}".getBytes(StandardCharsets.UTF_8));
      }
    });
    Field instance = StorageCacheManager.class.getDeclaredField("instance");
    instance.setAccessible(true);
    Object previous = instance.get(null);
    instance.set(null, null);
    try {
      StorageCacheManager.initialize(cacheDir);
      MicrosoftGraphTokenManager tokens =
          new MicrosoftGraphTokenManager("graph-token", "https://graph.microsoft.com");
      MicrosoftGraphStorageProvider a =
          new MicrosoftGraphStorageProvider(tokens, "A", base + "/v1.0");
      MicrosoftGraphStorageProvider b =
          new MicrosoftGraphStorageProvider(tokens, "B", base + "/v1.0");

      // A leading slash names the same file, so the second read of each drive is a cache hit.
      for (String path : List.of("/data/a.csv", "data/a.csv")) {
        try (InputStream in = a.openInputStream(path)) {
          assertEquals("drvA", new String(readAll(in), StandardCharsets.UTF_8));
        }
        try (InputStream in = b.openInputStream(path)) {
          assertEquals("drvB", new String(readAll(in), StandardCharsets.UTF_8));
        }
      }
      assertEquals(List.of("drvA", "drvB"), downloads);
    } finally {
      StorageCacheManager.getInstance().shutdown();
      instance.set(null, previous);
    }
  }

  @Test void oneDriveConfigValidation() {
    Map<String, Object> noUpn = new HashMap<>();
    noUpn.put("tenantId", "t");
    noUpn.put("clientId", "c");
    noUpn.put("clientSecret", "s");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("onedrive", noUpn));

    Map<String, Object> noSecret = new HashMap<>();
    noSecret.put("userPrincipalName", "pat@contoso.com");
    noSecret.put("tenantId", "t");
    noSecret.put("clientId", "c");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("onedrive", noSecret));

    Map<String, Object> ok = new HashMap<>(noUpn);
    ok.put("userPrincipalName", "pat@contoso.com");
    StorageProvider p = StorageProviderFactory.createFromType("onedrive", ok);
    assertInstanceOf(MicrosoftGraphStorageProvider.class, p);
    assertEquals("onedrive", p.getStorageType());
  }

  // ---------------------------------------------------------------- helpers

  private static Map<String, String> query(String raw) {
    Map<String, String> out = new HashMap<>();
    if (raw == null || raw.isEmpty()) {
      return out;
    }
    for (String pair : raw.split("&")) {
      int eq = pair.indexOf('=');
      out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
          URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
    }
    return out;
  }

  private static void send(HttpExchange ex, int code, byte[] body) throws IOException {
    ex.sendResponseHeaders(code, body.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(body);
    }
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
}
