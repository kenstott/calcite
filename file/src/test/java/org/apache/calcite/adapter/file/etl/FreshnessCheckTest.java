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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link FreshnessConfig} parsing and {@link FreshnessCheck} token logic. */
@Tag("unit")
public class FreshnessCheckTest {

  private static Map<String, Object> map(Object... kv) {
    Map<String, Object> m = new HashMap<String, Object>();
    for (int i = 0; i + 1 < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  private static Map<String, String> headers(String... kv) {
    Map<String, String> m = new HashMap<String, String>();
    for (int i = 0; i + 1 < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test void parsesTypeAndCountProbe() {
    FreshnessConfig c = FreshnessConfig.fromMap(
        map("type", "count", "count_probe", map("url", "u", "path", "$.totalResults")));
    assertEquals(FreshnessConfig.Type.COUNT, c.getType());
    assertEquals("u", c.getCountUrl());
    assertEquals("$.totalResults", c.getCountPath());
  }

  @Test void parsesOtherTypes() {
    assertEquals(FreshnessConfig.Type.ETAG, FreshnessConfig.fromMap(map("type", "etag")).getType());
    assertEquals(FreshnessConfig.Type.LAST_MODIFIED,
        FreshnessConfig.fromMap(map("type", "last_modified")).getType());
    assertEquals(FreshnessConfig.Type.VERSION,
        FreshnessConfig.fromMap(map("type", "version", "version_field", "catalogVersion"))
            .getVersionField().equals("catalogVersion")
            ? FreshnessConfig.Type.VERSION : null);
    assertNull(FreshnessConfig.fromMap(map("type", "bogus")));   // unknown → off
    assertNull(FreshnessConfig.fromMap(null));
  }

  @Test void etagAndLastModifiedAndSizeFromHeaders() {
    FreshnessConfig etag = FreshnessConfig.fromMap(map("type", "etag"));
    assertEquals("\"abc\"", FreshnessCheck.token(etag, headers("etag", "\"abc\""), null, null));
    FreshnessConfig lm = FreshnessConfig.fromMap(map("type", "last_modified"));
    assertEquals("Mon, 01 Jan 2026 00:00:00 GMT",
        FreshnessCheck.token(lm, headers("Last-Modified", "Mon, 01 Jan 2026 00:00:00 GMT"), null, null));
    FreshnessConfig size = FreshnessConfig.fromMap(map("type", "size"));
    assertEquals("196608000",
        FreshnessCheck.token(size, headers("content-length", "196608000"), null, null));
  }

  @Test void versionAndCountFromJsonBody() {
    FreshnessConfig version = FreshnessConfig.fromMap(map("type", "version", "version_field", "$.catalogVersion"));
    assertEquals("2024.01.30",
        FreshnessCheck.token(version, null, "{\"catalogVersion\":\"2024.01.30\"}", null));
    FreshnessConfig count = FreshnessConfig.fromMap(
        map("type", "count", "count_probe", map("url", "u", "path", "$.totalResults")));
    assertEquals("355000",
        FreshnessCheck.token(count, null, "{\"totalResults\":355000}", null));
  }

  @Test void parsesGraphqlQueryAndPath() {
    FreshnessConfig c = FreshnessConfig.fromMap(map(
        "type", "graphql",
        "query", "{ securityAdvisories(first:1){ nodes{ updatedAt } } }",
        "path", "data.securityAdvisories.nodes[0].updatedAt"));
    assertEquals(FreshnessConfig.Type.GRAPHQL, c.getType());
    assertEquals("{ securityAdvisories(first:1){ nodes{ updatedAt } } }", c.getQuery());
    assertEquals("data.securityAdvisories.nodes[0].updatedAt", c.getQueryPath());
    // "gql" alias resolves to the same type
    assertEquals(FreshnessConfig.Type.GRAPHQL, FreshnessConfig.fromMap(map("type", "gql")).getType());
  }

  @Test void graphqlExtractsValueViaArrayPath() {
    FreshnessConfig c = FreshnessConfig.fromMap(map(
        "type", "graphql",
        "query", "q",
        "path", "data.securityAdvisories.nodes[0].updatedAt"));
    String body = "{\"data\":{\"securityAdvisories\":{\"nodes\":"
        + "[{\"updatedAt\":\"2026-06-05T12:00:00Z\"}]}}}";
    assertEquals("2026-06-05T12:00:00Z", FreshnessCheck.token(c, null, body, null));
  }

  @Test void graphqlExtractsScalarTotalCount() {
    FreshnessConfig c = FreshnessConfig.fromMap(map(
        "type", "graphql", "query", "q", "path", "data.securityAdvisories.totalCount"));
    assertEquals("31362",
        FreshnessCheck.token(c, null, "{\"data\":{\"securityAdvisories\":{\"totalCount\":31362}}}", null));
  }

  @Test void graphqlMissingNodeYieldsNull() {
    FreshnessConfig c = FreshnessConfig.fromMap(map(
        "type", "graphql", "query", "q", "path", "data.securityAdvisories.nodes[5].updatedAt"));
    // index out of range → null token → treated as "changed" (won't wrongly skip)
    assertNull(FreshnessCheck.token(c, null,
        "{\"data\":{\"securityAdvisories\":{\"nodes\":[]}}}", null));
  }

  @Test void checksumSidecarFirstTokenAndObjectMetadata() {
    FreshnessConfig sidecar = FreshnessConfig.fromMap(map("type", "checksum", "checksum_url", "u"));
    assertEquals("d41d8cd98f00b204",
        FreshnessCheck.token(sidecar, null, "d41d8cd98f00b204  all.zip\n", null));
    FreshnessConfig meta = FreshnessConfig.fromMap(map("type", "checksum", "object_metadata", true));
    assertEquals("md5val", FreshnessCheck.token(meta, headers("Content-MD5", "md5val"), null, null));
  }

  @Test void hashOverContent() {
    String a = FreshnessCheck.token(FreshnessConfig.fromMap(map("type", "hash")),
        null, null, "hello".getBytes(StandardCharsets.UTF_8));
    String b = FreshnessCheck.sha256Hex("hello");
    assertNotNull(a);
    assertEquals(b, a);
    assertFalse(b.equals(FreshnessCheck.sha256Hex("world")));
  }

  @Test void changedSemantics() {
    assertTrue(FreshnessCheck.changed(null, "x"));     // first run
    assertTrue(FreshnessCheck.changed("x", null));     // can't prove unchanged
    assertTrue(FreshnessCheck.changed("x", "y"));      // differ
    assertFalse(FreshnessCheck.changed("x", "x"));     // unchanged → skip
  }
}
