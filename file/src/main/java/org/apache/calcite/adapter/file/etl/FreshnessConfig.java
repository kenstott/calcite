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

import java.util.Map;

/**
 * Optional freshness check for {@code snapshot} / {@code computed_delta} sources:
 * when the upstream is unchanged since the last successful run, the pipeline skips
 * the pull and the commit so no redundant Iceberg snapshot is created.
 *
 * <p>A {@code type} discriminator selects the cheap signal to compare; per-type
 * options say where to read it from. Off by default (absent {@code freshness:}).
 *
 * <pre>{@code
 * freshness:
 *   type: count
 *   count_probe: { url: "...?resultsPerPage=1", path: "$.totalResults" }
 * }</pre>
 *
 * <p>For a GraphQL source, a small POST query reads a cheap signal (e.g. the global
 * max {@code updatedAt}) and {@code path} extracts the comparable value:
 *
 * <pre>{@code
 * freshness:
 *   type: graphql
 *   query: "{ securityAdvisories(first:1, orderBy:{field:UPDATED_AT, direction:DESC}){ nodes{ updatedAt } } }"
 *   path: data.securityAdvisories.nodes[0].updatedAt
 * }</pre>
 */
final class FreshnessConfig {

  /** Freshness signal. See {@code period_dimensions_design.md} §8. */
  enum Type {
    /** HTTP {@code ETag} (conditional / 304). Pre-download. */
    ETAG,
    /** HTTP {@code Last-Modified}. Pre-download. */
    LAST_MODIFIED,
    /** HTTP {@code Content-Length} / object size. Pre-download (weak). */
    SIZE,
    /** A version/date value in cheap metadata ({@code catalogVersion}). Pre-download. */
    VERSION,
    /** A source-provided digest (sidecar / object {@code md5Hash}). Pre-download. */
    CHECKSUM,
    /** Source record count ({@code totalResults}). Pre-download (weak; misses revisions). */
    COUNT,
    /**
     * A value (date or scalar) read from a GraphQL endpoint via a small POST query.
     * Pre-download; the general signal for GraphQL sources — e.g. the global max
     * {@code updatedAt} of a result set. {@code query} is the GraphQL POST body and
     * {@code path} extracts the comparable value from the JSON response.
     */
    GRAPHQL,
    /** Digest computed over the downloaded body. Post-download (skips commit only). */
    HASH;

    static Type fromString(String value) {
      if (value == null) {
        return null;
      }
      switch (value.trim().toLowerCase()) {
      case "etag":
        return ETAG;
      case "last_modified":
      case "lastmodified":
        return LAST_MODIFIED;
      case "size":
        return SIZE;
      case "version":
        return VERSION;
      case "checksum":
        return CHECKSUM;
      case "count":
        return COUNT;
      case "graphql":
      case "gql":
        return GRAPHQL;
      case "hash":
        return HASH;
      default:
        return null;
      }
    }
  }

  private final Type type;
  private final String probeUrl;
  private final String versionField;
  private final String checksumUrl;
  private final boolean objectMetadata;
  private final String countUrl;
  private final String countPath;
  private final String query;
  private final String queryPath;
  private final String normalize;
  private final String watermarkVar;

  private FreshnessConfig(Type type, String probeUrl, String versionField,
      String checksumUrl, boolean objectMetadata, String countUrl, String countPath,
      String query, String queryPath, String normalize, String watermarkVar) {
    this.type = type;
    this.probeUrl = probeUrl;
    this.versionField = versionField;
    this.checksumUrl = checksumUrl;
    this.objectMetadata = objectMetadata;
    this.countUrl = countUrl;
    this.countPath = countPath;
    this.query = query;
    this.queryPath = queryPath;
    this.normalize = normalize;
    this.watermarkVar = watermarkVar;
  }

  Type getType() {
    return type;
  }

  /** Optional override URL for the pre-download probe (defaults to the source URL). */
  String getProbeUrl() {
    return probeUrl;
  }

  /** Path into the metadata/payload head for {@link Type#VERSION}. */
  String getVersionField() {
    return versionField;
  }

  /** Sidecar digest URL for {@link Type#CHECKSUM}. */
  String getChecksumUrl() {
    return checksumUrl;
  }

  /** Whether {@link Type#CHECKSUM} reads the object's storage metadata (ETag/md5). */
  boolean isObjectMetadata() {
    return objectMetadata;
  }

  /** Probe URL for {@link Type#COUNT}. */
  String getCountUrl() {
    return countUrl;
  }

  /** Path to the count value for {@link Type#COUNT}. */
  String getCountPath() {
    return countPath;
  }

  /** GraphQL query (POST body) for {@link Type#GRAPHQL}; sent to the probe/source URL. */
  String getQuery() {
    return query;
  }

  /** JSON path that extracts the comparable value from the GraphQL response for {@link Type#GRAPHQL}. */
  String getQueryPath() {
    return queryPath;
  }

  /** Optional normalization hint for {@link Type#HASH}. */
  String getNormalize() {
    return normalize;
  }

  /**
   * Name of a fetch variable to populate with the recovered watermark (the last committed
   * freshness token) before the pull, enabling an incremental/delta fetch. For a GRAPHQL
   * source the token is the previous max {@code updatedAt}; injected as e.g. {@code updatedSince}
   * so the crawl fetches only records changed since the prior snapshot. Null disables delta
   * (full pull). Pairs with an Iceberg append write so the delta adds to, not replaces, the table.
   */
  String getWatermarkVar() {
    return watermarkVar;
  }

  /**
   * Parses a {@code freshness:} map, or returns null if absent/invalid (freshness off).
   */
  @SuppressWarnings("unchecked")
  static FreshnessConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }
    Type type = Type.fromString(asString(map.get("type")));
    if (type == null) {
      return null;
    }
    String probeUrl = asString(map.get("probe_url"));
    String versionField = asString(map.get("version_field"));
    String checksumUrl = asString(map.get("checksum_url"));
    boolean objectMetadata = Boolean.TRUE.equals(map.get("object_metadata"))
        || "true".equalsIgnoreCase(asString(map.get("object_metadata")));
    String countUrl = null;
    String countPath = null;
    Object countProbe = map.get("count_probe");
    if (countProbe instanceof Map) {
      Map<String, Object> cp = (Map<String, Object>) countProbe;
      countUrl = asString(cp.get("url"));
      countPath = asString(cp.get("path"));
    }
    String query = asString(map.get("query"));
    String queryPath = asString(map.get("path"));
    String normalize = asString(map.get("normalize"));
    String watermarkVar = asString(map.get("watermark_var"));
    return new FreshnessConfig(type, probeUrl, versionField, checksumUrl, objectMetadata,
        countUrl, countPath, query, queryPath, normalize, watermarkVar);
  }

  private static String asString(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
