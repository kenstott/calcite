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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Factory for ServiceNow schemas.
 *
 * <p>Operands ({@code instanceUrl}, {@code authType} and its credentials are required):
 * <ul>
 *   <li>{@code instanceUrl}: {@code https://<instance>.service-now.com}. Plain http is accepted
 *   only for a loopback host.
 *   <li>{@code authType}: {@code basic}. {@code oauth_client_credentials} is recognised and
 *   rejected as not implemented yet. It is never inferred from which credentials are present.
 *   <li>{@code username}, {@code password}: for {@code basic}.
 *   <li>{@code tables}: names of the only tables to expose, as a list or comma-separated text.
 *   Default: every table in {@code sys_db_object}.
 *   <li>{@code excludeColumnTypes}: field types whose columns are left out, as a list or
 *   comma-separated text. A column of a type the adapter cannot map is otherwise an error.
 *   <li>{@code pageSize}: rows per request when reading a table; default 1000.
 *   <li>{@code metadataPageSize}: rows per request when reading the metadata tables; default 1000.
 *   <li>{@code maxConcurrentRequests}: default 2. REST traffic shares a small thread pool with the
 *   instance's other integrations.
 *   <li>{@code maxRetries}: retries of a 429 response; default 3.
 *   <li>{@code maxRetryWaitSeconds}: total wait across retries; default 60.
 *   <li>{@code requestTimeoutSeconds}: default 75, above ServiceNow's 60 second Table API quota.
 *   <li>{@code catalogCacheDirectory}: default {@code ~/.calcite/servicenow/catalog-cache}.
 *   <li>{@code catalogCacheTtlMinutes}: default 1440; 0 keeps nothing on disk.
 *   <li>{@code pushdownVerification}: path of a pushdown verification record written by the
 *   harness. Default: the bundled record, which verifies nothing. See
 *   {@link PushdownVerification}.
 *   <li>{@code trustPushdown}: names of pushdown entries to trust without a record, as a list or
 *   comma-separated text. Default: none. See {@link PushdownCapabilities}.
 *   <li>{@code pushdownObserver}: a {@link PushdownObserver}; Java callers only.
 * </ul>
 * Numbers may be given as numbers or as text, because the JDBC driver passes URL text.
 */
public class ServiceNowSchemaFactory implements SchemaFactory {

  public static final ServiceNowSchemaFactory INSTANCE = new ServiceNowSchemaFactory();

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    final URI instanceUrl = instanceUrl(required(operand, "instanceUrl"));
    final ServiceNowAuth auth = auth(operand);
    final int pageSize = integer(operand, "pageSize", 1000, 1);
    final int metadataPageSize = integer(operand, "metadataPageSize", 1000, 1);
    final ServiceNowConnection connection = new ServiceNowConnection(instanceUrl, auth,
        integer(operand, "maxConcurrentRequests", 2, 1),
        integer(operand, "maxRetries", 3, 0),
        Duration.ofSeconds(integer(operand, "maxRetryWaitSeconds", 60, 0)),
        Duration.ofSeconds(integer(operand, "requestTimeoutSeconds", 75, 1)));

    final Object directory = operand.get("catalogCacheDirectory");
    final Path cacheDirectory = directory == null
        ? Paths.get(System.getProperty("user.home"), ".calcite", "servicenow", "catalog-cache")
        : Paths.get(directory.toString());
    final Duration cacheTtl =
        Duration.ofMinutes(integer(operand, "catalogCacheTtlMinutes", 1440, 0));

    final List<String> tables = new ArrayList<>(names(operand, "tables"));
    for (String table : tables) {
      if (!ServiceNowCatalog.NAME.matcher(table).matches()) {
        throw new IllegalArgumentException("Not a ServiceNow table name in 'tables': " + table);
      }
    }
    final Object recordFile = operand.get("pushdownVerification");
    final PushdownCapabilities capabilities = new PushdownCapabilities(
        PushdownVerification.resolve(
            recordFile == null ? null : Paths.get(recordFile.toString()), instanceUrl,
            names(operand, "trustPushdown")));
    final Object observer = operand.get("pushdownObserver");
    if (observer != null && !(observer instanceof PushdownObserver)) {
      throw new IllegalArgumentException("'pushdownObserver' must be a PushdownObserver: "
          + observer.getClass());
    }
    return new ServiceNowSchema(connection, pageSize, metadataPageSize, cacheDirectory,
        cacheTtl, tables, names(operand, "excludeColumnTypes"), capabilities,
        (PushdownObserver) observer);
  }

  private static ServiceNowAuth auth(Map<String, Object> operand) {
    final String authType = required(operand, "authType");
    switch (authType.toLowerCase(Locale.ROOT)) {
    case "basic":
      return ServiceNowAuth.basic(required(operand, "username"), required(operand, "password"));
    case "oauth_client_credentials":
      throw new IllegalArgumentException("authType 'oauth_client_credentials' is not implemented "
          + "yet; use 'basic'");
    default:
      throw new IllegalArgumentException("Unknown authType '" + authType
          + "'; supported: basic");
    }
  }

  private static URI instanceUrl(String text) {
    final URI uri;
    try {
      uri = URI.create(text);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("'instanceUrl' is not a valid URL: " + text, e);
    }
    final String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("'instanceUrl' has no host: " + text);
    }
    final boolean loopback = host.equals("localhost") || host.equals("127.0.0.1");
    if (!"https".equals(uri.getScheme()) && !("http".equals(uri.getScheme()) && loopback)) {
      throw new IllegalArgumentException(
          "'instanceUrl' must be https (http is accepted only for localhost): " + text);
    }
    return uri;
  }

  private static String required(Map<String, Object> operand, String key) {
    final Object value = operand.get(key);
    if (value == null || value.toString().isEmpty()) {
      throw new IllegalArgumentException("ServiceNow operand '" + key + "' is required");
    }
    return value.toString();
  }

  private static int integer(Map<String, Object> operand, String key, int defaultValue,
      int minimum) {
    final Object value = operand.get(key);
    if (value == null) {
      return defaultValue;
    }
    final int number;
    try {
      number = value instanceof Number ? ((Number) value).intValue()
          : Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "ServiceNow operand '" + key + "' is not a whole number: " + value, e);
    }
    if (number < minimum) {
      throw new IllegalArgumentException(
          "ServiceNow operand '" + key + "' must be at least " + minimum + ": " + number);
    }
    return number;
  }

  private static Set<String> names(Map<String, Object> operand, String key) {
    final Object value = operand.get(key);
    final Set<String> names = new LinkedHashSet<>();
    if (value == null) {
      return names;
    }
    final Collection<?> items = value instanceof Collection
        ? (Collection<?>) value : Arrays.asList(value.toString().split(","));
    for (Object item : items) {
      final String text = item.toString().trim();
      if (!text.isEmpty()) {
        names.add(text);
      }
    }
    return names;
  }
}
