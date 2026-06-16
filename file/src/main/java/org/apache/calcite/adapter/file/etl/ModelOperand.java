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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Typed, dotted-path accessor over the derived file-schema operand.
 *
 * <p>Every schema's fully-derived operand (the parsed schema YAML, env already
 * resolved at load, with model-JSON overrides applied) is captured here by
 * {@link FileSchemaFactory} keyed by schema name. Any class then reads a
 * configuration value by its natural path in the model — for example
 * {@code ModelOperand.getString("cyber_vuln.partitionedTables.vuln_cross_refs.source.headers.Authorization")}
 * or {@code ModelOperand.getInt("test.thing.valueInt")} — instead of
 * {@code System.getenv} or hand-threaded config.
 *
 * <p>Paths are rooted at the schema name (the first segment), so dependency
 * schemas created in the same run (siblings, e.g. {@code econ_reference} under
 * {@code econ}) each keep their own tree and never collide. Subsequent segments
 * walk nested maps; a segment landing on a list matches an element by its
 * {@code name} field (e.g. {@code partitionedTables}) or by numeric index.
 *
 * <p>Values are literal (resolved at YAML load); a surviving {@code ${VAR}}
 * token in a string is resolved on read via {@link VariableResolver}, the one
 * place permitted to read the environment. Backed by a {@link ConcurrentHashMap}
 * for the parallel ETL workers.
 */
public final class ModelOperand {

  /** schema name -> that schema's derived operand tree. */
  private static final ConcurrentHashMap<String, Object> BY_SCHEMA = new ConcurrentHashMap<>();

  private ModelOperand() {
  }

  /** Capture a schema's derived operand. Called once per schema at {@code FileSchemaFactory.create}. */
  public static void capture(String schemaName, Map<String, Object> operand) {
    if (schemaName != null && operand != null) {
      BY_SCHEMA.put(schemaName, operand);
    }
  }

  /** String value at {@code path} (rooted at schema name), {@code ${VAR}} resolved, or {@code null}. */
  public static String getString(String path) {
    return getString(path, null);
  }

  /** String value at {@code path}, {@code ${VAR}} resolved, or {@code defaultValue} if absent. */
  public static String getString(String path, String defaultValue) {
    Object v = navigate(path);
    if (v == null) {
      return defaultValue;
    }
    String s = String.valueOf(v);
    return s.indexOf("${") >= 0 ? VariableResolver.resolveEnvVars(s) : s;
  }

  /** int value at {@code path}, or {@code defaultValue} if absent/unparseable. */
  public static int getInt(String path, int defaultValue) {
    Object v = navigate(path);
    if (v instanceof Number) {
      return ((Number) v).intValue();
    }
    String s = getString(path);
    if (s == null || s.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** long value at {@code path}, or {@code defaultValue} if absent/unparseable. */
  public static long getLong(String path, long defaultValue) {
    Object v = navigate(path);
    if (v instanceof Number) {
      return ((Number) v).longValue();
    }
    String s = getString(path);
    if (s == null || s.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(s.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** boolean value at {@code path}, or {@code defaultValue} if absent. */
  public static boolean getBoolean(String path, boolean defaultValue) {
    Object v = navigate(path);
    if (v instanceof Boolean) {
      return (Boolean) v;
    }
    String s = getString(path);
    return s != null ? "true".equalsIgnoreCase(s.trim()) : defaultValue;
  }

  /** True if a non-null value exists at {@code path}. */
  public static boolean has(String path) {
    return navigate(path) != null;
  }

  private static Object navigate(String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }
    Object current = BY_SCHEMA;
    for (String segment : path.split("\\.")) {
      if (current instanceof Map) {
        current = ((Map<?, ?>) current).get(segment);
      } else if (current instanceof List) {
        current = fromList((List<?>) current, segment);
      } else {
        return null;
      }
      if (current == null) {
        return null;
      }
    }
    return current;
  }

  /** Resolve a list segment by numeric index or by an element's {@code name} field. */
  private static Object fromList(List<?> list, String segment) {
    try {
      int idx = Integer.parseInt(segment);
      return idx >= 0 && idx < list.size() ? list.get(idx) : null;
    } catch (NumberFormatException ignore) {
      // not an index — match by name
    }
    for (Object element : list) {
      if (element instanceof Map && segment.equals(((Map<?, ?>) element).get("name"))) {
        return element;
      }
    }
    return null;
  }

  /** Visible for testing — clears all captured schemas. */
  public static void clear() {
    BY_SCHEMA.clear();
  }
}
