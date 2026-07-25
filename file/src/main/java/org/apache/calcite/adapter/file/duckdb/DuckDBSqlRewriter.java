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
package org.apache.calcite.adapter.file.duckdb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transparently rewrites the reserved-keyword statistical aggregates
 * ({@code corr}, {@code regr_slope}, {@code regr_intercept}, {@code regr_r2},
 * {@code regr_avgx}, {@code regr_avgy}, {@code regr_sxy}) to their non-reserved
 * {@code AGG_*} aliases before Calcite parses the SQL.
 *
 * <p>Those names are reserved words in Calcite's parser grammar, so {@code corr(...)}
 * fails at parse time and can never reach the operator table. The aliases are registered
 * as aggregate declarations (see {@code FileAdapterFunctions} / {@code DuckDBStatsFunctions})
 * and mapped back to the native DuckDB name on unparse (see {@code DuckDBFunctionMapping}).
 * This rewriter is the middle piece: it lets users keep typing the real names while the
 * DuckDB engine computes them.
 *
 * <p>DuckDB-engine only: the aliases only push down under DuckDB, so the rewrite is applied
 * exclusively when a caller wraps a DuckDB-backed connection.
 */
public final class DuckDBSqlRewriter {

  /** Reserved function name (lower-case) -> non-reserved alias. */
  private static final Map<String, String> RESERVED_TO_ALIAS = new LinkedHashMap<>();

  static {
    RESERVED_TO_ALIAS.put("corr", "agg_corr");
    RESERVED_TO_ALIAS.put("regr_slope", "agg_regr_slope");
    RESERVED_TO_ALIAS.put("regr_intercept", "agg_regr_intercept");
    RESERVED_TO_ALIAS.put("regr_r2", "agg_regr_r2");
    RESERVED_TO_ALIAS.put("regr_avgx", "agg_regr_avgx");
    RESERVED_TO_ALIAS.put("regr_avgy", "agg_regr_avgy");
    RESERVED_TO_ALIAS.put("regr_sxy", "agg_regr_sxy");
  }

  private DuckDBSqlRewriter() {
  }

  /**
   * Rewrites reserved statistical-aggregate function calls to their aliases. Only replaces
   * a name when it appears as a whole word immediately followed by {@code (} and is OUTSIDE
   * a string literal ({@code '...'}) or a quoted identifier ({@code "..."}), so column
   * names, quoted identifiers, and string contents are never touched.
   *
   * @param sql the incoming SQL (may be null)
   * @return the rewritten SQL, or the original when there is nothing to change
   */
  public static String rewrite(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    StringBuilder out = new StringBuilder(sql.length() + 16);
    int n = sql.length();
    int i = 0;
    boolean changed = false;
    while (i < n) {
      char ch = sql.charAt(i);
      if (ch == '\'' || ch == '"') {
        // Copy the whole quoted span verbatim (handles doubled-quote escapes).
        char q = ch;
        out.append(ch);
        i++;
        while (i < n) {
          char c = sql.charAt(i);
          out.append(c);
          i++;
          if (c == q) {
            if (i < n && sql.charAt(i) == q) {
              out.append(q);
              i++;               // escaped quote — stay inside
            } else {
              break;             // closing quote
            }
          }
        }
        continue;
      }
      if (isIdentStart(ch)) {
        int j = i + 1;
        while (j < n && isIdentPart(sql.charAt(j))) {
          j++;
        }
        String word = sql.substring(i, j);
        // A function call is the word followed (after optional whitespace) by '('.
        int k = j;
        while (k < n && Character.isWhitespace(sql.charAt(k))) {
          k++;
        }
        String alias = k < n && sql.charAt(k) == '('
            ? RESERVED_TO_ALIAS.get(word.toLowerCase(java.util.Locale.ROOT)) : null;
        if (alias != null) {
          out.append(alias);
          changed = true;
        } else {
          out.append(word);
        }
        i = j;
        continue;
      }
      out.append(ch);
      i++;
    }
    return changed ? out.toString() : sql;
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Wraps a DuckDB-backed JDBC connection so every SQL string it runs is passed through
   * {@link #rewrite(String)} first. Statements created by the wrapped connection rewrite on
   * execute; prepared statements rewrite on creation.
   *
   * @param conn the underlying connection (returned unchanged if null)
   * @return a rewriting proxy over {@code conn}
   */
  public static Connection wrap(Connection conn) {
    if (conn == null) {
      return null;
    }
    return (Connection) Proxy.newProxyInstance(
        DuckDBSqlRewriter.class.getClassLoader(),
        new Class<?>[] { Connection.class },
        new ConnHandler(conn));
  }

  /** Rewrites {@code prepareStatement(sql)} eagerly and wraps plain statements. */
  private static final class ConnHandler implements InvocationHandler {
    private final Connection target;

    ConnHandler(Connection target) {
      this.target = target;
    }

    @Override public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String name = method.getName();
      if (("prepareStatement".equals(name) || "prepareCall".equals(name))
          && args != null && args.length > 0 && args[0] instanceof String) {
        args[0] = rewrite((String) args[0]);
      }
      Object result = invokeTarget(method, args);
      if (result instanceof Statement && ("createStatement".equals(name))) {
        return Proxy.newProxyInstance(
            DuckDBSqlRewriter.class.getClassLoader(),
            new Class<?>[] { Statement.class },
            new StmtHandler((Statement) result));
      }
      return result;
    }

    private Object invokeTarget(Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  /** Rewrites the SQL argument of {@code execute*(String)} calls on a plain Statement. */
  private static final class StmtHandler implements InvocationHandler {
    private final Statement target;

    StmtHandler(Statement target) {
      this.target = target;
    }

    @Override public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String name = method.getName();
      if ((name.startsWith("execute") || "addBatch".equals(name))
          && args != null && args.length > 0 && args[0] instanceof String) {
        args[0] = rewrite((String) args[0]);
      }
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  /** Marker so callers can avoid importing {@link PreparedStatement} solely for javadoc. */
  static Class<?> preparedStatementType() {
    return PreparedStatement.class;
  }
}
