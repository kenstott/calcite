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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for variable substitution in configuration values.
 *
 * <p>Supports the following patterns:
 * <ul>
 *   <li>{@code {varName}} - Substitutes from provided variables map</li>
 *   <li>{@code {env:VAR_NAME}} - Substitutes from environment variable or system property</li>
 *   <li>{@code {env:VAR_NAME:default}} - Environment variable with default value</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Simple substitution
 * String url = VariableResolver.substitute(
 *     "https://api.example.com/{year}/data",
 *     Map.of("year", "2024"));
 * // Result: "https://api.example.com/2024/data"
 *
 * // Environment variable with default
 * String startYear = VariableResolver.substitute("{env:ETL_START_YEAR:2020}", null);
 * // Result: value of ETL_START_YEAR env var, or "2020" if not set
 * }</pre>
 */
public final class VariableResolver {

  private static final Pattern VAR_PATTERN = Pattern.compile("\\{([^}]+)\\}");
  private static final Pattern ENV_PATTERN = Pattern.compile("env:(.+)");
  /**
   * Pattern for ${VAR_NAME} style environment variable references.
   * Uses [^{}]+ to match only innermost patterns (no nested braces),
   * allowing iterative resolution from inside out.
   */
  private static final Pattern DOLLAR_VAR_PATTERN = Pattern.compile("\\$\\{([^{}]+)\\}");

  private VariableResolver() {
    // Utility class
  }

  /**
   * Substitutes variables in a string template.
   *
   * @param template Template string with {variable} placeholders
   * @param variables Map of variable names to values (may be null)
   * @return String with variables substituted
   */
  public static String substitute(String template, Map<String, String> variables) {
    if (template == null || template.isEmpty()) {
      return template;
    }

    // First resolve ${VAR} style environment variables
    String resolved = resolveEnvVars(template);

    // Then resolve {variable} style placeholders
    StringBuffer result = new StringBuffer();
    Matcher matcher = VAR_PATTERN.matcher(resolved);

    while (matcher.find()) {
      String varExpr = matcher.group(1);
      String replacement = resolveVariable(varExpr, variables);
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Resolves ${VAR_NAME} style environment variable references.
   *
   * <p>This is the common shell-style variable syntax. Supports defaults
   * with ${VAR_NAME:-default} syntax. Resolution is iterative, so nested
   * variables like ${A:-${B}/path} are fully resolved.
   *
   * @param template Template string with ${VAR} placeholders
   * @return String with environment variables resolved
   */
  public static String resolveEnvVars(String template) {
    if (template == null || template.isEmpty()) {
      return template;
    }

    // Iterative resolution to handle nested variables
    String current = template;
    int maxIterations = 10; // Prevent infinite loops
    for (int i = 0; i < maxIterations; i++) {
      String resolved = resolveEnvVarsOnce(current);
      if (resolved.equals(current)) {
        // No more substitutions made
        break;
      }
      current = resolved;
    }
    return current;
  }

  /**
   * Single pass of ${VAR} resolution.
   */
  private static String resolveEnvVarsOnce(String template) {
    StringBuffer result = new StringBuffer();
    Matcher matcher = DOLLAR_VAR_PATTERN.matcher(template);

    while (matcher.find()) {
      String varExpr = matcher.group(1);
      String envName;
      String defaultValue = "";

      // Support ${VAR:-default} syntax
      int colonIdx = varExpr.indexOf(":-");
      if (colonIdx > 0) {
        envName = varExpr.substring(0, colonIdx);
        defaultValue = varExpr.substring(colonIdx + 2);
      } else {
        // Also support ${VAR:default} (single colon) for backward compatibility
        colonIdx = varExpr.indexOf(':');
        if (colonIdx > 0) {
          envName = varExpr.substring(0, colonIdx);
          defaultValue = varExpr.substring(colonIdx + 1);
        } else {
          envName = varExpr;
        }
      }

      // Try environment variable first, then system property
      String resolved = System.getenv(envName);
      if (resolved == null || resolved.isEmpty()) {
        resolved = System.getProperty(envName);
      }
      if (resolved == null || resolved.isEmpty()) {
        resolved = defaultValue;
      }

      // If still empty, keep original placeholder for debugging
      if (resolved.isEmpty()) {
        resolved = "${" + varExpr + "}";
      }

      matcher.appendReplacement(result, Matcher.quoteReplacement(resolved));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Resolves a single variable expression.
   *
   * @param varExpr Variable expression (without braces)
   * @param variables Map of variable names to values (may be null)
   * @return Resolved value, or original expression if not resolvable
   */
  private static String resolveVariable(String varExpr, Map<String, String> variables) {
    Matcher envMatcher = ENV_PATTERN.matcher(varExpr);
    if (envMatcher.matches()) {
      return resolveEnvVariable(envMatcher.group(1));
    }

    // Regular variable from map
    if (variables != null && variables.containsKey(varExpr)) {
      return variables.get(varExpr);
    }

    // Keep original if not found
    return "{" + varExpr + "}";
  }

  /**
   * Resolves an environment variable expression, supporting default values.
   *
   * <p>Format: {@code VAR_NAME} or {@code VAR_NAME:default}
   *
   * @param envExpr Environment variable expression
   * @return Resolved value
   */
  private static String resolveEnvVariable(String envExpr) {
    String envName;
    String defaultValue = "";

    int colonIdx = envExpr.indexOf(':');
    if (colonIdx > 0) {
      envName = envExpr.substring(0, colonIdx);
      defaultValue = envExpr.substring(colonIdx + 1);
    } else {
      envName = envExpr;
    }

    // Try environment variable first, then system property
    String resolved = System.getenv(envName);
    if (resolved == null || resolved.isEmpty()) {
      resolved = System.getProperty(envName);
    }
    if (resolved == null || resolved.isEmpty()) {
      resolved = defaultValue;
    }

    return resolved;
  }

  /**
   * Resolves a variable expression to an integer.
   *
   * <p>Supports plain integers, environment variables, and environment
   * variables with defaults.
   *
   * @param value String value to resolve
   * @return Resolved integer, or null if not resolvable
   */
  public static Integer resolveInteger(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    // If it's a variable expression, substitute first
    String resolved = substitute(value, null);

    // Parse as integer
    try {
      return Integer.parseInt(resolved.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
