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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes post-processing scripts for column, table, or schema-level hooks.
 *
 * <p>Scripts are executed in an external shell with variable substitution for:
 * <ul>
 *   <li>${year} - The year being processed</li>
 *   <li>${table} - The table name</li>
 *   <li>${schema} - The schema name</li>
 *   <li>${partition} - The partition identifier</li>
 *   <li>${base_dir} - The base directory for the adapter</li>
 * </ul>
 *
 * <p>Script paths are resolved relative to base directory unless absolute.
 */
public class PostProcessExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessExecutor.class);

  /** Default timeout for script execution (30 minutes). */
  private static final long DEFAULT_TIMEOUT_MINUTES = 30;

  private final Path baseDir;
  private final Map<String, String> environmentVariables;
  private long timeoutMinutes = DEFAULT_TIMEOUT_MINUTES;

  /**
   * Creates a new PostProcessExecutor.
   *
   * @param baseDir Base directory for resolving relative script paths
   */
  public PostProcessExecutor(Path baseDir) {
    this.baseDir = baseDir;
    this.environmentVariables = new HashMap<>();
  }

  /**
   * Creates a new PostProcessExecutor with environment variables.
   *
   * @param baseDir Base directory for resolving relative script paths
   * @param env Additional environment variables for script execution
   */
  public PostProcessExecutor(Path baseDir, Map<String, String> env) {
    this.baseDir = baseDir;
    this.environmentVariables = new HashMap<>(env);
  }

  /**
   * Sets the timeout for script execution.
   *
   * @param timeoutMinutes Timeout in minutes
   * @return this executor for chaining
   */
  public PostProcessExecutor withTimeout(long timeoutMinutes) {
    this.timeoutMinutes = timeoutMinutes;
    return this;
  }

  /**
   * Adds an environment variable for script execution.
   *
   * @param name Variable name
   * @param value Variable value
   * @return this executor for chaining
   */
  public PostProcessExecutor withEnv(String name, String value) {
    this.environmentVariables.put(name, value);
    return this;
  }

  /**
   * Executes a post-processing script.
   *
   * @param config The post-process configuration
   * @param variables Variables for substitution (year, table, schema, partition)
   * @return true if execution succeeded (or started for async), false otherwise
   * @throws PostProcessException if onFailure is ERROR and script fails
   */
  public boolean execute(PostProcessConfig config, Map<String, String> variables)
      throws PostProcessException {

    String scriptPath = resolveScriptPath(config.getScript());
    List<String> command = buildCommand(scriptPath, config.getArgs(), variables);

    if (config.isAsync()) {
      LOGGER.info("Executing post-process '{}' ASYNC: {}", config.getName(),
          String.join(" ", command));
    } else {
      LOGGER.info("Executing post-process '{}': {}", config.getName(),
          String.join(" ", command));
    }

    try {
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.directory(baseDir.toFile());
      pb.redirectErrorStream(true);

      // Set environment variables
      Map<String, String> env = pb.environment();
      env.putAll(environmentVariables);
      env.put("BASE_DIR", baseDir.toString());

      // Add variables to environment as well
      for (Map.Entry<String, String> entry : variables.entrySet()) {
        env.put("POSTPROCESS_" + entry.getKey().toUpperCase(), entry.getValue());
      }

      Process process = pb.start();

      // For async execution, start a background thread to log output and return immediately
      // Thread is NOT daemon so JVM waits for async process to complete (for proper monitoring)
      if (config.isAsync()) {
        final String processName = config.getName();
        Thread outputThread = new Thread(() -> {
          try (BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream(),
                  java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
              LOGGER.info("[{}] {}", processName, line);
            }
          } catch (IOException e) {
            LOGGER.warn("[{}] Error reading output: {}", processName, e.getMessage());
          }
          // Log process exit
          try {
            int exitCode = process.waitFor();
            if (exitCode == 0) {
              LOGGER.info("[{}] Process completed successfully", processName);
            } else {
              LOGGER.warn("[{}] Process exited with code {}", processName, exitCode);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("[{}] Interrupted waiting for process", processName);
          }
        }, "postprocess-" + processName + "-output");
        outputThread.setDaemon(false);
        outputThread.start();

        LOGGER.info("Post-process '{}' started asynchronously (PID: {})",
            config.getName(), process.pid());
        return true;
      }

      // Synchronous execution - wait for completion
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream(),
              java.nio.charset.StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          LOGGER.info("[{}] {}", config.getName(), line);
        }
      }

      boolean completed = process.waitFor(timeoutMinutes, TimeUnit.MINUTES);

      if (!completed) {
        process.destroyForcibly();
        String msg =
            String.format("Post-process '%s' timed out after %d minutes", config.getName(), timeoutMinutes);
        return handleFailure(config, msg, null);
      }

      int exitCode = process.exitValue();
      if (exitCode != 0) {
        String msg =
            String.format("Post-process '%s' failed with exit code %d", config.getName(), exitCode);
        return handleFailure(config, msg, null);
      }

      LOGGER.info("Post-process '{}' completed successfully", config.getName());
      return true;

    } catch (IOException | InterruptedException e) {
      String msg =
          String.format("Post-process '%s' execution error: %s", config.getName(), e.getMessage());
      return handleFailure(config, msg, e);
    }
  }

  /**
   * Resolves script path relative to base directory.
   */
  private String resolveScriptPath(String script) {
    Path scriptPath = Paths.get(script);
    if (scriptPath.isAbsolute()) {
      return script;
    }
    return baseDir.resolve(script).toString();
  }

  /**
   * Builds the command with variable substitution.
   */
  private List<String> buildCommand(String scriptPath, List<String> args,
      Map<String, String> variables) {
    List<String> command = new ArrayList<>();
    command.add(scriptPath);

    for (String arg : args) {
      command.add(substituteVariables(arg, variables));
    }

    return command;
  }

  /**
   * Substitutes ${variable} patterns in a string.
   */
  private String substituteVariables(String input, Map<String, String> variables) {
    String result = input;

    // Add base_dir to variables
    Map<String, String> allVars = new HashMap<>(variables);
    allVars.put("base_dir", baseDir.toString());

    for (Map.Entry<String, String> entry : allVars.entrySet()) {
      String placeholder = "${" + entry.getKey() + "}";
      if (result.contains(placeholder)) {
        result = result.replace(placeholder, entry.getValue());
      }
    }

    return result;
  }

  /**
   * Handles script failure based on onFailure configuration.
   */
  private boolean handleFailure(PostProcessConfig config, String message, Exception cause)
      throws PostProcessException {

    if (config.getOnFailure() == PostProcessConfig.OnFailure.ERROR) {
      LOGGER.error(message);
      throw new PostProcessException(message, cause);
    } else {
      LOGGER.warn(message);
      return false;
    }
  }

  /**
   * Exception thrown when post-processing fails with onFailure=ERROR.
   */
  public static class PostProcessException extends Exception {
    public PostProcessException(String message) {
      super(message);
    }

    public PostProcessException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
