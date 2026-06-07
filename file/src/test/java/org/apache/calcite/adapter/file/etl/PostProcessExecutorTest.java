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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PostProcessExecutor}.
 */
@Tag("unit")
class PostProcessExecutorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PostProcessExecutorTest.class);

  @TempDir
  Path tempDir;

  private PostProcessExecutor executor;

  @BeforeEach
  void setUp() {
    executor = new PostProcessExecutor(tempDir);
  }

  @Test void testExecuteSuccessfulScript() throws Exception {
    // Create a simple script that exits 0
    Path script = createScript("success.sh", "#!/bin/bash\necho \"Hello World\"\nexit 0");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_success")
        .script(script.toString())
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    boolean result = executor.execute(config, variables);
    assertTrue(result);
  }

  @Test void testExecuteFailingScriptWithWarn() throws Exception {
    Path script = createScript("fail.sh", "#!/bin/bash\nexit 1");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_fail_warn")
        .script(script.toString())
        .onFailure(PostProcessConfig.OnFailure.WARN)
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    boolean result = executor.execute(config, variables);
    assertFalse(result);
  }

  @Test void testExecuteFailingScriptWithError() throws Exception {
    Path script = createScript("fail.sh", "#!/bin/bash\nexit 1");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_fail_error")
        .script(script.toString())
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    Map<String, String> variables = new HashMap<String, String>();

    assertThrows(PostProcessExecutor.PostProcessException.class,
        () -> executor.execute(config, variables));
  }

  @Test void testVariableSubstitution() throws Exception {
    // Script that writes variables to a file for verification
    Path outputFile = tempDir.resolve("output.txt");
    Path script =
        createScript("vars.sh", "#!/bin/bash\necho \"$1 $2\" > " + outputFile.toString() + "\nexit 0");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_vars")
        .script(script.toString())
        .args(Arrays.asList("${year}", "${table}"))
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    variables.put("table", "sales");

    boolean result = executor.execute(config, variables);
    assertTrue(result);

    // Verify variable substitution worked
    String output = new String(Files.readAllBytes(outputFile)).trim();
    assertEquals("2024 sales", output);
  }

  @Test void testBaseDirectoryVariableSubstitution() throws Exception {
    Path outputFile = tempDir.resolve("basedir_output.txt");
    Path script =
        createScript("basedir.sh", "#!/bin/bash\necho \"$1\" > " + outputFile.toString() + "\nexit 0");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_basedir")
        .script(script.toString())
        .args(Collections.singletonList("${base_dir}"))
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    boolean result = executor.execute(config, new HashMap<String, String>());
    assertTrue(result);

    String output = new String(Files.readAllBytes(outputFile)).trim();
    assertEquals(tempDir.toString(), output);
  }

  @Test void testEnvironmentVariables() throws Exception {
    Path outputFile = tempDir.resolve("env_output.txt");
    Path script =
        createScript("env.sh", "#!/bin/bash\necho \"$POSTPROCESS_YEAR $MY_VAR\" > "
            + outputFile.toString() + "\nexit 0");

    Map<String, String> env = new HashMap<String, String>();
    env.put("MY_VAR", "custom_value");
    PostProcessExecutor executorWithEnv = new PostProcessExecutor(tempDir, env);

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_env")
        .script(script.toString())
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2025");

    boolean result = executorWithEnv.execute(config, variables);
    assertTrue(result);

    String output = new String(Files.readAllBytes(outputFile)).trim();
    assertTrue(output.contains("2025"));
    assertTrue(output.contains("custom_value"));
  }

  @Test void testWithEnvChaining() {
    PostProcessExecutor chained = executor.withEnv("KEY1", "VALUE1");
    assertNotNull(chained);
    assertEquals(executor, chained);
  }

  @Test void testWithTimeoutChaining() {
    PostProcessExecutor chained = executor.withTimeout(5);
    assertNotNull(chained);
    assertEquals(executor, chained);
  }

  @Test void testRelativeScriptPath() throws Exception {
    // Script in base directory should be resolved
    Path script = createScript("relative.sh", "#!/bin/bash\nexit 0");
    // Use relative path (just filename since it's in tempDir)
    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_relative")
        .script(script.getFileName().toString())
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    boolean result = executor.execute(config, new HashMap<String, String>());
    assertTrue(result);
  }

  @Test void testAbsoluteScriptPath() throws Exception {
    Path script = createScript("absolute.sh", "#!/bin/bash\nexit 0");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_absolute")
        .script(script.toAbsolutePath().toString())
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    boolean result = executor.execute(config, new HashMap<String, String>());
    assertTrue(result);
  }

  @Test void testNonExistentScriptWithWarn() throws Exception {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_nonexistent")
        .script("/nonexistent/path/script.sh")
        .onFailure(PostProcessConfig.OnFailure.WARN)
        .build();

    boolean result = executor.execute(config, new HashMap<String, String>());
    assertFalse(result);
  }

  @Test void testNonExistentScriptWithError() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_nonexistent_error")
        .script("/nonexistent/path/script.sh")
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    assertThrows(PostProcessExecutor.PostProcessException.class,
        () -> executor.execute(config, new HashMap<String, String>()));
  }

  @Test void testAsyncExecution() throws Exception {
    Path script =
        createScript("async.sh", "#!/bin/bash\necho \"async running\"\nexit 0");

    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_async")
        .script(script.toString())
        .async(true)
        .onFailure(PostProcessConfig.OnFailure.ERROR)
        .build();

    boolean result = executor.execute(config, new HashMap<String, String>());
    // Async always returns true immediately
    assertTrue(result);
  }

  @Test void testPostProcessExceptionWithMessage() {
    PostProcessExecutor.PostProcessException ex =
        new PostProcessExecutor.PostProcessException("test error");
    assertEquals("test error", ex.getMessage());
  }

  @Test void testPostProcessExceptionWithCause() {
    IOException cause = new IOException("io error");
    PostProcessExecutor.PostProcessException ex =
        new PostProcessExecutor.PostProcessException("wrapped", cause);
    assertEquals("wrapped", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  private Path createScript(String name, String content) throws IOException {
    Path script = tempDir.resolve(name);
    Files.write(script, content.getBytes());
    Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.OWNER_WRITE);
    perms.add(PosixFilePermission.OWNER_EXECUTE);
    try {
      Files.setPosixFilePermissions(script, perms);
    } catch (UnsupportedOperationException e) {
      // Windows doesn't support POSIX permissions
      LOGGER.debug("POSIX permissions not supported: {}", e.getMessage());
    }
    return script;
  }
}
