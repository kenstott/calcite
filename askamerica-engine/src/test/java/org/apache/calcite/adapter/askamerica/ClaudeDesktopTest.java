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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The setup wizard restarts Claude Desktop and nothing else. The Claude Code command-line
 * tool shares its name ({@code claude}, {@code claude.exe}) and must never be taken for it.
 */
@Tag("unit")
class ClaudeDesktopTest {

  private static Path file(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    return Files.write(path, new byte[0]);
  }

  @Test void theWindowsDesktopProgramIsRecognisedByItsApplicationFiles(@TempDir Path dir)
      throws IOException {
    Path install = dir.resolve("AppData/Local/AnthropicClaude/app-1.2.3");
    file(install.resolve("resources/app.asar"));
    assertTrue(ClaudeDesktop.isDesktopProgram(file(install.resolve("Claude.exe"))));
    assertTrue(ClaudeDesktop.isDesktopProgram(file(install.resolve("claude.exe"))),
        "the program name is compared without regard to case");
  }

  @Test void theMacDesktopProgramIsRecognisedInsideItsApplicationBundle(@TempDir Path dir)
      throws IOException {
    Path contents = dir.resolve("Applications/Claude.app/Contents");
    file(contents.resolve("Resources/app.asar"));
    assertTrue(ClaudeDesktop.isDesktopProgram(file(contents.resolve("MacOS/Claude"))));
  }

  @Test void theClaudeCodeCommandIsNotClaudeDesktop(@TempDir Path dir) throws IOException {
    assertFalse(ClaudeDesktop.isDesktopProgram(file(dir.resolve(".local/bin/claude"))),
        "the Claude Code command on Linux and macOS");
    assertFalse(ClaudeDesktop.isDesktopProgram(file(dir.resolve(".local/bin/claude.exe"))),
        "the Claude Code command on Windows");
    assertFalse(ClaudeDesktop.isDesktopProgram(file(dir.resolve("npm/claude.cmd"))),
        "a command script");
  }

  @Test void aProgramWithAnotherNameIsNotClaudeDesktop(@TempDir Path dir) throws IOException {
    Path install = dir.resolve("SomeApp");
    file(install.resolve("resources/app.asar"));
    assertFalse(ClaudeDesktop.isDesktopProgram(file(install.resolve("SomeApp.exe"))));
  }

  @Test void nothingIsStartedWhenClaudeDesktopWasNotFound(@TempDir Path dir) throws IOException {
    if (System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("mac")) {
      // On macOS the application is started by its identifier, not from a program path.
      return;
    }
    assertThrows(IOException.class, () -> ClaudeDesktop.relaunch(null));
    Path claudeCode = file(dir.resolve(".local/bin/claude"));
    assertThrows(IOException.class, () -> ClaudeDesktop.relaunch(claudeCode),
        "the Claude Code command is refused, not started");
  }
}
