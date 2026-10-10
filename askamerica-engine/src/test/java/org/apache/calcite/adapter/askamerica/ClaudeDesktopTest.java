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
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

  // --- how Claude Desktop is started again ------------------------------------------------

  private static final ClaudeDesktop.StoreLookup NO_STORE_BUILD = () -> null;
  private static final ClaudeDesktop.StoreLookup STORE_BUILD = () -> "Claude_pzs8sxrjxfjjc!Claude";

  @Test void withNoClaudeDesktopNothingIsStarted(@TempDir Path dir) throws IOException {
    for (String os : new String[] {"Windows 11", "Linux"}) {
      IOException e = assertThrows(IOException.class,
          () -> ClaudeDesktop.relaunchCommand(os, null, NO_STORE_BUILD));
      assertTrue(e.getMessage().contains("nothing was started"), e.getMessage());
    }
    Path claudeCode = file(dir.resolve(".local/bin/claude.exe"));
    assertThrows(IOException.class,
        () -> ClaudeDesktop.relaunchCommand("Windows 11", claudeCode, NO_STORE_BUILD),
        "the Claude Code command is refused, not started");
  }

  @Test void aClassicInstallIsStartedFromTheProgramThatWasRunning(@TempDir Path dir)
      throws IOException {
    Path install = dir.resolve("AppData/Local/AnthropicClaude/app-1.2.3");
    file(install.resolve("resources/app.asar"));
    Path program = file(install.resolve("Claude.exe"));
    assertEquals(Collections.singletonList(program.toString()),
        ClaudeDesktop.relaunchCommand("Windows 11", program, NO_STORE_BUILD));
    assertEquals(Collections.singletonList(program.toString()),
        ClaudeDesktop.relaunchCommand("Windows 11", program, STORE_BUILD),
        "a classic install that was running is restarted as it was, Store build present or not");
  }

  @Test void theStoreBuildIsStartedByItsApplicationIdentityNeverByItsPath() throws IOException {
    java.util.List<String> expected =
        Arrays.asList("explorer.exe", "shell:AppsFolder\\Claude_pzs8sxrjxfjjc!Claude");
    assertEquals(expected, ClaudeDesktop.relaunchCommand("Windows 11", null, STORE_BUILD),
        "installed from the Store and not running");
    Path storeProgram = java.nio.file.Paths.get(
        "C:\\Program Files\\WindowsApps\\Claude_2.31226.1.0_x64__pzs8sxrjxfjjc\\app\\Claude.exe");
    assertEquals(expected, ClaudeDesktop.relaunchCommand("Windows 11", storeProgram, STORE_BUILD),
        "running from WindowsApps");
  }

  @Test void onMacOsItIsStartedByItsApplicationIdentifier() throws IOException {
    assertEquals(Arrays.asList("open", "-b", "com.anthropic.claudefordesktop"),
        ClaudeDesktop.relaunchCommand("Mac OS X", null, NO_STORE_BUILD));
  }

  @Test void onlyAPackageIdentityOfTheRightShapeIsAccepted() {
    assertEquals("Claude_pzs8sxrjxfjjc!Claude",
        ClaudeDesktop.parseAppUserModelId("\r\nClaude_pzs8sxrjxfjjc!Claude\r\n"));
    assertNull(ClaudeDesktop.parseAppUserModelId(""));
    assertNull(ClaudeDesktop.parseAppUserModelId(null));
    assertNull(ClaudeDesktop.parseAppUserModelId("Get-AppxPackage : Access is denied."));
    assertNull(ClaudeDesktop.parseAppUserModelId("claude!x"), "the bare name is not an identity");
  }

  // --- the program Claude Desktop is told to start ------------------------------------------

  @Test void theConfiguredLauncherIsTheRunningProgramAndMustExist(@TempDir Path dir)
      throws IOException {
    String key = "askamerica.launcher.command";
    String before = System.getProperty(key);
    try {
      System.clearProperty(key);
      IOException unknown = assertThrows(IOException.class, SetupWindow::executablePath);
      assertTrue(unknown.getMessage().contains("was not configured"), unknown.getMessage());

      System.setProperty(key, dir.resolve("Program Files/AskAmerica MCP/AskAmerica MCP.exe").toString());
      IOException missing = assertThrows(IOException.class, SetupWindow::executablePath,
          "a launcher that is not there is never written into Claude Desktop's config");
      assertTrue(missing.getMessage().contains("is not at"), missing.getMessage());

      Path launcher = file(dir.resolve("installed/AskAmerica MCP.exe"));
      System.setProperty(key, launcher.toString());
      assertEquals(launcher.toString(), SetupWindow.executablePath());
    } finally {
      if (before == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, before);
      }
    }
  }
}
