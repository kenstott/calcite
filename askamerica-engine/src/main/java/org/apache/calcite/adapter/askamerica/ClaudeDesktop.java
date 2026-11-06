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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Finds, quits and restarts the Claude Desktop application, and nothing else.
 *
 * <p>Claude Desktop is never identified by the bare name {@code Claude}: the Claude Code
 * command-line tool answers to the same name (its Windows program is also {@code claude.exe},
 * and {@code claude} is on the PATH of anyone who has it). It is identified by what it is: an
 * application whose program sits beside its own {@code resources/app.asar} (on macOS, inside
 * {@code Claude.app}). A program named {@code claude} without that is some other program and
 * is left alone.
 *
 * <p>When Claude Desktop cannot be found, nothing is started.
 */
final class ClaudeDesktop {

  /** Claude Desktop's application identifier on macOS. */
  static final String MAC_BUNDLE_ID = "com.anthropic.claudefordesktop";

  private ClaudeDesktop() {
  }

  /**
   * Whether {@code program} is Claude Desktop's own executable.
   *
   * @param program the path a running process reports for its executable
   */
  static boolean isDesktopProgram(Path program) {
    Path fileName = program.getFileName();
    if (fileName == null) {
      return false;
    }
    String name = fileName.toString().toLowerCase(Locale.ROOT);
    if (!name.equals("claude") && !name.equals("claude.exe")) {
      return false;
    }
    Path dir = program.getParent();
    if (dir == null) {
      return false;
    }
    // Windows and Linux: <install>/Claude(.exe) beside <install>/resources/app.asar.
    if (Files.isRegularFile(dir.resolve("resources").resolve("app.asar"))) {
      return true;
    }
    // macOS: Claude.app/Contents/MacOS/Claude, with Claude.app/Contents/Resources/app.asar.
    Path contents = dir.getParent();
    Path macOsDir = dir.getFileName();
    return contents != null && macOsDir != null && macOsDir.toString().equals("MacOS")
        && Files.isRegularFile(contents.resolve("Resources").resolve("app.asar"));
  }

  /** The running processes that are Claude Desktop. Empty when it is not running. */
  static List<ProcessHandle> running() {
    List<ProcessHandle> found = new ArrayList<ProcessHandle>();
    ProcessHandle.allProcesses().forEach(handle -> {
      Optional<String> command = handle.info().command();
      if (command.isPresent() && isDesktopProgram(Paths.get(command.get()))) {
        found.add(handle);
      }
    });
    return found;
  }

  /** The program of a running Claude Desktop, or null when it is not running. */
  static Path runningProgram() {
    for (ProcessHandle handle : running()) {
      Optional<String> command = handle.info().command();
      if (command.isPresent()) {
        return Paths.get(command.get());
      }
    }
    return null;
  }

  /**
   * Asks Claude Desktop to quit: a request the application can act on normally, never a
   * forced kill. Only the processes found by {@link #running()} are addressed, so a Claude
   * Code session with the same program name is not touched.
   */
  static void quit() throws IOException, InterruptedException {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("mac")) {
      new ProcessBuilder("osascript", "-e", "quit app id \"" + MAC_BUNDLE_ID + "\"")
          .start().waitFor();
      return;
    }
    for (ProcessHandle handle : running()) {
      if (os.contains("win")) {
        // No /F: a plain taskkill sends WM_CLOSE, giving the app the same chance to shut
        // down cleanly that closing its window would. By process id, never by image name.
        new ProcessBuilder("taskkill", "/PID", Long.toString(handle.pid())).start().waitFor();
      } else {
        // SIGTERM, not SIGKILL.
        handle.destroy();
      }
    }
  }

  /**
   * Looks up the Microsoft Store build of Claude Desktop among the installed packages.
   * A Store application is started by its application identity
   * ({@code <PackageFamilyName>!<ApplicationId>}), never by the path of its program, which
   * lives under {@code WindowsApps} and may not be started from there.
   */
  interface StoreLookup {
    /** The application identity of the installed Store build, or null when it is not
     *  installed. */
    String appUserModelId() throws IOException;
  }

  /** Asks Windows for the installed package named {@code Claude} and its application id. */
  static final StoreLookup WINDOWS_STORE = new StoreLookup() {
    @Override public String appUserModelId() throws IOException {
      Process process = new ProcessBuilder("powershell.exe", "-NoProfile", "-NonInteractive",
          "-Command",
          "$p = Get-AppxPackage -Name Claude | Select-Object -First 1; "
              + "if ($p) { $id = (Get-AppxPackageManifest $p).Package.Applications.Application"
              + " | Select-Object -First 1 -ExpandProperty Id; "
              + "Write-Output ($p.PackageFamilyName + '!' + $id) }")
          .redirectErrorStream(true).start();
      String output;
      try (java.io.InputStream in = process.getInputStream()) {
        output = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
      }
      try {
        process.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while looking up the Claude package", e);
      }
      return parseAppUserModelId(output);
    }
  };

  /**
   * The application identity in what the package lookup printed, or null when it printed
   * none. Only a line of the exact shape {@code Name_publisherid!ApplicationId} is accepted.
   */
  static String parseAppUserModelId(String lookupOutput) {
    if (lookupOutput == null) {
      return null;
    }
    for (String line : lookupOutput.split("\\R")) {
      String candidate = line.trim();
      if (candidate.matches("Claude_[a-z0-9]+![A-Za-z0-9._-]+")) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * The command that starts Claude Desktop again.
   *
   * @param os the value of {@code os.name}
   * @param program the program Claude Desktop was running from before it was asked to quit
   *     (see {@link #runningProgram()}), or null when it was not running
   * @param store the Store package lookup (Windows only)
   * @throws IOException if Claude Desktop cannot be identified; there is then no command, and
   *     nothing is started
   */
  static List<String> relaunchCommand(String os, Path program, StoreLookup store)
      throws IOException {
    String lower = os.toLowerCase(Locale.ROOT);
    List<String> command = new ArrayList<String>();
    if (lower.contains("mac")) {
      command.add("open");
      command.add("-b");
      command.add(MAC_BUNDLE_ID);
      return command;
    }
    if (lower.contains("win")) {
      boolean fromStore = program == null
          || program.toString().toLowerCase(Locale.ROOT).contains("\\windowsapps\\");
      if (fromStore) {
        String identity = store.appUserModelId();
        if (identity != null) {
          command.add("explorer.exe");
          command.add("shell:AppsFolder\\" + identity);
          return command;
        }
      }
    }
    if (program == null || !isDesktopProgram(program)) {
      throw new IOException("Claude Desktop was not found, so nothing was started");
    }
    command.add(program.toString());
    return command;
  }

  /**
   * Starts Claude Desktop again.
   *
   * @param program the program Claude Desktop was running from before it was asked to quit
   * @throws IOException if Claude Desktop cannot be identified; nothing is started then
   */
  static void relaunch(Path program) throws IOException {
    new ProcessBuilder(
        relaunchCommand(System.getProperty("os.name", ""), program, WINDOWS_STORE)).start();
  }
}
