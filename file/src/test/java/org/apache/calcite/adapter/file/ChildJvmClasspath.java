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
package org.apache.calcite.adapter.file;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Hands this JVM's class path to a child JVM a test starts.
 *
 * <p>The class path of a test run is longer than the 32,767 characters Windows allows a command
 * line, so it goes in an argument file ({@code java @file}) instead of after {@code -cp}.
 */
public final class ChildJvmClasspath {
  private ChildJvmClasspath() {
  }

  /** The single {@code @file} argument that gives a child JVM this JVM's class path. */
  public static String argument() throws IOException {
    Path file = Files.createTempFile("child-jvm-classpath", ".args");
    file.toFile().deleteOnExit();
    // Inside the quotes of an argument file a backslash escapes the next character
    String classpath = System.getProperty("java.class.path").replace("\\", "\\\\");
    Files.write(file, ("-cp \"" + classpath + "\"").getBytes(StandardCharsets.UTF_8));
    return "@" + file;
  }
}
