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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The MCP server writes its answers and its log lines as UTF-8 on every platform. On Windows
 * the runtime's own standard streams, through a pipe, encode in the system code page; the
 * server's streams are layered over them and must not inherit that.
 */
@Tag("unit")
class McpServerOutputEncodingTest {

  /** An answer with characters outside ASCII and outside windows-1252. */
  private static final String ANSWER =
      "{\"instructions\":\"schemas — sec, geo; § 12; Española; 東京\"}";

  @Test void anAnswerIsWrittenAsUtf8OverAStreamInTheSystemCodePage() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // What System.out is on Windows when it is a pipe: a PrintStream in windows-1252.
    PrintStream systemOut = new PrintStream(bytes, true, "windows-1252");

    PrintStream mcpOut = McpServer.utf8(systemOut, false);
    mcpOut.print(ANSWER);
    mcpOut.flush();

    assertArrayEquals(ANSWER.getBytes(StandardCharsets.UTF_8), bytes.toByteArray());
  }

  @Test void aLogLineIsWrittenAsUtf8AndFlushedByTheLine() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream systemErr = new PrintStream(bytes, true, "windows-1252");

    PrintStream log = McpServer.utf8(systemErr, true);
    log.println("no server listening — attempting to spawn one");

    assertArrayEquals(
        ("no server listening — attempting to spawn one" + System.lineSeparator())
            .getBytes(StandardCharsets.UTF_8),
        bytes.toByteArray());
  }
}
