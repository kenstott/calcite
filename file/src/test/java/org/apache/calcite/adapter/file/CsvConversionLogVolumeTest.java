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
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reading and converting a CSV file logs a bounded number of events at DEBUG and above,
 * not one or more per cell. Per-cell logging at DEBUG/INFO flooded the test output
 * (over a million lines in two minutes) whenever a schema scanned many fixture rows; on
 * the Windows CI runner the output pipeline could not keep up and the file tests wedged
 * until the job's time limit.
 */
@Tag("unit")
@Isolated("raises the file adapter's log levels to DEBUG for the whole JVM")
public class CsvConversionLogVolumeTest {

  private static final String ADAPTER_LOGGERS = "org.apache.calcite.adapter.file";

  @TempDir
  Path tempDir;

  @Test void testLogEventsDoNotGrowWithRowCount() throws Exception {
    Map<String, Integer> small = logEventsReading("small", 3);
    Map<String, Integer> large = logEventsReading("large", 300);
    int smallTotal = total(small);
    int largeTotal = total(large);
    // 297 more rows of 4 cells each: per-cell logging adds thousands of events.
    assertTrue(largeTotal - smallTotal < 50,
        "Reading 300 rows logged " + largeTotal + " events at DEBUG and above, 3 rows "
            + smallTotal + ": the adapter logs per row or per cell. Most frequent at 300 rows: "
            + mostFrequent(large));
  }

  private static int total(Map<String, Integer> events) {
    return events.values().stream().mapToInt(Integer::intValue).sum();
  }

  private static String mostFrequent(Map<String, Integer> events) {
    return events.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .limit(10)
        .map(e -> e.getValue() + " x " + e.getKey())
        .collect(Collectors.joining("\n  ", "\n  ", ""));
  }

  /**
   * Creates a schema over one CSV of {@code rows} rows, reads it, and counts log events by
   * logger and message pattern.
   */
  private Map<String, Integer> logEventsReading(String name, int rows) throws Exception {
    File dir = Files.createDirectory(tempDir.resolve(name)).toFile();
    StringBuilder csv = new StringBuilder("id,district,opened,status\n");
    for (int i = 0; i < rows; i++) {
      csv.append(i).append(",District ").append(i % 7)
          .append(",2026-08-").append(String.format("%02d", 1 + i % 28)).append("T04:47")
          .append(',').append(i % 2 == 0 ? "open" : "closed").append('\n');
    }
    Files.write(new File(dir, "complaints.csv").toPath(),
        csv.toString().getBytes(StandardCharsets.UTF_8));

    Map<String, Integer> count = new ConcurrentHashMap<>();
    Thread reader = Thread.currentThread();
    try (CapturedLogs ignored = new CapturedLogs(count, reader);
         Connection conn = DriverManager.getConnection("jdbc:calcite:", connectionProperties());
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", dir.getAbsolutePath());
      operand.put("baseDirectory", dir.getAbsolutePath());
      operand.put("ephemeralCache", true);
      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "logvol_" + name, operand);
      rootSchema.add("logvol_" + name, schema);
      int read = 0;
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("select * from logvol_" + name + ".complaints")) {
        while (rs.next()) {
          read++;
        }
      }
      assertEquals(rows, read);
    }
    return count;
  }

  private static Properties connectionProperties() {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return info;
  }

  /**
   * Counts the reading thread's events from the adapter's loggers at DEBUG and above, with
   * every adapter logger raised to DEBUG; restores the levels on close.
   */
  private static final class CapturedLogs implements AutoCloseable {
    private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    private final Map<LoggerConfig, Level> saved = new HashMap<>();
    private final AbstractAppender appender;

    CapturedLogs(Map<String, Integer> count, Thread reader) {
      appender = new AbstractAppender("log-volume", null, null, true, Property.EMPTY_ARRAY) {
        @Override public void append(LogEvent event) {
          if (event.getLoggerName().startsWith(ADAPTER_LOGGERS)
              && event.getThreadName().equals(reader.getName())) {
            count.merge(event.getLoggerName() + " " + event.getLevel(), 1, Integer::sum);
          }
        }
      };
      appender.start();
      Configuration config = context.getConfiguration();
      LoggerConfig adapter = config.getLoggerConfig(ADAPTER_LOGGERS);
      for (LoggerConfig lc : config.getLoggers().values()) {
        if (lc.getName().startsWith(ADAPTER_LOGGERS)) {
          saved.put(lc, lc.getLevel());
          lc.setLevel(Level.DEBUG);
          lc.addAppender(appender, Level.DEBUG, null);
        }
      }
      if (!adapter.getName().equals(ADAPTER_LOGGERS)) {
        throw new IllegalStateException("log4j2-test.xml configures no " + ADAPTER_LOGGERS
            + " logger; the capture would miss the adapter's events");
      }
      context.updateLoggers();
    }

    @Override public void close() {
      for (Map.Entry<LoggerConfig, Level> e : saved.entrySet()) {
        e.getKey().removeAppender(appender.getName());
        e.getKey().setLevel(e.getValue());
      }
      context.updateLoggers();
      appender.stop();
    }
  }
}
