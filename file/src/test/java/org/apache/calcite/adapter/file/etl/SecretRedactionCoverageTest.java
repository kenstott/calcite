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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards that the HTTP-facing classes never put a request URL or an exception message (which the
 * JDK builds from the URL) into a log line or a thrown message without {@link SecretRedaction}.
 */
@Tag("unit")
public class SecretRedactionCoverageTest {

  private static final String[] SOURCES = {
      "../src/main/java/org/apache/calcite/adapter/file/etl/HttpSource.java",
      "../src/main/java/org/apache/calcite/adapter/file/etl/DocumentSource.java",
      "../src/main/java/org/apache/calcite/adapter/file/etl/SchemaLifecycleProcessor.java",
      "../src/main/java/org/apache/calcite/adapter/file/storage/HttpStorageProvider.java",
  };

  private static final Pattern EMITS = Pattern.compile("LOGGER\\.|throw new ");
  private static final Pattern RAW_URL = Pattern.compile(
      "\\b(url|urlStr|urlString|fullUrl|resolverUrl|sliceUrl)\\b|\\.getMessage\\(\\)");

  @Test void fredTimeoutMessageIsRedacted() {
    String msg = "Timed out waiting for response headers from "
        + "https://api.stlouisfed.org/fred/series/observations?api_key=abcdef0123&file_type=json";
    String actual = SecretRedaction.redact(msg);
    assertTrue(actual.contains("api_key=<redacted>&file_type=json"), actual);
  }

  @Test void noEmitterPrintsUnredactedUrlOrMessage() throws IOException {
    List<String> offenders = new ArrayList<>();
    for (String source : SOURCES) {
      Path path = Paths.get(source);
      List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
      for (int i = 0; i < lines.size(); i++) {
        String line = lines.get(i);
        if (!EMITS.matcher(line).find() || line.contains("SecretRedaction.redact(")) {
          continue;
        }
        // an emitter statement can span lines; inspect up to its terminating semicolon
        StringBuilder stmt = new StringBuilder(line);
        for (int j = i; j < lines.size() && !lines.get(j).contains(";"); j++) {
          stmt.append(' ').append(lines.get(j + 1));
        }
        if (RAW_URL.matcher(stmt).find()
            && !stmt.toString().contains("SecretRedaction.redact(")
            && !stmt.toString().contains("baseUri(")) {
          offenders.add(path.getFileName() + ":" + (i + 1) + ": " + line.trim());
        }
      }
    }
    assertEquals(Arrays.<String>asList(), offenders,
        "log/exception sites must pass URLs and exception messages through SecretRedaction");
  }
}
