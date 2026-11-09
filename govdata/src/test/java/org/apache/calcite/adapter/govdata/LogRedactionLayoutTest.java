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
package org.apache.calcite.adapter.govdata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.SimpleMessage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The shipped log4j2.xml must strip credentials from messages and from stack traces. */
@Tag("unit")
public class LogRedactionLayoutTest {

  private static String render(String message, Throwable thrown) throws Exception {
    // the test classpath carries its own log4j2.xml, so read the shipped one by path
    // (the test working directory is the module's build directory)
    try (InputStream in = Files.newInputStream(Paths.get("../src/main/resources/log4j2.xml"))) {
      Element layout = (Element) DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(in).getElementsByTagName("PatternLayout").item(0);
      PatternLayout pl = PatternLayout.newBuilder()
          .withPattern(layout.getAttribute("pattern")).build();
      Log4jLogEvent event = Log4jLogEvent.newBuilder().setLoggerName("t")
          .setLevel(Level.WARN).setMessage(new SimpleMessage(message)).setThrown(thrown).build();
      return pl.toSerializable(event);
    }
  }

  @Test void redactsFredKeyInMessageAndStackTrace() throws Exception {
    String url = "https://api.stlouisfed.org/fred/series/observations?api_key=SECRETKEY123"
        + "&file_type=json&series_id=CPIAUCNS";
    String out = render("Request failed: " + url,
        new SocketTimeoutException("Timed out waiting for response headers from " + url));
    assertFalse(out.contains("SECRETKEY123"), out);
    assertTrue(out.contains("api_key=<redacted>&file_type=json&series_id=CPIAUCNS"), out);
  }
}
