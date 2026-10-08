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
package org.apache.calcite.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Where the Splunk server the live tests talk to is, and how to sign in.
 *
 * <p>Read from {@code local-properties.settings}, a file git ignores (see
 * {@code local-properties.settings.sample}), or else from the {@code SPLUNK_URL},
 * {@code SPLUNK_USER} and {@code SPLUNK_PASSWORD} environment variables. No server and no
 * password is written in a test.
 *
 * <p>The live tests are tagged {@code integration} and run only when the build is given
 * {@code CALCITE_TEST_SPLUNK=true} or {@code -Dcalcite.test.splunk=true}. Asked for a
 * setting that is not there, this class fails: a live test that was asked to run and
 * cannot must not pass.
 */
public final class SplunkTestSettings {
  private static final String FILE = "local-properties.settings";

  private static Properties settings;

  private SplunkTestSettings() {
  }

  /** The management URL, such as {@code https://localhost:8089}. */
  public static String url() {
    return required("splunk.url", "SPLUNK_URL");
  }

  public static String user() {
    return required("splunk.username", "SPLUNK_USER");
  }

  public static String password() {
    return required("splunk.password", "SPLUNK_PASSWORD");
  }

  /** Whether the server's certificate is to go unchecked, as a container's must. */
  public static boolean sslInsecure() {
    String fromFile = load().getProperty("splunk.ssl.insecure");
    return Boolean.parseBoolean(
        fromFile != null ? fromFile : System.getenv("SPLUNK_SSL_INSECURE"));
  }

  /** Whether a server and a sign-in are configured. */
  public static boolean configured() {
    return value("splunk.url", "SPLUNK_URL") != null
        && value("splunk.username", "SPLUNK_USER") != null
        && value("splunk.password", "SPLUNK_PASSWORD") != null;
  }

  private static String required(String property, String variable) {
    String value = value(property, variable);
    if (value == null) {
      throw new IllegalStateException("A live Splunk test was asked to run without " + property
          + ": set it in splunk/" + FILE + " (copy " + FILE + ".sample) or set " + variable);
    }
    return value;
  }

  private static String value(String property, String variable) {
    String fromFile = load().getProperty(property);
    return fromFile != null ? fromFile : System.getenv(variable);
  }

  private static synchronized Properties load() {
    if (settings != null) {
      return settings;
    }
    Properties loaded = new Properties();
    // Gradle runs the tests from the module's directory; an IDE may run them from the root
    for (File file : new File[] {new File(FILE), new File("splunk", FILE)}) {
      if (!file.isFile()) {
        continue;
      }
      try (InputStream in = new FileInputStream(file)) {
        loaded.load(in);
      } catch (IOException e) {
        throw new IllegalStateException("Reading " + file.getAbsolutePath() + " failed: "
            + e.getMessage(), e);
      }
      break;
    }
    settings = loaded;
    return settings;
  }
}
