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

import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import org.jsoup.select.Elements;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import static org.apache.calcite.util.TestUtil.getJavaMajorVersion;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;

/**
 * Unit tests for FileReader.
 */
@Tag("unit")
@ExtendWith(RequiresNetworkExtension.class)
@SuppressWarnings("deprecation")
class FileReaderTest {

  /**
   * These pages are served by a local HTTP server started in {@link #startFixtureServer()} rather
   * than fetched from en.wikipedia.org. The tests own their input: a page that moves, an offline
   * build or a corporate proxy used to fail them, which is indistinguishable from a real parser
   * regression. The fixtures under {@code /wiki-*.html} mirror the structure the tests rely on —
   * a {@code #mw-content-text} container and a sortable wikitable — so the HTTP fetch and Jsoup
   * parse paths are still exercised end to end.
   */
  private static HttpServer fixtureServer;
  private static Source citiesSource;
  private static Source statesSource;

  @BeforeAll static void startFixtureServer() throws IOException {
    fixtureServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 16);
    fixtureServer.createContext("/cities", serve("/wiki-cities.html"));
    fixtureServer.createContext("/states", serve("/wiki-states.html"));
    fixtureServer.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
    fixtureServer.start();
    String base = "http://127.0.0.1:" + fixtureServer.getAddress().getPort();
    citiesSource = Sources.url(base + "/cities");
    statesSource = Sources.url(base + "/states");
  }

  @AfterAll static void stopFixtureServer() {
    if (fixtureServer != null) {
      fixtureServer.stop(0);
    }
  }

  private static HttpHandler serve(String resource) {
    return exchange -> {
      byte[] body;
      try (InputStream in =
               requireNonNull(FileReaderTest.class.getResourceAsStream(resource), resource)) {
        body = readAll(in);
      }
      exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    };
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[8192];
    int n;
    while ((n = in.read(chunk)) != -1) {
      buf.write(chunk, 0, n);
    }
    return buf.toByteArray();
  }

  private static Source resource(String path) {
    final URL url =
        requireNonNull(FileReaderTest.class.getResource("/" + path), "url");
    return Sources.of(url);
  }

  private static String resourcePath(String path) {
    return resource(path).file().getAbsolutePath();
  }

  /** Tests {@link FileReader} URL instantiation - no path. */
  @Test public void testFileReaderUrlNoPath() throws FileReaderException {
    // No TLS assumption needed: the fixture server is plain HTTP on loopback, so the JDK
    // root-certificate caveat that used to gate this test against https://en.wikipedia.org
    // no longer applies.
    FileReader t = new FileReader(statesSource);
    t.refresh();
  }

  /** Tests {@link FileReader} URL instantiation - with path. */
  @Test public void testFileReaderUrlWithPath() throws FileReaderException {
    FileReader t =
        new FileReader(citiesSource,
            "#mw-content-text table.wikitable.sortable", 0);
    t.refresh();
  }

  /** Tests {@link FileReader} URL fetch. */
  @Test public void testFileReaderUrlFetch() throws FileReaderException {
    FileReader t =
        new FileReader(statesSource,
            "#mw-content-text table.wikitable.sortable", 0);
    int i = 0;
    for (Elements row : t) {
      i++;
    }
    assertThat(i, is(51));
  }

  /** Tests failed {@link FileReader} instantiation - malformed URL. */
  @Test void testFileReaderMalUrl() {
    try {
      final Source badSource = Sources.url("bad" + citiesSource.url());
      fail("expected exception, got " + badSource);
    } catch (RuntimeException e) {
      assertThat(e.getCause(), instanceOf(MalformedURLException.class));
      assertThat(e.getCause().getMessage(), is("unknown protocol: badhttp"));
    }
  }

  /** Tests failed {@link FileReader} instantiation - bad URL. */
  @Test void testFileReaderBadUrl() {
    final String uri =
        "http://ex.wikipedia.org/wiki/List_of_United_States_cities_by_population";
    assertThrows(FileReaderException.class, () -> {
      FileReader t = new FileReader(Sources.url(uri), "table:eq(4)");
      t.refresh();
    });
  }

  /** Tests failed {@link FileReader} instantiation - bad selector. */
  @Test void testFileReaderBadSelector() {
    final Source source = resource("tableOK.html");
    assertThrows(FileReaderException.class, () -> {
      FileReader t = new FileReader(source, "table:eq(1)");
      t.refresh();
    });
  }

  /** Test {@link FileReader} with static file - headings. */
  @Test void testFileReaderHeadings() throws FileReaderException {
    final Source source = resource("tableOK.html");
    FileReader t = new FileReader(source);
    Elements headings = t.getHeadings();
    assertThat(headings.get(1).text(), is("H1"));
  }

  /** Test {@link FileReader} with static file - data. */
  @Test void testFileReaderData() throws FileReaderException {
    final Source source = resource("tableOK.html");
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertThat(row.get(2).text(), is("R0C2"));
    row = i.next();
    assertThat(row.get(0).text(), is("R1C0"));
  }

  /** Tests {@link FileReader} with bad static file - headings. */
  @Test void testFileReaderHeadingsBadFile() throws FileReaderException {
    final Source source = resource("tableNoTheadTbody.html");
    FileReader t = new FileReader(source);
    Elements headings = t.getHeadings();
    assertThat(headings.get(1).text(), is("H1"));
  }

  /** Tests {@link FileReader} with bad static file - data. */
  @Test void testFileReaderDataBadFile() throws FileReaderException {
    final Source source = resource("tableNoTheadTbody.html");
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertThat(row.get(2).text(), is("R0C2"));
    row = i.next();
    assertThat(row.get(0).text(), is("R1C0"));
  }

  /** Tests {@link FileReader} with no headings static file - data. */
  @Test void testFileReaderDataNoTh() throws FileReaderException {
    final Source source = resource("tableNoTH.html");
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertThat(row.get(2).text(), is("R0C2"));
  }

  /** Tests {@link FileReader} iterator with a static file. */
  @Test void testFileReaderIterator() throws FileReaderException {
    final Source source = resource("tableOK.html");
    FileReader t = new FileReader(source);
    Elements row = null;
    for (Elements aT : t) {
      row = aT;
    }
    assertNotNull(row);
    assertThat(row.get(1).text(), is("R2C1"));
  }

  /** Tests reading a CSV file via the file adapter. Based on the test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1952">[CALCITE-1952]
   * NPE in planner</a>. */
  @Test void testCsvFile() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-csv");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"files\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + ",\n"
        + "        \"ephemeralCache\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select * from \"files\".\"depts\"";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("10"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("20"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("30"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }

  /**
   * Tests reading a JSON file via the file adapter.
   */
  @Test void testJsonFile() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-json");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"files\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + ",\n"
        + "        \"ephemeralCache\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select * from \"files\".\"depts\"";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("10"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("20"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("30"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }

  /**
   * Tests reading two JSON file with join via the file adapter.
   */
  @Test void testJsonFileWithJoin() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-json");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"files\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + ",\n"
        + "        \"ephemeralCache\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select a.\"empno\",a.\"name\",a.\"city\",b.\"deptno\" "
          + "from \"files\".\"emps\" a, \"files\".\"depts\" b where a.\"deptno\" = b.\"deptno\"";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("100"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("110"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("120"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }
}
