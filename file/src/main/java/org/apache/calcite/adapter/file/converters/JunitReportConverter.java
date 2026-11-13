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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Reads the JUnit XML test reports of a schema (what pytest {@code --junitxml}, vitest,
 * Playwright's junit reporter and Maven surefire write) as three tables, combining every report
 * the schema finds:
 *
 * <ul>
 *   <li>{@value #RUNS_TABLE}: one row per report file;</li>
 *   <li>{@value #CASES_TABLE}: one row per distinct test case (classname and name);</li>
 *   <li>{@value #RESULTS_TABLE}: one row per test case per report.</li>
 * </ul>
 *
 * <p>A file is a JUnit report when its root element is {@code testsuites} or
 * {@code testsuite}; any other XML file is read by {@link XmlToJsonConverter}. A file whose root
 * names it a JUnit report but that cannot be read as one fails, naming the file.
 */
public final class JunitReportConverter {
  /** The table of report files. */
  public static final String RUNS_TABLE = "junit__runs";
  /** The table of distinct test cases. */
  public static final String CASES_TABLE = "junit__cases";
  /** The table of each test case's result in each report. */
  public static final String RESULTS_TABLE = "junit__results";

  private static final String CONVERSION_TYPE = "JUNIT_TO_JSON";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
  private static final String[] OUTCOME_ELEMENTS = {"failure", "error", "skipped"};

  private JunitReportConverter() {
  }

  /** A report file, with the key of its run: its path under the schema's directory. */
  public static final class Report {
    final File file;
    final String runKey;
    final String filePath;

    /**
     * @param file The report as read locally
     * @param runKey The report's path under the schema's directory, with {@code /} separators
     * @param filePath The report's location as the schema names it (a local path or a URI)
     */
    public Report(File file, String runKey, String filePath) {
      this.file = file;
      this.runKey = runKey;
      this.filePath = filePath;
    }

    /** The report as read locally. */
    public File file() {
      return file;
    }
  }

  /**
   * Whether {@code file} is a JUnit report: its root element is {@code testsuites} or
   * {@code testsuite}. A file that is not well-formed before its root element is not one.
   *
   * @throws IOException if the file cannot be read
   */
  public static boolean isJunitReport(File file) throws IOException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    try (InputStream in = Sources.of(file).openStream()) {
      XMLStreamReader reader = factory.createXMLStreamReader(in);
      try {
        while (reader.hasNext()) {
          if (reader.next() == XMLStreamConstants.START_ELEMENT) {
            String root = reader.getLocalName();
            return "testsuites".equals(root) || "testsuite".equals(root);
          }
        }
        return false;
      } finally {
        reader.close();
      }
    } catch (XMLStreamException e) {
      // Not XML up to its root element: not a JUnit report, read as any other XML file.
      return false;
    }
  }

  /**
   * Writes the three tables of {@code reports} as JSON files in {@code outputDir}, named
   * {@code <table>.json}, and records each conversion against the first report by run key.
   *
   * @param reports The schema's JUnit reports; not empty
   * @param outputDir Where the JSON files are written
   * @param baseDirectory Where conversion metadata is kept; null for beside the output
   * @return The JSON files written: runs, cases, results
   * @throws IOException if a report is malformed (the message names it) or a file cannot be
   *     written
   */
  public static List<File> convert(List<Report> reports, File outputDir,
      @Nullable File baseDirectory) throws IOException {
    if (reports.isEmpty()) {
      throw new IllegalArgumentException("No JUnit reports to convert");
    }
    List<Report> sorted = new ArrayList<>(reports);
    sorted.sort(Comparator.comparing(r -> r.runKey));

    ArrayNode runs = MAPPER.createArrayNode();
    Map<String, ObjectNode> cases = new LinkedHashMap<>();
    ArrayNode results = MAPPER.createArrayNode();
    for (Report report : sorted) {
      try {
        readReport(report, runs, cases, results);
      } catch (MalformedReportException e) {
        throw new IOException("Malformed JUnit report " + report.filePath + ": "
            + e.getMessage(), e);
      }
    }

    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IOException("Cannot create directory " + outputDir);
    }
    ArrayNode caseRows = MAPPER.createArrayNode();
    cases.values().forEach(caseRows::add);
    List<File> written = new ArrayList<>();
    written.add(write(runs, new File(outputDir, RUNS_TABLE + ".json")));
    written.add(write(caseRows, new File(outputDir, CASES_TABLE + ".json")));
    written.add(write(results, new File(outputDir, RESULTS_TABLE + ".json")));
    for (File json : written) {
      ConversionRecorder.recordConversion(sorted.get(0).file, json, CONVERSION_TYPE,
          baseDirectory);
    }
    return written;
  }

  private static File write(ArrayNode rows, File json) throws IOException {
    try (Writer writer =
             new OutputStreamWriter(new FileOutputStream(json), StandardCharsets.UTF_8)) {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(writer, rows);
    }
    return json;
  }

  private static void readReport(Report report, ArrayNode runs, Map<String, ObjectNode> cases,
      ArrayNode results) throws IOException, MalformedReportException {
    Document document;
    try {
      document = XmlTableScanner.parseXmlDocument(Sources.of(report.file));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new MalformedReportException(e.getMessage(), e);
    }
    Element root = document.getDocumentElement();
    List<Element> suites = "testsuite".equals(localName(root))
        ? java.util.Collections.singletonList(root)
        : childElements(root, "testsuite");

    ObjectNode run = runs.addObject();
    run.put("run_key", report.runKey);
    run.put("file_path", report.filePath);
    run.put("name", attribute(root, "name"));
    run.put("run_timestamp", timestamp(first(root, suites, "timestamp")));
    run.put("hostname", first(root, suites, "hostname"));
    putCount(run, "tests", root, suites);
    putCount(run, "failures", root, suites);
    putCount(run, "errors", root, suites);
    putCount(run, "skipped", root, suites);
    putTotalSeconds(run, root, suites);
    run.put("properties", properties(root, suites));

    NodeList testcases = root.getElementsByTagNameNS("*", "testcase");
    for (int i = 0; i < testcases.getLength(); i++) {
      Element testcase = (Element) testcases.item(i);
      String name = attribute(testcase, "name");
      if (name == null) {
        throw new MalformedReportException("a testcase has no name attribute");
      }
      String classname = attribute(testcase, "classname");
      String caseKey = classname == null ? name : classname + "::" + name;
      if (!cases.containsKey(caseKey)) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("case_key", caseKey);
        row.put("classname", classname);
        row.put("name", name);
        row.put("file", attribute(testcase, "file"));
        String line = attribute(testcase, "line");
        if (line == null) {
          row.putNull("line");
        } else {
          row.put("line", parseInt(line, "line"));
        }
        cases.put(caseKey, row);
      }

      ObjectNode result = results.addObject();
      result.put("run_key", report.runKey);
      result.put("case_key", caseKey);
      Element outcome = outcomeElement(testcase);
      result.put("outcome", outcome == null ? "passed" : outcomeName(outcome));
      putSeconds(result, "duration_seconds", attribute(testcase, "time"));
      result.put("message", outcome == null ? null : attribute(outcome, "message"));
      result.put("type", outcome == null ? null : attribute(outcome, "type"));
      result.put("text", outcome == null ? null : text(outcome));
    }
  }

  /** The testcase's first failure, error or skipped child, in document order; null if none. */
  private static @Nullable Element outcomeElement(Element testcase) {
    for (Node child = testcase.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element) {
        String name = localName((Element) child);
        for (String outcome : OUTCOME_ELEMENTS) {
          if (outcome.equals(name)) {
            return (Element) child;
          }
        }
      }
    }
    return null;
  }

  private static String outcomeName(Element outcome) {
    switch (localName(outcome)) {
    case "failure":
      return "failed";
    case "error":
      return "error";
    case "skipped":
      return "skipped";
    default:
      throw new IllegalStateException("Not an outcome element: " + localName(outcome));
    }
  }

  private static @Nullable String text(Element element) {
    String text = element.getTextContent();
    return text == null || text.trim().isEmpty() ? null : text.trim();
  }

  /**
   * The run-level count: the root's attribute, else the sum over the root's testsuites that
   * give it; null when none does.
   */
  private static void putCount(ObjectNode run, String attribute, Element root,
      List<Element> suites) throws MalformedReportException {
    String own = attribute(root, attribute);
    if (own != null) {
      run.put(attribute, parseInt(own, attribute));
      return;
    }
    Integer sum = null;
    for (Element suite : suites) {
      String value = attribute(suite, attribute);
      if (value != null) {
        sum = (sum == null ? 0 : sum) + parseInt(value, attribute);
      }
    }
    if (sum == null) {
      run.putNull(attribute);
    } else {
      run.put(attribute, sum.intValue());
    }
  }

  /** The root's attribute, else the first of its testsuites' that gives it; null if none. */
  private static @Nullable String first(Element root, List<Element> suites, String attribute) {
    String own = attribute(root, attribute);
    if (own != null) {
      return own;
    }
    for (Element suite : suites) {
      String value = attribute(suite, attribute);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  /**
   * The properties the root and its testsuites give ({@code <properties><property name=
   * value=/>}), as a JSON object; a property's value is its {@code value} attribute, else its
   * text.
   */
  private static String properties(Element root, List<Element> suites)
      throws IOException, MalformedReportException {
    ObjectNode properties = MAPPER.createObjectNode();
    List<Element> owners = new ArrayList<>();
    if (!suites.contains(root)) {
      owners.add(root);
    }
    owners.addAll(suites);
    for (Element owner : owners) {
      for (Element block : childElements(owner, "properties")) {
        for (Element property : childElements(block, "property")) {
          String name = attribute(property, "name");
          if (name == null) {
            throw new MalformedReportException("a property has no name attribute");
          }
          String value = attribute(property, "value");
          properties.put(name, value != null ? value : text(property));
        }
      }
    }
    return MAPPER.writeValueAsString(properties);
  }

  /**
   * A report's timestamp as an ISO local date-time: one with an offset is taken to UTC; one
   * without is kept as written.
   */
  private static @Nullable String timestamp(@Nullable String value)
      throws MalformedReportException {
    if (value == null) {
      return null;
    }
    try {
      LocalDateTime local;
      try {
        local = OffsetDateTime.parse(value).withOffsetSameInstant(ZoneOffset.UTC)
            .toLocalDateTime();
      } catch (DateTimeParseException notOffset) {
        local = LocalDateTime.parse(value);
      }
      return local.format(TIMESTAMP_FORMAT);
    } catch (DateTimeParseException e) {
      throw new MalformedReportException("timestamp '" + value + "' is not an ISO date-time", e);
    }
  }

  /**
   * The run's time: the root's {@code time}, else the sum over the root's testsuites that give
   * it; null when none does.
   */
  private static void putTotalSeconds(ObjectNode run, Element root, List<Element> suites)
      throws MalformedReportException {
    String own = attribute(root, "time");
    if (own != null) {
      run.put("time_seconds", seconds(own));
      return;
    }
    Double sum = null;
    for (Element suite : suites) {
      String value = attribute(suite, "time");
      if (value != null) {
        sum = (sum == null ? 0d : sum) + seconds(value);
      }
    }
    if (sum == null) {
      run.putNull("time_seconds");
    } else {
      run.put("time_seconds", sum.doubleValue());
    }
  }

  private static void putSeconds(ObjectNode row, String column, @Nullable String value)
      throws MalformedReportException {
    if (value == null) {
      row.putNull(column);
      return;
    }
    row.put(column, seconds(value));
  }

  private static double seconds(String value) throws MalformedReportException {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new MalformedReportException("time '" + value + "' is not a number", e);
    }
  }

  private static int parseInt(String value, String attribute) throws MalformedReportException {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new MalformedReportException(attribute + " '" + value + "' is not an integer", e);
    }
  }

  private static @Nullable String attribute(Element element, String name) {
    return element.hasAttribute(name) ? element.getAttribute(name) : null;
  }

  private static String localName(Element element) {
    return element.getLocalName() != null ? element.getLocalName() : element.getTagName();
  }

  private static List<Element> childElements(Element parent, String name) {
    List<Element> children = new ArrayList<>();
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element && name.equals(localName((Element) child))) {
        children.add((Element) child);
      }
    }
    return children;
  }

  /** A report whose root names it a JUnit report but that cannot be read as one. */
  private static final class MalformedReportException extends Exception {
    MalformedReportException(String message) {
      super(message);
    }

    MalformedReportException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
