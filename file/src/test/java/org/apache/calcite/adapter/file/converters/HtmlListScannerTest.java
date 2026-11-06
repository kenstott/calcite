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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The table shapes {@link HtmlListScanner} reads from a page's lists (REQ-2186).
 */
@Tag("unit")
public class HtmlListScannerTest {

  private static final String PAGE = "https://example.test/wiki/Page";

  private static Element first(String html, String selector) {
    Document doc = Jsoup.parse("<html><body>" + html + "</body></html>", PAGE);
    return doc.selectFirst(selector);
  }

  private static List<String> columns(Map<String, Object> row) {
    return new ArrayList<>(row.keySet());
  }

  @Test void aPlainListHasValueAndLevel() {
    List<Map<String, Object>> rows = HtmlListScanner.rows(
        first("<ul><li>Red</li><li> Green\n light </li><li>Blue</li></ul>", "ul"));
    assertEquals(3, rows.size());
    assertEquals(Arrays.asList("value", "level"), columns(rows.get(0)));
    assertEquals("Red", rows.get(0).get("value"));
    assertEquals("Green light", rows.get(1).get("value"));
    assertEquals(1, rows.get(2).get("level"));
  }

  @Test void aLinkListHasAnAbsoluteUriAndAnEmptyOneForAnItemWithoutALink() {
    List<Map<String, Object>> rows = HtmlListScanner.rows(first("<ul>"
        + "<li><a href='/wiki/France'>France</a></li>"
        + "<li>Germany (no page)</li>"
        + "<li><a href='Italy'>Italy</a> and more</li>"
        + "<li><a href='https://other.test/x'>Elsewhere</a></li>"
        + "</ul>", "ul"));
    assertEquals(Arrays.asList("value", "uri", "level"), columns(rows.get(0)));
    assertEquals("France", rows.get(0).get("value"));
    assertEquals("https://example.test/wiki/France", rows.get(0).get("uri"));
    assertEquals("", rows.get(1).get("uri"));
    assertEquals("Italy and more", rows.get(2).get("value"));
    assertEquals("https://example.test/wiki/Italy", rows.get(2).get("uri"));
    assertEquals("https://other.test/x", rows.get(3).get("uri"));
  }

  @Test void aNumberedListAddsPosition() {
    List<Map<String, Object>> rows = HtmlListScanner.rows(
        first("<ol><li>First</li><li>Second</li><li>Third</li></ol>", "ol"));
    assertEquals(Arrays.asList("value", "level", "position"), columns(rows.get(0)));
    assertEquals(1, rows.get(0).get("position"));
    assertEquals(3, rows.get(2).get("position"));
  }

  @Test void aNestedListIsFlattenedInDocumentOrderWithItsLevelAndPosition() {
    Element list = first("<ol>"
        + "<li>Europe<ol><li>France</li><li>Spain<ul><li>Madrid</li></ul></li></ol></li>"
        + "<li>Asia<ol><li>Japan</li></ol></li>"
        + "<li>Africa</li>"
        + "</ol>", "ol");
    List<Map<String, Object>> rows = HtmlListScanner.rows(list);
    List<String> values = new ArrayList<>();
    List<Object> levels = new ArrayList<>();
    List<Object> positions = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      values.add((String) row.get("value"));
      levels.add(row.get("level"));
      positions.add(row.get("position"));
    }
    // An item's value is its own text, not the text of the lists nested in it.
    assertEquals(
        Arrays.asList("Europe", "France", "Spain", "Madrid", "Asia", "Japan", "Africa"), values);
    assertEquals(Arrays.<Object>asList(1, 2, 2, 3, 1, 2, 1), levels);
    // Each item's number within its own list.
    assertEquals(Arrays.<Object>asList(1, 1, 2, 1, 2, 1, 3), positions);
  }

  @Test void aLinkInANestedListIsItsItemsNotTheOuterItems() {
    List<Map<String, Object>> rows = HtmlListScanner.rows(first("<ul>"
        + "<li>Group<ul><li><a href='/a'>A</a></li></ul></li>"
        + "<li>Two</li><li>Three</li></ul>", "ul"));
    assertEquals("", rows.get(0).get("uri"));
    assertEquals("https://example.test/a", rows.get(1).get("uri"));
    assertEquals(2, rows.get(1).get("level"));
  }

  @Test void aListOfFewerThanThreeItemsOrANestedListIsNoTable() {
    Document doc = Jsoup.parse("<html><body><div id='c'>"
        + "<ul id='short'><li>One</li><li>Two</li></ul>"
        + "<ul id='outer'><li>A<ul id='inner'><li>x</li><li>y</li><li>z</li></ul></li>"
        + "<li>B</li><li>C</li></ul>"
        + "</div></body></html>", PAGE);
    Element content = doc.getElementById("c");
    assertFalse(HtmlListScanner.isTable(doc.getElementById("short"), content));
    assertTrue(HtmlListScanner.isTable(doc.getElementById("outer"), content));
    assertFalse(HtmlListScanner.isTable(doc.getElementById("inner"), content));
  }

  @Test void aListInANavigationHeaderFooterOrAsideIsNeverATable() {
    for (String landmark : Arrays.asList("nav", "header", "footer", "aside")) {
      Document doc = Jsoup.parse("<html><body><div id='c'><h2>Links</h2>"
          + "<" + landmark + "><div><ul id='l'><li>a</li><li>b</li><li>c</li></ul></div></"
          + landmark + ">"
          + "</div></body></html>", PAGE);
      Element content = doc.getElementById("c");
      assertFalse(HtmlListScanner.isTable(doc.getElementById("l"), content), landmark);
      assertTrue(HtmlListScanner.scan(content).isEmpty(), landmark);
    }
  }

  @Test void aLandmarkHoldingTheWholeContentStillHoldsNoTable() {
    // Whatever the content selector picks, a list inside a landmark is not content.
    Document doc = Jsoup.parse("<html><body><nav><div id='c'>"
        + "<ul id='l'><li>a</li><li>b</li><li>c</li></ul></div></nav></body></html>", PAGE);
    assertFalse(
        HtmlListScanner.isTable(doc.getElementById("l"), doc.getElementById("c")));
  }

  @Test void scanNamesEachListForTheHeadingAboveItInDocumentOrder() {
    Document doc = Jsoup.parse("<html><body><div id='c'>"
        + "<ul><li>1</li><li>2</li><li>3</li></ul>"
        + "<h2>Member states</h2><p>text</p>"
        + "<ol><li>a</li><li>b</li><li>c</li></ol>"
        + "<h3>!!</h3>"
        + "<ul><li>x</li><li>y</li><li>z</li></ul>"
        + "</div></body></html>", PAGE);
    List<String> names = new ArrayList<>();
    for (HtmlListScanner.ListTable table : HtmlListScanner.scan(doc.getElementById("c"))) {
      names.add(table.name);
    }
    // A heading that leaves nothing in a name names no list.
    assertEquals(Arrays.asList("list_1", "member_states", "list_2"), names);
  }
}
