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

import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the lists of an HTML page's content ({@code ul} and {@code ol}) as tables.
 *
 * <p>A list is read when it is not nested in another list of the content and has at least
 * {@link #MIN_ITEMS} items of its own. A nested list is flattened into its outer list's table:
 * its items are rows of that table, in document order. Every row has a {@code level}, 1 for an
 * item of the outer list and one more for each list it is nested in. A list in which any item is
 * a link also has a {@code uri}: the item's link made absolute against the page's address, empty
 * for an item with no link. A numbered list ({@code ol}) also has a {@code position}: the item's
 * number within its own list, from 1.
 */
public final class HtmlListScanner {
  /** The fewest items of its own a list has to be read as a table. */
  public static final int MIN_ITEMS = 3;

  /** The elements that are lists. */
  public static final String LIST_TAGS = "ul, ol";

  private HtmlListScanner() {
  }

  /** Whether {@code element} is a list. */
  public static boolean isList(Element element) {
    return "ul".equals(element.tagName()) || "ol".equals(element.tagName());
  }

  /**
   * Whether {@code list}, a list inside {@code content}, is read as a table: it is in no other
   * list of the content and has at least {@link #MIN_ITEMS} items.
   */
  public static boolean isTable(Element list, Element content) {
    for (Element parent = list.parent(); parent != null && parent != content;
        parent = parent.parent()) {
      if (isList(parent)) {
        return false;
      }
    }
    return items(list).size() >= MIN_ITEMS;
  }

  /**
   * The rows of {@code list}'s table, with its nested lists' items in document order. The
   * columns are {@code value}, then {@code uri} when any item is a link, then {@code level},
   * then {@code position} when the list is numbered.
   */
  public static List<Map<String, Object>> rows(Element list) {
    List<Item> items = new ArrayList<>();
    flatten(list, 1, items);
    boolean links = false;
    for (Item item : items) {
      links |= item.link != null;
    }
    boolean numbered = "ol".equals(list.tagName());
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Item item : items) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("value", item.value);
      if (links) {
        row.put("uri", item.link == null ? "" : item.link);
      }
      row.put("level", item.level);
      if (numbered) {
        row.put("position", item.position);
      }
      rows.add(row);
    }
    return rows;
  }

  /** One item of a list or of a list nested in it. */
  private static final class Item {
    final String value;
    final String link;
    final int level;
    final int position;

    Item(String value, String link, int level, int position) {
      this.value = value;
      this.link = link;
      this.level = level;
      this.position = position;
    }
  }

  private static void flatten(Element list, int level, List<Item> into) {
    int position = 0;
    for (Element item : items(list)) {
      into.add(new Item(ownText(item), ownLink(item), level, ++position));
      for (Element nested : item.select(LIST_TAGS)) {
        if (nested.parent() != null && nearestList(nested.parent()) == list) {
          flatten(nested, level + 1, into);
        }
      }
    }
  }

  /** The list's own items: its {@code li} children. */
  private static List<Element> items(Element list) {
    List<Element> items = new ArrayList<>();
    for (Element child : list.children()) {
      if ("li".equals(child.tagName())) {
        items.add(child);
      }
    }
    return items;
  }

  /** The nearest list that holds {@code element}, or {@code element} itself when it is one. */
  private static Element nearestList(Element element) {
    return element.closest(LIST_TAGS);
  }

  /** The item's text, without the text of the lists nested in it. */
  private static String ownText(Element item) {
    Element clone = item.clone();
    clone.select(LIST_TAGS).remove();
    return clone.text().replaceAll("\\s+", " ").trim();
  }

  /**
   * The absolute address of the item's first link outside the lists nested in it; null when it
   * has none.
   */
  private static String ownLink(Element item) {
    Element list = item.parent();
    for (Element link : item.select("a[href]")) {
      if (nearestList(link) == list) {
        String uri = link.absUrl("href");
        return uri.isEmpty() ? null : uri;
      }
    }
    return null;
  }
}
