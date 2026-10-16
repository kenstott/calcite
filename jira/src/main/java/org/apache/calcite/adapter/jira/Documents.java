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
package org.apache.calcite.adapter.jira;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Atlassian Document Format, the rich text of Jira Cloud's REST v3, to and from plain text.
 *
 * <p>Reading keeps the words: each block (paragraph, heading, list item, code block, ...) is a
 * line, a hard break is a line break, a mention reads as its text, an emoji as its short name and
 * a link card as its URL. Writing makes one paragraph per line.
 */
final class Documents {

  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private Documents() {}

  static String toText(JsonNode document) {
    final StringBuilder text = new StringBuilder();
    append(document, text);
    // Each block ends its line; the last one needs no line after it
    int end = text.length();
    while (end > 0 && text.charAt(end - 1) == '\n') {
      end--;
    }
    return text.substring(0, end);
  }

  private static void append(JsonNode node, StringBuilder text) {
    switch (node.path("type").asText("")) {
    case "text":
      text.append(node.path("text").asText(""));
      return;
    case "hardBreak":
      text.append('\n');
      return;
    case "mention":
      text.append(node.path("attrs").path("text").asText(""));
      return;
    case "emoji":
      text.append(node.path("attrs").path("shortName").asText(""));
      return;
    case "inlineCard":
    case "blockCard":
      text.append(node.path("attrs").path("url").asText(""));
      return;
    default:
      break;
    }
    final boolean block = !node.path("type").asText("").equals("doc")
        && node.has("content") && isBlock(node.path("type").asText(""));
    for (JsonNode child : node.path("content")) {
      append(child, text);
    }
    if (block && (text.length() == 0 || text.charAt(text.length() - 1) != '\n')) {
      text.append('\n');
    }
  }

  private static boolean isBlock(String type) {
    switch (type) {
    case "paragraph":
    case "heading":
    case "codeBlock":
    case "blockquote":
    case "panel":
    case "tableCell":
    case "tableHeader":
    case "rule":
      return true;
    default:
      return false;
    }
  }

  static ObjectNode fromText(String text) {
    final ObjectNode document = NODES.objectNode();
    document.put("type", "doc");
    document.put("version", 1);
    final ArrayNode content = document.putArray("content");
    for (String line : text.split("\n", -1)) {
      final ObjectNode paragraph = content.addObject();
      paragraph.put("type", "paragraph");
      final ArrayNode inline = paragraph.putArray("content");
      if (!line.isEmpty()) {
        inline.addObject().put("type", "text").put("text", line);
      }
    }
    return document;
  }
}
