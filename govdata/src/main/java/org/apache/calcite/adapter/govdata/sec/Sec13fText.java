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
package org.apache.calcite.adapter.govdata.sec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalization of the free text in a 13F information table row ({@code issuer_name},
 * {@code title_of_class}).
 *
 * <p>Filers escape the XML text more than once, and the SEC data sets publish the text unparsed,
 * so a value can arrive with its ampersand escaped two or three times, or with the {@code &}
 * lost ({@code Samp;P500}). Entity references are decoded until the text stops changing, and a bare
 * {@code amp;} that follows an alphanumeric character is restored to {@code &}. Over the
 * distinct issuer and class names of the SEC data sets every such match is a damaged ampersand;
 * none is part of a word.
 */
final class Sec13fText {
  private static final Pattern ENTITY =
      Pattern.compile("&(amp|lt|gt|quot|apos|nbsp|#[0-9]+|#[xX][0-9a-fA-F]+);");
  private static final Pattern LOST_AMPERSAND = Pattern.compile("(?<=[A-Za-z0-9])amp;");
  private static final Pattern SAND_P = Pattern.compile("(?i)\\bSANDamp;P");

  private Sec13fText() {
  }

  static String normalize(String text) {
    if (text == null) {
      return null;
    }
    String current = text;
    while (true) {
      String next = decodeOnce(current);
      if (next.equals(current)) {
        break;
      }
      current = next;
    }
    current = SAND_P.matcher(current).replaceAll("S&P");
    return LOST_AMPERSAND.matcher(current).replaceAll("&");
  }

  private static String decodeOnce(String text) {
    Matcher m = ENTITY.matcher(text);
    StringBuffer out = new StringBuffer(text.length());
    while (m.find()) {
      m.appendReplacement(out, Matcher.quoteReplacement(decode(m.group(1))));
    }
    m.appendTail(out);
    return out.toString();
  }

  private static String decode(String name) {
    switch (name) {
    case "amp":
      return "&";
    case "lt":
      return "<";
    case "gt":
      return ">";
    case "quot":
      return "\"";
    case "apos":
      return "'";
    case "nbsp":
      return " ";
    default:
      int radix = name.charAt(1) == 'x' || name.charAt(1) == 'X' ? 16 : 10;
      int codePoint = Integer.parseInt(name.substring(radix == 16 ? 2 : 1), radix);
      return new String(Character.toChars(codePoint));
    }
  }
}
