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
package org.apache.calcite.adapter.file.util;

import java.io.File;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Matches a relative path, held as a string, against a glob.
 *
 * <p>The glob syntax is that of {@link java.nio.file.FileSystem#getPathMatcher} on Unix:
 * {@code *} and {@code ?} stay within one name, {@code **} crosses directories,
 * {@code [abc]}, {@code [a-z]} and {@code [!abc]} match one character of a name,
 * {@code {a,b}} matches either alternative, and {@code \} takes the next character
 * literally. The pattern's separator is {@code /}.
 *
 * <p>The path matcher of the default file system is not used because it wants a
 * {@link java.nio.file.Path}, and the paths matched here are as often the keys of an
 * object store as they are files. On Windows a key with a colon or a question mark in it
 * is not a {@code Path} at all, and the matcher there ignores case, which is right for a
 * local file and wrong for a key.
 *
 * <p>So there are two kinds of matcher. One {@linkplain #forKeys for keys} separates by
 * {@code /} and respects case on every platform. One {@linkplain #forLocalPaths for local
 * paths} does the same except on Windows, where it also accepts {@code \} as a separator
 * in the path and ignores case, as that file system does.
 */
public final class GlobMatcher {
  private static final boolean WINDOWS = File.separatorChar == '\\';

  private static final String REGEX_META = "\\.[]{}()*+-?^$|";

  private final Pattern regex;
  private final boolean windowsLocal;

  private GlobMatcher(String glob, boolean windowsLocal) {
    this.windowsLocal = windowsLocal;
    this.regex =
        Pattern.compile(toRegex(glob), windowsLocal
            ? Pattern.DOTALL | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
            : Pattern.DOTALL);
  }

  /** A matcher for the keys of an object store, or the path of a URL. */
  public static GlobMatcher forKeys(String glob) {
    return new GlobMatcher(glob, false);
  }

  /** A matcher for paths of the local file system. */
  public static GlobMatcher forLocalPaths(String glob) {
    return forLocalPaths(glob, WINDOWS);
  }

  static GlobMatcher forLocalPaths(String glob, boolean windows) {
    return new GlobMatcher(glob, windows);
  }

  /** A matcher for paths that are local if {@code local} is true, and keys if not. */
  public static GlobMatcher of(String glob, boolean local) {
    return local ? forLocalPaths(glob) : forKeys(glob);
  }

  /** Whether the whole of a relative path matches the glob. */
  public boolean matches(String relativePath) {
    return regex.matcher(windowsLocal ? relativePath.replace('\\', '/') : relativePath)
        .matches();
  }

  /**
   * The regular expression a glob stands for, over a path separated by {@code /}.
   *
   * @throws PatternSyntaxException if a class or a group is not closed, a group is nested,
   *     a class names the separator, or the glob ends in an escape
   */
  static String toRegex(String glob) {
    final int n = glob.length();
    StringBuilder regex = new StringBuilder();
    boolean inGroup = false;
    int i = 0;
    while (i < n) {
      char c = glob.charAt(i++);
      switch (c) {
      case '\\':
        if (i == n) {
          throw new PatternSyntaxException("No character to escape", glob, i - 1);
        }
        appendLiteral(regex, glob.charAt(i++));
        break;
      case '[':
        i = appendClass(regex, glob, i);
        break;
      case '{':
        if (inGroup) {
          throw new PatternSyntaxException("Cannot nest groups", glob, i - 1);
        }
        regex.append("(?:(?:");
        inGroup = true;
        break;
      case '}':
        if (inGroup) {
          regex.append("))");
          inGroup = false;
        } else {
          appendLiteral(regex, c);
        }
        break;
      case ',':
        regex.append(inGroup ? ")|(?:" : ",");
        break;
      case '*':
        if (i < n && glob.charAt(i) == '*') {
          regex.append(".*");
          i++;
        } else {
          regex.append("[^/]*");
        }
        break;
      case '?':
        regex.append("[^/]");
        break;
      default:
        appendLiteral(regex, c);
      }
    }
    if (inGroup) {
      throw new PatternSyntaxException("Missing '}'", glob, n - 1);
    }
    return regex.toString();
  }

  /** Appends the class that opened before {@code start}; returns the index after its end. */
  private static int appendClass(StringBuilder regex, String glob, int start) {
    final int n = glob.length();
    int i = start;
    // one character of a name, so never the separator
    regex.append("[[^/]&&[");
    if (i < n && glob.charAt(i) == '!') {
      regex.append('^');
      i++;
    } else if (i < n && glob.charAt(i) == '^') {
      regex.append("\\^");
      i++;
    }
    while (i < n) {
      char c = glob.charAt(i++);
      if (c == ']') {
        regex.append("]]");
        return i;
      }
      if (c == '/') {
        throw new PatternSyntaxException("Separator in a class", glob, i - 1);
      }
      if (c == '\\' || c == '[' || c == '^'
          || (c == '&' && i < n && glob.charAt(i) == '&')) {
        regex.append('\\');
      }
      regex.append(c);
    }
    throw new PatternSyntaxException("Missing ']'", glob, n - 1);
  }

  private static void appendLiteral(StringBuilder regex, char c) {
    if (REGEX_META.indexOf(c) >= 0) {
      regex.append('\\');
    }
    regex.append(c);
  }
}
