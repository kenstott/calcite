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

/**
 * String operations on paths that may be local file paths or storage URIs.
 *
 * <p>A directory in this adapter is a string that is either a URI
 * ({@code s3://bucket/key}, {@code https://host/x}, {@code file:///x}) or a local path.
 * A URI is separated by {@code /} everywhere. A local path on Windows is separated by
 * {@code \}, by {@code /}, or by both in one string ({@code C:\data/sales}), and has a
 * drive letter whose case is not significant. Code that joins or compares such strings
 * with {@code "/"} alone is right for URIs and for Unix and wrong for Windows; these
 * methods are the one place that knows the difference.
 *
 * <p>Off Windows, and for URIs on any platform, every method here gives the result the
 * plain {@code "/"} string operation gives.
 */
public final class LocalPaths {
  private static final boolean WINDOWS = File.separatorChar == '\\';

  /** A scheme of two or more characters, so that a drive letter is not one. */
  private static final Pattern URI_PREFIX =
      Pattern.compile("^(?:[A-Za-z][A-Za-z0-9+.-]+://|file:).*", Pattern.DOTALL);

  private static final Pattern DRIVE_PREFIX = Pattern.compile("^[A-Za-z]:.*", Pattern.DOTALL);

  private LocalPaths() {
  }

  /** Whether a path is a URI ({@code scheme://...} or {@code file:...}), not a local path. */
  public static boolean isUri(String path) {
    return URI_PREFIX.matcher(path).matches();
  }

  /** Whether a character separates the segments of a path. */
  public static boolean isSeparator(String path, char c) {
    return isSeparator(path, c, WINDOWS);
  }

  static boolean isSeparator(String path, char c, boolean windows) {
    return c == '/' || (c == '\\' && windows && !isUri(path));
  }

  /**
   * A path with the separators of its kind: a local path on Windows gets {@code \}
   * throughout; a URI, and any path on another platform, is returned as it is.
   */
  public static String normalize(String path) {
    return normalize(path, WINDOWS);
  }

  static String normalize(String path, boolean windows) {
    return windows && !isUri(path) ? path.replace('/', '\\') : path;
  }

  /**
   * A path with {@code /} for every separator, for code that parses it by {@code /}: a
   * local path on Windows has its {@code \} replaced, one for one; any other path is
   * returned as it is.
   */
  public static String toSlashes(String path) {
    return toSlashes(path, WINDOWS);
  }

  static String toSlashes(String path, boolean windows) {
    return windows && !isUri(path) ? path.replace('\\', '/') : path;
  }

  /** Whether a path ends with a separator. */
  public static boolean endsWithSeparator(String path) {
    return endsWithSeparator(path, WINDOWS);
  }

  static boolean endsWithSeparator(String path, boolean windows) {
    return !path.isEmpty() && isSeparator(path, path.charAt(path.length() - 1), windows);
  }

  /** Whether a path starts with a separator. */
  public static boolean startsWithSeparator(String path) {
    return startsWithSeparator(path, WINDOWS);
  }

  static boolean startsWithSeparator(String path, boolean windows) {
    return !path.isEmpty() && isSeparator(path, path.charAt(0), windows);
  }

  /**
   * Whether a local path is absolute: it starts with a separator, or on Windows with a
   * drive letter.
   */
  public static boolean isAbsolute(String path) {
    return isAbsolute(path, WINDOWS);
  }

  static boolean isAbsolute(String path, boolean windows) {
    return startsWithSeparator(path, windows)
        || (windows && DRIVE_PREFIX.matcher(path).matches());
  }

  /**
   * Joins a directory and a child with one separator between them: {@code /} for a URI
   * and off Windows, {@code \} for a local path on Windows, where the result is also
   * {@linkplain #normalize normalized}.
   */
  public static String join(String base, String child) {
    return join(base, child, WINDOWS);
  }

  static String join(String base, String child, boolean windows) {
    if (windows && !isUri(base)) {
      String b = normalize(base, true);
      String c = normalize(child, true);
      return b.endsWith("\\") ? b + c : b + "\\" + c;
    }
    return base.endsWith("/") ? base + child : base + "/" + child;
  }

  /** The index of the last separator in a path, or -1. */
  public static int lastSeparator(String path) {
    return lastSeparator(path, WINDOWS);
  }

  static int lastSeparator(String path, boolean windows) {
    int slash = path.lastIndexOf('/');
    return windows && !isUri(path) ? Math.max(slash, path.lastIndexOf('\\')) : slash;
  }

  /** The index of the first separator in a path, or -1. */
  public static int firstSeparator(String path) {
    return firstSeparator(path, WINDOWS);
  }

  static int firstSeparator(String path, boolean windows) {
    for (int i = 0; i < path.length(); i++) {
      if (isSeparator(path, path.charAt(i), windows)) {
        return i;
      }
    }
    return -1;
  }

  /** The last segment of a path: what follows its last separator, or all of it. */
  public static String fileName(String path) {
    return fileName(path, WINDOWS);
  }

  static String fileName(String path, boolean windows) {
    return path.substring(lastSeparator(path, windows) + 1);
  }

  /** What precedes the last separator of a path, or null when it has no separator. */
  public static String parent(String path) {
    return parent(path, WINDOWS);
  }

  static String parent(String path, boolean windows) {
    int i = lastSeparator(path, windows);
    return i < 0 ? null : path.substring(0, i);
  }

  /**
   * The directory part of a relative path as a prefix for a file name: its separators
   * replaced by {@code _}; null when the path has no directory part.
   */
  public static String directoryPrefix(String relativePath) {
    return directoryPrefix(relativePath, WINDOWS);
  }

  static String directoryPrefix(String relativePath, boolean windows) {
    String directory = parent(relativePath, windows);
    if (directory == null) {
      return null;
    }
    String prefix = directory.replace('/', '_');
    return windows && !isUri(relativePath) ? prefix.replace('\\', '_') : prefix;
  }

  /**
   * The remainder of a path under a base directory, without a leading separator; the
   * empty string when they are the same path; null when the path is not under the base.
   *
   * <p>The base matches as whole segments: {@code /data/sales2} is not under
   * {@code /data/sales}. Between local paths on Windows either separator matches the
   * other and letter case is not significant.
   */
  public static String relativize(String base, String path) {
    return relativize(base, path, WINDOWS);
  }

  static String relativize(String base, String path, boolean windows) {
    boolean local = windows && !isUri(base) && !isUri(path);
    int n = base.length();
    while (n > 0 && isSeparator(base, base.charAt(n - 1), windows)) {
      n--;
    }
    if (path.length() < n) {
      return null;
    }
    for (int i = 0; i < n; i++) {
      char b = base.charAt(i);
      char p = path.charAt(i);
      if (b == p) {
        continue;
      }
      if (!local) {
        return null;
      }
      boolean bothSeparators = isSeparator(base, b, true) && isSeparator(path, p, true);
      if (!bothSeparators && Character.toLowerCase(b) != Character.toLowerCase(p)) {
        return null;
      }
    }
    if (path.length() == n) {
      return "";
    }
    if (base.isEmpty()) {
      return path;
    }
    int start = n;
    if (!isSeparator(path, path.charAt(start), windows)) {
      // "/data/sales2" is not under "/data/sales": the base ended inside a segment
      return null;
    }
    while (start < path.length() && isSeparator(path, path.charAt(start), windows)) {
      start++;
    }
    return path.substring(start);
  }

  /** Whether a path is a base directory or is under it; see {@link #relativize}. */
  public static boolean isUnder(String base, String path) {
    return relativize(base, path) != null;
  }
}
