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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * How a file schema converts its source files (Excel, HTML, XML, Markdown, DOCX, PPTX) to JSON:
 * where the output goes, the casing of names, and how an HTML page is read.
 *
 * <p>A schema's first conversion of a file and every later re-conversion of it (its watcher,
 * its refreshable tables) run through {@link #convert(File)}, so a re-conversion writes exactly
 * what the first conversion wrote.
 */
public final class SourceFileConversion {
  private final @Nullable String sourceRoot;
  private final @Nullable File outputDirectory;
  private final @Nullable File baseDirectory;
  private final String columnNameCasing;
  private final String tableNameCasing;
  private final CrawlerConfiguration pages;

  /**
   * @param sourceRoot The schema's data directory: a file's path under it is kept in the names
   *     of the files converted from it; null when the schema has none
   * @param outputDirectory Where converted files go; null to write each beside its source
   * @param baseDirectory Where the conversion metadata is kept; null for none
   * @param columnNameCasing The casing of column names
   * @param tableNameCasing The casing of table names
   * @param pages How an HTML page is read: its content, and whether its lists are tables
   */
  public SourceFileConversion(@Nullable String sourceRoot, @Nullable File outputDirectory,
      @Nullable File baseDirectory, String columnNameCasing, String tableNameCasing,
      CrawlerConfiguration pages) {
    this.sourceRoot = sourceRoot;
    this.outputDirectory = outputDirectory;
    this.baseDirectory = baseDirectory;
    this.columnNameCasing = columnNameCasing;
    this.tableNameCasing = tableNameCasing;
    this.pages = pages;
  }

  public String columnNameCasing() {
    return columnNameCasing;
  }

  public String tableNameCasing() {
    return tableNameCasing;
  }

  public @Nullable File baseDirectory() {
    return baseDirectory;
  }

  /** Where {@code file}'s converted files go. */
  public File outputDirectoryOf(File file) {
    return outputDirectory != null ? outputDirectory : file.getParentFile();
  }

  /**
   * {@code file}'s path under the source root, kept in its converted files' names; null when
   * the schema has no root or the file is not under it.
   */
  public @Nullable String relativePathOf(File file) throws IOException {
    if (sourceRoot == null) {
      return null;
    }
    Path path = file.toPath().toAbsolutePath().normalize();
    // The root as given, or as resolved: a file a conversion record names is resolved (on macOS
    // a temporary /var/folders/... is /private/var/folders/...), a listed one is not.
    Path root = new File(sourceRoot).toPath().toAbsolutePath().normalize();
    if (path.startsWith(root)) {
      return root.relativize(path).toString();
    }
    Path resolvedRoot = new File(sourceRoot).getCanonicalFile().toPath();
    return path.startsWith(resolvedRoot) ? resolvedRoot.relativize(path).toString() : null;
  }

  /**
   * Converts {@code file} when it needs it; a failed conversion is logged and is no conversion.
   *
   * @return whether a conversion was performed
   * @throws IOException if the file's path cannot be resolved
   */
  public boolean convertIfNeeded(File file) throws IOException {
    return FileConversionManager.convertIfNeeded(file, outputDirectoryOf(file), columnNameCasing,
        tableNameCasing, baseDirectory, relativePathOf(file), pages);
  }

  /**
   * Converts {@code file} again when it needs it, as it was first converted, into
   * {@code outputDirectory}: where the files being refreshed were first written.
   *
   * @return whether a conversion was performed
   * @throws Exception if the conversion fails, so the caller can try again
   */
  public boolean reconvert(File file, File outputDirectory) throws Exception {
    return FileConversionManager.convert(file, outputDirectory, columnNameCasing,
        tableNameCasing, baseDirectory, relativePathOf(file), pages);
  }
}
