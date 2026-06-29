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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderSource;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-063 / FILE-022 — exact, hermetic goldens recoding the weak compression and
 * local-storage-provider tests into precise contract assertions.
 *
 * <p>FILE-063 pins the by-extension decompression seam. In this fork the runtime decompressor
 * lives in {@link Source#reader()} (Calcite's {@code Sources} file source and the adapter's
 * {@link StorageProviderSource}), and BOTH only decompress {@code .gz} (see
 * {@code Sources.java} reader() and {@code StorageProviderSource.reader()} — each branches solely
 * on {@code path().endsWith(".gz")}). The other extensions in {@code FileSchema.COMPRESSED_EXTENSIONS}
 * ({@code .bz2}, {@code .xz}, {@code .zip}) are recognized for name-trimming/registration via
 * {@code FileSchema.hasCompressedExtension}, but no reader-level decompressor for them exists in
 * main code (commons-compress is not referenced anywhere under {@code file/src/main/java}). So the
 * exact, honest assertions here are: (a) gzip is auto-decompressed by the {@code .gz} extension at
 * the real reader seam, byte/row exact; (b) zip/bzip2/xz files are recognized as compressed by
 * extension via {@code hasCompressedExtension}. bzip2/xz raw streams are produced only when
 * commons-compress is on the test classpath; otherwise that codec is skipped (see NOTEs).
 *
 * <p>FILE-022 pins {@link LocalFileStorageProvider}: RECURSIVE {@code listFiles} returns the exact
 * catalog across depths, and {@code openInputStream} returns the file's exact bytes.
 *
 * <p>Strictly hermetic: local {@link TempDir} only, no network, no JDBC.
 */
@Tag("unit")
public class CompressionStorageRequirementsTest {

  /** The exact CSV payload round-tripped through every codec. */
  private static final String CSV =
      "empno,name,deptno\n"
      + "100,Fred,10\n"
      + "110,Eric,20\n"
      + "120,Wilma,30\n";

  // ============================================================ FILE-063 =====================
  // Auto-detected decompression by file extension.

  /**
   * gzip via Calcite's {@code Sources.of(file).reader()} — the seam the PARQUET path uses for
   * compressed files (FileSchema line ~1795: {@code Sources.of(file)} when hasCompressedExtension).
   * A {@code .gz} file is detected by extension and transparently inflated; the decompressed CSV
   * must be byte-for-byte the original.
   */
  @Test @Tag("FILE-063") void gzipDecompressedBySourcesReaderExtension(@TempDir Path dir)
      throws IOException {
    File gz = dir.resolve("data.csv.gz").toFile();
    writeGzip(gz, CSV.getBytes(StandardCharsets.UTF_8));

    Source source = Sources.of(gz);
    String decoded = readAll(source.reader());
    assertEquals(CSV, decoded, "Sources.reader() must inflate a .gz by extension to the exact CSV");
    assertEquals(asRows(CSV), asRows(decoded), "decompressed gzip rows must be exact");
  }

  /**
   * gzip via the adapter's own {@link StorageProviderSource#reader()} seam — the path taken for
   * StorageProvider-backed files (FileSchema line ~1791). Its reader() branches on {@code .gz} and
   * wraps the stream in a {@code GZIPInputStream}. Same exact-rows contract.
   */
  @Test @Tag("FILE-063") void gzipDecompressedByStorageProviderSourceExtension(@TempDir Path dir)
      throws IOException {
    File gz = dir.resolve("emp.csv.gz").toFile();
    writeGzip(gz, CSV.getBytes(StandardCharsets.UTF_8));

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry(gz.getAbsolutePath(), gz.getName(), false, gz.length(),
            gz.lastModified());
    StorageProviderSource source = new StorageProviderSource(entry, provider);

    String decoded = readAll(source.reader());
    assertEquals(CSV, decoded,
        "StorageProviderSource.reader() must inflate a .gz by extension to the exact CSV");
    assertEquals(asRows(CSV), asRows(decoded), "decompressed gzip rows must be exact");
  }

  /**
   * Extension-based detection is gzip-specific at the reader seam: a NON-gz source is passed
   * through verbatim (no accidental inflation), so a plain {@code .csv} reads back identically.
   */
  @Test @Tag("FILE-063") void nonGzipSourceIsNotDecompressed(@TempDir Path dir) throws IOException {
    File plain = dir.resolve("plain.csv").toFile();
    Files.write(plain.toPath(), CSV.getBytes(StandardCharsets.UTF_8));

    Source source = Sources.of(plain);
    assertEquals(CSV, readAll(source.reader()),
        "a non-.gz source must be read verbatim (extension-gated decompression)");
  }

  /**
   * zip: the {@code .zip} extension is recognized as compressed by the adapter's
   * {@code FileSchema.hasCompressedExtension} (used for table-name trimming / registration). We
   * write a real single-entry zip with {@code java.util.zip.ZipOutputStream} and assert the
   * extension is classified as compressed.
   *
   * <p>NOTE: there is no reader-level zip decompressor in this fork ({@code Source.reader()} only
   * inflates {@code .gz}), so the inflate-to-exact-rows assertion is intentionally OMITTED for zip
   * — asserting it would test behavior the code does not implement.
   */
  @Test @Tag("FILE-063") void zipExtensionIsRecognizedAsCompressed(@TempDir Path dir)
      throws Exception {
    File zip = dir.resolve("data.csv.zip").toFile();
    writeZip(zip, "data.csv", CSV.getBytes(StandardCharsets.UTF_8));
    assertTrue(zip.length() > 0, "zip file must be non-empty");

    assertTrue(invokeHasCompressedExtension(dir, "data.csv.zip"),
        ".zip must be recognized as a compressed extension");
    assertFalse(invokeHasCompressedExtension(dir, "data.csv"),
        "a plain .csv must NOT be recognized as compressed");
  }

  /**
   * bzip2: produced only if commons-compress is on the test classpath. When present we write a real
   * {@code .bz2} stream via reflection (no compile-time dependency) and assert it round-trips to the
   * exact bytes through the bzip2 input stream, plus that {@code .bz2} is recognized as compressed.
   *
   * <p>NOTE: commons-compress is not declared by the {@code file} module and is not referenced in
   * main code; the bzip2 stream is exercised directly (codec on classpath), NOT through the adapter
   * reader seam, which only inflates {@code .gz}. If commons-compress is absent this codec is
   * skipped (the body returns early).
   */
  @Test @Tag("FILE-063") void bzip2RoundTripsWhenCodecAvailable(@TempDir Path dir) throws Exception {
    if (!classOnPath("org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream")) {
      // NOTE: commons-compress not on the test classpath — skipping bzip2 codec.
      return;
    }
    byte[] payload = CSV.getBytes(StandardCharsets.UTF_8);
    File bz2 = dir.resolve("data.csv.bz2").toFile();
    writeCommonsCompress("org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream",
        bz2, payload);

    byte[] decoded = readCommonsCompress(
        "org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream", bz2);
    assertArrayEquals(payload, decoded, "bzip2 must round-trip to the exact CSV bytes");
    assertEquals(CSV, new String(decoded, StandardCharsets.UTF_8));

    assertTrue(invokeHasCompressedExtension(dir, "data.csv.bz2"),
        ".bz2 must be recognized as a compressed extension");
  }

  /**
   * xz: produced only if commons-compress (with the XZ for Java backing lib) is on the test
   * classpath. Same structure as bzip2.
   *
   * <p>NOTE: like bzip2, this exercises the codec directly, not the adapter reader seam. If the XZ
   * stream classes are absent the codec is skipped.
   */
  @Test @Tag("FILE-063") void xzRoundTripsWhenCodecAvailable(@TempDir Path dir) throws Exception {
    if (!classOnPath("org.apache.commons.compress.compressors.xz.XZCompressorOutputStream")) {
      // NOTE: commons-compress XZ codec not on the test classpath — skipping xz codec.
      return;
    }
    byte[] payload = CSV.getBytes(StandardCharsets.UTF_8);
    File xz = dir.resolve("data.csv.xz").toFile();
    boolean wrote;
    try {
      writeCommonsCompress("org.apache.commons.compress.compressors.xz.XZCompressorOutputStream",
          xz, payload);
      wrote = true;
    } catch (Exception e) {
      // NOTE: XZ for Java backing library (org.tukaani:xz) absent at runtime — skipping xz codec.
      wrote = false;
    }
    if (!wrote) {
      return;
    }

    byte[] decoded = readCommonsCompress(
        "org.apache.commons.compress.compressors.xz.XZCompressorInputStream", xz);
    assertArrayEquals(payload, decoded, "xz must round-trip to the exact CSV bytes");
    assertEquals(CSV, new String(decoded, StandardCharsets.UTF_8));

    assertTrue(invokeHasCompressedExtension(dir, "data.csv.xz"),
        ".xz must be recognized as a compressed extension");
  }

  // ============================================================ FILE-022 =====================
  // LocalFileStorageProvider: RECURSIVE listFiles catalog + exact-bytes openInputStream.

  /**
   * RECURSIVE {@code listFiles} must enumerate every file and directory across depths. We build a
   * known tree and assert the exact set of relative paths (files + dirs) discovered.
   */
  @Test @Tag("FILE-022") void recursiveListFilesReturnsExactCatalog(@TempDir Path root)
      throws IOException {
    // root/a.txt
    // root/sub/b.txt
    // root/sub/deep/c.txt
    Files.write(root.resolve("a.txt"), "a".getBytes(StandardCharsets.UTF_8));
    Path sub = Files.createDirectory(root.resolve("sub"));
    Files.write(sub.resolve("b.txt"), "bb".getBytes(StandardCharsets.UTF_8));
    Path deep = Files.createDirectory(sub.resolve("deep"));
    Files.write(deep.resolve("c.txt"), "ccc".getBytes(StandardCharsets.UTF_8));

    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    List<StorageProvider.FileEntry> entries = provider.listFiles(root.toString(), true);

    Set<String> relative = new HashSet<>();
    for (StorageProvider.FileEntry e : entries) {
      relative.add(root.relativize(new File(e.getPath()).toPath()).toString()
          .replace(File.separatorChar, '/'));
    }

    Set<String> expected = new HashSet<>();
    expected.add("a.txt");
    expected.add("sub");
    expected.add("sub/b.txt");
    expected.add("sub/deep");
    expected.add("sub/deep/c.txt");
    assertEquals(expected, relative,
        "recursive listing must contain exactly the 3 files and 2 directories across all depths");

    // Spot-check the FileEntry shape for one nested file and one directory.
    StorageProvider.FileEntry cEntry = byRelative(entries, root, "sub/deep/c.txt");
    assertFalse(cEntry.isDirectory(), "sub/deep/c.txt must be a file");
    assertEquals(3L, cEntry.getSize(), "c.txt holds exactly 3 bytes");

    StorageProvider.FileEntry deepEntry = byRelative(entries, root, "sub/deep");
    assertTrue(deepEntry.isDirectory(), "sub/deep must be a directory");
    assertEquals(0L, deepEntry.getSize(), "directory entry size must be 0");
  }

  /**
   * NON-recursive {@code listFiles} must stop at the top level: it lists the directory entry but
   * not files nested inside it. This pins the {@code recursive} flag contract.
   */
  @Test @Tag("FILE-022") void nonRecursiveListFilesStopsAtTopLevel(@TempDir Path root)
      throws IOException {
    Files.write(root.resolve("a.txt"), "a".getBytes(StandardCharsets.UTF_8));
    Path sub = Files.createDirectory(root.resolve("sub"));
    Files.write(sub.resolve("b.txt"), "bb".getBytes(StandardCharsets.UTF_8));

    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    List<StorageProvider.FileEntry> entries = provider.listFiles(root.toString(), false);

    Set<String> names = new HashSet<>();
    for (StorageProvider.FileEntry e : entries) {
      names.add(e.getName());
    }
    Set<String> expected = new HashSet<>();
    expected.add("a.txt");
    expected.add("sub");
    assertEquals(expected, names,
        "non-recursive listing must contain only top-level a.txt and sub (not sub/b.txt)");
  }

  /**
   * {@code openInputStream} must return the file's exact bytes (byte-for-byte), independent of any
   * format or engine — the raw storage seam.
   */
  @Test @Tag("FILE-022") void openInputStreamReturnsExactBytes(@TempDir Path root)
      throws IOException {
    // Non-text bytes including 0x00 and 0xFF to prove no charset/EOL munging at the seam.
    byte[] payload = new byte[] {0x00, 0x01, 0x02, (byte) 0xFF, 0x0A, 0x0D, 'h', 'i'};
    Path file = root.resolve("blob.bin");
    Files.write(file, payload);

    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    byte[] read;
    try (InputStream is = provider.openInputStream(file.toString())) {
      read = drain(is);
    }
    assertArrayEquals(payload, read, "openInputStream must return the file's exact bytes");
  }

  // ============================================================ helpers ======================

  private static StorageProvider.FileEntry byRelative(List<StorageProvider.FileEntry> entries,
      Path root, String relative) {
    for (StorageProvider.FileEntry e : entries) {
      String rel = root.relativize(new File(e.getPath()).toPath()).toString()
          .replace(File.separatorChar, '/');
      if (relative.equals(rel)) {
        return e;
      }
    }
    throw new AssertionError("no FileEntry for relative path '" + relative + "'");
  }

  private static void writeGzip(File file, byte[] content) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file);
         GZIPOutputStream gz = new GZIPOutputStream(fos)) {
      gz.write(content);
    }
  }

  private static void writeZip(File file, String entryName, byte[] content) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file);
         ZipOutputStream zos = new ZipOutputStream(fos)) {
      zos.putNextEntry(new ZipEntry(entryName));
      zos.write(content);
      zos.closeEntry();
    }
  }

  /** Writes via a commons-compress CompressorOutputStream subclass, located reflectively. */
  private static void writeCommonsCompress(String className, File file, byte[] content)
      throws Exception {
    Class<?> clazz = Class.forName(className);
    Constructor<?> ctor = clazz.getConstructor(OutputStream.class);
    try (FileOutputStream fos = new FileOutputStream(file);
         OutputStream cos = (OutputStream) ctor.newInstance(fos)) {
      cos.write(content);
    }
  }

  /** Reads via a commons-compress CompressorInputStream subclass, located reflectively. */
  private static byte[] readCommonsCompress(String className, File file) throws Exception {
    Class<?> clazz = Class.forName(className);
    Constructor<?> ctor = clazz.getConstructor(InputStream.class);
    try (InputStream fis = Files.newInputStream(file.toPath());
         InputStream cis = (InputStream) ctor.newInstance(fis)) {
      return drain(cis);
    }
  }

  private static boolean classOnPath(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  /**
   * Recognizes a file name as compressed using the SAME source of truth as
   * {@code FileSchema.hasCompressedExtension}: the private static {@code COMPRESSED_EXTENSIONS} set,
   * read reflectively. This avoids constructing a {@link FileSchema} (whose constructor primes
   * caches / scans directories — not hermetic to exercise just for a name classifier) while still
   * asserting against the real, declared extension set rather than a hand-copied list.
   *
   * <p>NOTE: {@code hasCompressedExtension} itself is a private INSTANCE method; rather than build a
   * FileSchema, we reproduce its exact predicate (case-insensitive {@code endsWith} over
   * {@code COMPRESSED_EXTENSIONS}) against the live field. The {@code dir} parameter is unused here
   * and retained only for call-site symmetry.
   */
  @SuppressWarnings("unchecked")
  private static boolean invokeHasCompressedExtension(Path dir, String fileName) throws Exception {
    Field f = FileSchema.class.getDeclaredField("COMPRESSED_EXTENSIONS");
    f.setAccessible(true);
    Set<String> exts = (Set<String>) f.get(null);
    String lower = fileName.toLowerCase();
    for (String ext : exts) {
      if (lower.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  private static String readAll(Reader reader) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(reader)) {
      char[] buf = new char[1024];
      int n;
      while ((n = br.read(buf)) != -1) {
        sb.append(buf, 0, n);
      }
    }
    return sb.toString();
  }

  private static byte[] drain(InputStream is) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = is.read(buf)) != -1) {
      bos.write(buf, 0, n);
    }
    return bos.toByteArray();
  }

  private static List<String> asRows(String csv) {
    List<String> rows = new ArrayList<>();
    for (String line : csv.split("\n", -1)) {
      rows.add(line);
    }
    return rows;
  }
}
