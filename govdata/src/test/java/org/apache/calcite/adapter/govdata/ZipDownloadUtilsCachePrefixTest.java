/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that restoring a cached ZIP extract returns only that cache directory's files.
 *
 * <p>Object stores list by key prefix, so listing {@code .../title=05} also returned everything
 * under {@code .../title=05a/}. The U.S. Code has both titles, and once both were cached the
 * restored temp directory for title 05 held two XML files.
 */
@Tag("unit")
public class ZipDownloadUtilsCachePrefixTest {

  /** A local provider whose {@code listFiles} matches by key prefix, as an object store does. */
  private static final class PrefixListingProvider extends LocalFileStorageProvider {
    @Override public List<FileEntry> listFiles(String path, boolean recursive)
        throws IOException {
      String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
      File root = new File(trimmed).getParentFile();
      List<FileEntry> matches = new ArrayList<FileEntry>();
      for (FileEntry entry : super.listFiles(root.getPath(), true)) {
        if (entry.getPath().startsWith(path)) {
          matches.add(entry);
        }
      }
      return matches;
    }
  }

  @Test void cacheHitDoesNotRestoreSiblingDirectoriesSharingTheKeyPrefix(@TempDir File cacheRoot)
      throws Exception {
    writeFile(new File(cacheRoot, "title=05/usc05.xml"), "<uscDoc/>");
    writeFile(new File(cacheRoot, "title=05a/usc05A.xml"), "<uscDoc/>");
    String cachePath = new File(cacheRoot, "title=05").getPath();
    StorageProvider provider = new PrefixListingProvider();

    File restored =
        ZipDownloadUtils.downloadZipToTempDirCached("http://127.0.0.1:1/unused.zip", null,
            "zdu-prefix-test", cachePath, provider);
    try {
      File[] xml = restored.listFiles((d, n) -> n.endsWith(".xml"));
      assertEquals(1, xml.length, "restored files for title=05");
      assertEquals("usc05.xml", xml[0].getName());
    } finally {
      ZipDownloadUtils.deleteDirectory(restored);
    }
  }

  private static void writeFile(File file, String content) throws IOException {
    file.getParentFile().mkdirs();
    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
  }
}
