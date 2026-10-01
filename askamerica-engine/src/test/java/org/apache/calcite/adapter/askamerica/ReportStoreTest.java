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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A QC'd report survives the engine process: instructions as JSON, page as HTML. */
@Tag("unit")
class ReportStoreTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test void savedReportRoundTripsWithItsInstructions(@TempDir File dir) throws Exception {
    ObjectNode args = MAPPER.createObjectNode();
    args.put("title", "Rents rose 4% where migration surged");
    ReportStore.Saved saved = ReportStore.save(dir, "create_report_artifact",
        "Rents rose 4% where migration surged", "Did migration raise rents?", args,
        "<html>page</html>");

    assertTrue(saved.id.endsWith("-rents-rose-4-where-migration-surged"), saved.id);
    JsonNode doc = MAPPER.readTree(saved.json);
    assertEquals("create_report_artifact", doc.path("tool").asText());
    assertEquals("Rents rose 4% where migration surged",
        doc.path("report").path("title").asText());

    ReportStore.Saved loaded = ReportStore.load(dir, saved.id);
    assertEquals("<html>page</html>", loaded.html);
    assertEquals("Did migration raise rents?", loaded.question);
    assertEquals(saved.title, loaded.title);
  }

  @Test void unknownOrMalformedIdIsRejected(@TempDir File dir) {
    assertThrows(IllegalArgumentException.class, () -> ReportStore.load(dir, "../../etc/passwd"));
    assertThrows(IOException.class, () -> ReportStore.load(dir, "20260930-120000-missing"));
  }

  @Test void savedPageHasAFileLinkAndIsListedNewestFirst(@TempDir File dir) throws Exception {
    ReportStore.Saved first = ReportStore.save(dir, "preview_report", "First", "Q1",
        MAPPER.createObjectNode(), "<html>1</html>");
    Thread.sleep(1100);
    ReportStore.Saved second = ReportStore.save(dir, "create_report_artifact", "Second", "Q2",
        MAPPER.createObjectNode(), "<html>2</html>");

    String url = ReportStore.fileUrl(dir, second.id);
    assertTrue(url.startsWith("file:") && url.endsWith(second.id + ".html"), url);
    assertEquals("<html>2</html>", new String(
        java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(new java.net.URI(url))),
        java.nio.charset.StandardCharsets.UTF_8));

    java.util.List<JsonNode> listed = ReportStore.list(dir, 10);
    assertEquals(2, listed.size());
    assertEquals(second.id, listed.get(0).path("id").asText());
    assertEquals(first.id, listed.get(1).path("id").asText());
    assertEquals(1, ReportStore.list(dir, 1).size());
  }

  @Test void listIsEmptyWhenNothingWasSaved(@TempDir File dir) throws Exception {
    assertTrue(ReportStore.list(dir, 10).isEmpty());
  }

  @Test void slugIsBoundedAndNeverEmpty() {
    assertEquals("report", ReportStore.slug("!!!"));
    assertEquals("report", ReportStore.slug(null));
    assertTrue(ReportStore.slug("a very long title that keeps going well past forty chars")
      .length() <= 40);
  }
}
