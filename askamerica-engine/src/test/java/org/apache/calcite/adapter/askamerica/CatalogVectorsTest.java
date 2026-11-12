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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The catalog similarity index: per-entry caching (only new or edited descriptions are
 * embedded again), model-change invalidation, and fusing similarity with keyword ranking.
 * EMBED is replaced by a fake that derives a vector from the text, so no data server is needed.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class CatalogVectorsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern VALUES_ROW = Pattern.compile("\\((\\d+), '((?:[^']++|'')*+)'\\)");
    private static final Pattern SINGLE = Pattern.compile("SELECT EMBED\\('((?:[^']++|'')*+)'\\)");

    @TempDir File dir;

    @AfterEach void reset() {
        CatalogVectors.resetForTest();
    }

    /** A fake EMBED: a 4-d vector from the text's hash, offset by {@code model}. */
    private static final class FakeEmbed implements CatalogVectors.SqlRunner {
        final AtomicInteger embedded = new AtomicInteger();
        final int model;

        FakeEmbed(int model) {
            this.model = model;
        }

        String vec(String text) {
            int h = text.hashCode();
            return (1 + (h & 7) + model) + "," + (1 + ((h >> 3) & 7)) + "," + (1 + ((h >> 6) & 7))
                + "," + (1 + ((h >> 9) & 7));
        }

        @Override public ArrayNode rows(String sql, int limit) {
            ArrayNode out = MAPPER.createArrayNode();
            Matcher one = SINGLE.matcher(sql);
            if (one.find()) {
                out.addObject().put("v", vec(one.group(1).replace("''", "'")));
                return out;
            }
            Matcher m = VALUES_ROW.matcher(sql);
            while (m.find()) {
                ObjectNode r = out.addObject();
                r.put("k", Integer.parseInt(m.group(1)));
                r.put("v", vec(m.group(2).replace("''", "'")));
                embedded.incrementAndGet();
            }
            return out;
        }
    }

    private static PrintStream quiet() {
        return new PrintStream(new ByteArrayOutputStream());
    }

    @Test void secondBuildReusesTheCacheAndEmbedsNothing() throws Exception {
        File cache = new File(dir, "catalog-vectors.bin");
        int entries = Catalog.embeddableEntries().size();
        assertTrue(entries > 1000, "expected the real catalog, got " + entries + " entries");

        FakeEmbed first = new FakeEmbed(0);
        CatalogVectors.build(first, cache, quiet());
        assertEquals(entries, first.embedded.get());
        assertTrue(CatalogVectors.ready());
        assertTrue(cache.isFile());

        CatalogVectors.resetForTest();
        FakeEmbed second = new FakeEmbed(0);
        CatalogVectors.build(second, cache, quiet());
        assertEquals(0, second.embedded.get(), "an unchanged catalog must come entirely from cache");
        assertTrue(CatalogVectors.ready());
    }

    @Test void aChangedModelReEmbedsEverything() throws Exception {
        File cache = new File(dir, "catalog-vectors.bin");
        CatalogVectors.build(new FakeEmbed(0), cache, quiet());
        CatalogVectors.resetForTest();
        FakeEmbed other = new FakeEmbed(5);
        CatalogVectors.build(other, cache, quiet());
        assertEquals(Catalog.embeddableEntries().size(), other.embedded.get());
    }

    @Test void anEditedDescriptionIsTheOnlyOneEmbeddedAgain() throws Exception {
        File cache = new File(dir, "catalog-vectors.bin");
        FakeEmbed seed = new FakeEmbed(0);
        CatalogVectors.build(seed, cache, quiet());
        // Rewrite the cache with one entry's hash corrupted, as if its description changed.
        List<String[]> cat = Catalog.embeddableEntries();
        java.util.Map<String, ?> before = CatalogVectors.read(cache,
            CatalogVectors.probeSignature(CatalogVectors.parseUnit(seed.vec(
                "askamerica catalog vector probe: rent burden of renter households"))), quiet());
        assertEquals(cat.size(), before.size());
        List<String> hashes = new ArrayList<>();
        float[][] vs = new float[cat.size()][];
        for (int i = 0; i < cat.size(); i++) {
            hashes.add(i == 7 ? "stale" : CatalogVectors.sha256(CatalogVectors.clip(cat.get(i)[6])));
            vs[i] = CatalogVectors.parseUnit(seed.vec(cat.get(i)[6]));
        }
        CatalogVectors.write(cache, CatalogVectors.probeSignature(CatalogVectors.parseUnit(
            seed.vec("askamerica catalog vector probe: rent burden of renter households"))),
            cat, hashes, vs);
        CatalogVectors.resetForTest();
        FakeEmbed again = new FakeEmbed(0);
        CatalogVectors.build(again, cache, quiet());
        assertEquals(1, again.embedded.get());
    }

    /** Calcite encodes SQL literals in ISO-8859-1; an em dash failed a whole EMBED batch. */
    @Test void clipMapsTextIntoLatin1() {
        String out = CatalogVectors.clip("2009\u20132026 \u2014 \u201Cquoted\u201D it\u2019s\u2026 caf\u00e9 \u4e2d");
        assertEquals("2009-2026 - \"quoted\" it's... caf\u00e9 ", out);
        for (char ch : out.toCharArray()) {
            assertTrue(ch <= 0xFF, "outside Latin-1: " + (int) ch);
        }
        StringBuilder longText = new StringBuilder();
        for (int i = 0; i < 3000; i++) {
            longText.append('x');
        }
        assertEquals(CatalogVectors.MAX_TEXT_CHARS, CatalogVectors.clip(longText.toString()).length());
    }

    @Test void parseUnitScalesToUnitLength() {
        float[] v = CatalogVectors.parseUnit("3,4");
        assertEquals(0.6f, v[0], 1e-6);
        assertEquals(0.8f, v[1], 1e-6);
    }

    /** The q3 case: keyword ranks a table named *_renters first; similarity ranks the
     *  rent-burden column first; fused, the column wins. */
    @Test void fusionLiftsWhatBothSearchesAgreeOn() {
        ArrayNode keyword = MAPPER.createArrayNode();
        keyword.add(Catalog.match("table", "disasters", "ihp_housing_assistance_renters", "table",
            null, "FEMA renters", 13));
        keyword.add(Catalog.match("column", "census", "acs_congressional_district", "long",
            "renters_cost_burdened", "Rent-burdened renter households", 10));
        List<CatalogVectors.Hit> similar = new ArrayList<>();
        similar.add(new CatalogVectors.Hit(new String[]{"column", "census",
            "acs_congressional_district", "renters_cost_burdened", "long", "Rent-burdened", ""},
            0.92));
        for (int i = 0; i < 8; i++) {
            similar.add(new CatalogVectors.Hit(new String[]{"table", "census", "t" + i, "", "table",
                "other", ""}, 0.8 - i * 0.01));
        }
        similar.add(new CatalogVectors.Hit(new String[]{"table", "disasters",
            "ihp_housing_assistance_renters", "", "table", "FEMA renters", ""}, 0.70));
        ArrayNode fused = McpServer.fuse(keyword, similar, 5);
        assertEquals("acs_congressional_district", fused.get(0).path("table").asText());
        assertEquals("table", fused.get(0).path("kind").asText());
        JsonNode col = fused.get(0).path("matching_columns").get(0);
        assertEquals("renters_cost_burdened", col.path("column").asText());
        assertEquals(0.92, col.path("similarity").asDouble(), 1e-9);
        assertEquals("ihp_housing_assistance_renters", fused.get(1).path("table").asText());
    }

    @Test void locateFindsAWordAnywhereInTheCatalog() {
        List<String> where = Catalog.locate("burden", 50);
        assertTrue(where.stream().anyMatch(w -> w.startsWith("census.acs_congressional_district")),
            String.valueOf(where));
        assertTrue(Catalog.locate("zzqxnotaword", 5).isEmpty());
    }
}
