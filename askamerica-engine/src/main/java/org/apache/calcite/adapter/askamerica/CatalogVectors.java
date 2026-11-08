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
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Similarity search over the catalog's own descriptions: every table description and every
 * column comment, embedded once and cached locally, so search_catalog can rank by meaning and
 * not only by keyword.
 *
 * <p>Embedding is SQL: {@code EMBED(text)} runs in the local data server (pgwire-govdata),
 * which carries the embedder. This process carries none (owner rule ASKAM-007).
 *
 * <p>The cache is per entry, keyed {@code schema.table} or {@code schema.table.column}, each
 * holding a SHA-256 of the exact text embedded and its vector. On load every entry is looked
 * up: an unchanged one is reused, a new or edited one is embedded, a removed one is dropped.
 * So editing one description re-embeds that one entry. A changed embedding model, detected by
 * embedding a fixed probe sentence and comparing it with the one the cache was built with,
 * discards the whole cache.
 *
 * <p>Measured live 2026-11-07: cosine of four rent-burden searches ("rent burden", "gross rent
 * as a percentage of household income", ...) against
 * {@code census.acs_congressional_district.renters_cost_burdened}'s comment was 0.79-0.93,
 * first every time, while keyword search ranked FEMA's renter-aid table first.
 */
final class CatalogVectors {

    /** Runs SQL against the data server and returns the rows. */
    interface SqlRunner {
        ArrayNode rows(String sql, int limit) throws Exception;
    }

    static final int BATCH = 100;
    /** Text beyond this is cut before embedding; the tokenizer keeps only 512 tokens anyway. */
    static final int MAX_TEXT_CHARS = 2000;
    private static final int MAGIC = 0x43564543;  // "CVEC"
    private static final int FORMAT = 1;
    private static final String PROBE = "askamerica catalog vector probe: rent burden of renter "
        + "households";

    private static final Object LOCK = new Object();
    private static volatile List<String[]> entries;
    private static volatile float[][] vectors;
    private static volatile String status = "not started";
    private static volatile boolean running;
    private static volatile long failedAtMillis;
    /** After a failed build, how long before a search may start another. */
    static final long RETRY_AFTER_MILLIS = 5 * 60 * 1000L;
    private static final AtomicInteger EMBEDDED = new AtomicInteger();
    private static volatile int toEmbed;

    private CatalogVectors() {
    }

    /** True once every catalog entry has a vector. */
    static boolean ready() {
        return vectors != null;
    }

    /** What the index is doing, for search_catalog to say when it cannot rank by similarity. */
    static String status() {
        if (running) {
            return "building: " + EMBEDDED.get() + " of " + toEmbed + " new or changed "
                + "descriptions embedded";
        }
        return status;
    }

    /** Starts building in the background unless it is built or already building. */
    static void start(SqlRunner sql, File cacheFile, java.io.PrintStream log) {
        synchronized (LOCK) {
            if (running || vectors != null) {
                return;
            }
            if (failedAtMillis > 0
                    && System.currentTimeMillis() - failedAtMillis < RETRY_AFTER_MILLIS) {
                return;
            }
            running = true;
        }
        Thread t = new Thread(() -> {
            try {
                build(sql, cacheFile, log);
            } catch (Exception e) {
                failedAtMillis = System.currentTimeMillis();
                String msg = String.valueOf(e.getMessage());
                status = "failed: " + e.getClass().getSimpleName() + ": "
                    + (msg.length() > 300 ? msg.substring(0, 150) + " ... "
                        + msg.substring(msg.length() - 150) : msg)
                    + " (retried no sooner than " + (RETRY_AFTER_MILLIS / 60000) + " minutes "
                    + "later)";
                log.println("[askamerica-mcp] catalog vectors " + status);
            } finally {
                running = false;
            }
        }, "catalog-vectors");
        t.setDaemon(true);
        t.start();
    }

    /** Builds synchronously; {@link #start} runs this on a background thread. */
    static void build(SqlRunner sql, File cacheFile, java.io.PrintStream log)
            throws Exception {
        long t0 = System.currentTimeMillis();
        List<String[]> cat = Catalog.embeddableEntries();
        String probe = probeSignature(embed(sql, PROBE));
        Map<String, Cached> cache = read(cacheFile, probe, log);
        float[][] out = new float[cat.size()][];
        List<Integer> missing = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        for (int i = 0; i < cat.size(); i++) {
            String text = textOf(cat.get(i));
            String h = sha256(text);
            hashes.add(h);
            Cached c = cache.get(keyOf(cat.get(i)));
            if (c != null && c.hash.equals(h)) {
                out[i] = c.vector;
            } else {
                missing.add(i);
            }
        }
        toEmbed = missing.size();
        EMBEDDED.set(0);
        for (int from = 0; from < missing.size(); from += BATCH) {
            List<Integer> batch = missing.subList(from, Math.min(missing.size(), from + BATCH));
            StringBuilder values = new StringBuilder();
            for (int b = 0; b < batch.size(); b++) {
                if (b > 0) {
                    values.append(',');
                }
                values.append('(').append(batch.get(b)).append(", '")
                    .append(textOf(cat.get(batch.get(b))).replace("'", "''")).append("')");
            }
            ArrayNode rows = sql.rows("SELECT k, EMBED(t) AS v FROM (VALUES " + values
                + ") AS x(k, t)", batch.size());
            if (rows.size() != batch.size()) {
                throw new IllegalStateException("EMBED returned " + rows.size() + " rows for a "
                    + "batch of " + batch.size());
            }
            for (JsonNode r : rows) {
                out[r.path("k").asInt()] = parseUnit(r.path("v").asText());
            }
            EMBEDDED.addAndGet(batch.size());
        }
        for (int i = 0; i < out.length; i++) {
            if (out[i] == null) {
                throw new IllegalStateException("no vector for " + keyOf(cat.get(i)));
            }
        }
        if (!missing.isEmpty() || cache.size() != cat.size()) {
            write(cacheFile, probe, cat, hashes, out);
        }
        entries = cat;
        vectors = out;
        status = "ready";
        log.println("[askamerica-mcp] catalog vectors ready: " + cat.size() + " entries, "
            + missing.size() + " embedded, " + (cat.size() - missing.size()) + " from cache, "
            + (System.currentTimeMillis() - t0) + "ms");
    }

    /** One query's unit vector, embedded by the data server. */
    static float[] embed(SqlRunner sql, String text) throws Exception {
        ArrayNode rows = sql.rows("SELECT EMBED('" + clip(text).replace("'", "''") + "') AS v", 1);
        if (rows.size() != 1) {
            throw new IllegalStateException("EMBED returned " + rows.size() + " rows");
        }
        return parseUnit(rows.get(0).path("v").asText());
    }

    /** One similarity hit: the catalog entry and its cosine to the query. */
    static final class Hit {
        final String[] entry;
        final double cosine;

        Hit(String[] entry, double cosine) {
            this.entry = entry;
            this.cosine = cosine;
        }
    }

    /** The {@code k} entries most similar to {@code query}, best first. Requires {@link #ready}. */
    static List<Hit> rank(float[] query, int k) {
        List<String[]> es = entries;
        float[][] vs = vectors;
        List<Hit> hits = new ArrayList<>();
        for (int i = 0; i < vs.length; i++) {
            double dot = 0;
            for (int d = 0; d < query.length; d++) {
                dot += query[d] * vs[i][d];
            }
            hits.add(new Hit(es.get(i), dot));
        }
        hits.sort((a, b) -> Double.compare(b.cosine, a.cosine));
        return hits.subList(0, Math.min(k, hits.size()));
    }

    static String keyOf(String[] e) {
        return e[3].isEmpty() ? e[1] + "." + e[2] + e[7] : e[1] + "." + e[2] + "." + e[3];
    }

    private static String textOf(String[] e) {
        return clip(e[6]);
    }

    /**
     * The text as sent to EMBED: mapped into Latin-1 and clipped. Calcite encodes a SQL string
     * literal in ISO-8859-1, so an em dash or curly quote in a description failed the whole
     * batch ("... in character set 'ISO-8859-1'", measured live 2026-11-07). Dashes and quotes
     * map to their ASCII forms; anything else outside Latin-1 is dropped, which changes nothing
     * a similarity search depends on.
     */
    static String clip(String s) {
        StringBuilder sb = new StringBuilder(Math.min(s.length(), MAX_TEXT_CHARS));
        for (int i = 0; i < s.length() && sb.length() < MAX_TEXT_CHARS; i++) {
            char ch = s.charAt(i);
            switch (ch) {
            case '\u2010': case '\u2011': case '\u2012': case '\u2013': case '\u2014':
            case '\u2015': case '\u2212':
                sb.append('-');
                break;
            case '\u2018': case '\u2019': case '\u201A': case '\u2032':
                sb.append('\'');
                break;
            case '\u201C': case '\u201D': case '\u201E': case '\u2033':
                sb.append('"');
                break;
            case '\u2026':
                sb.append("...");
                break;
            default:
                if (ch <= 0xFF) {
                    sb.append(ch);
                } else if (Character.isWhitespace(ch) || Character.isSpaceChar(ch)) {
                    sb.append(' ');
                }
            }
        }
        return sb.toString();
    }

    /** Parses EMBED's comma-separated vector and scales it to unit length. */
    static float[] parseUnit(String csv) {
        String[] parts = csv.split(",");
        float[] v = new float[parts.length];
        double norm = 0;
        for (int i = 0; i < parts.length; i++) {
            v[i] = Float.parseFloat(parts[i].trim());
            norm += v[i] * v[i];
        }
        if (norm == 0) {
            throw new IllegalStateException("EMBED returned a zero vector");
        }
        float inv = (float) (1.0 / Math.sqrt(norm));
        for (int i = 0; i < v.length; i++) {
            v[i] *= inv;
        }
        return v;
    }

    /** Identifies the embedding model by what it does to a fixed sentence. */
    static String probeSignature(float[] probe) {
        StringBuilder sb = new StringBuilder().append(probe.length);
        for (int i = 0; i < Math.min(8, probe.length); i++) {
            sb.append(',').append(String.format(java.util.Locale.ROOT, "%.3f", probe[i]));
        }
        return sb.toString();
    }

    static String sha256(String s) throws Exception {
        byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : d) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static final class Cached {
        final String hash;
        final float[] vector;

        Cached(String hash, float[] vector) {
            this.hash = hash;
            this.vector = vector;
        }
    }

    /** The cached entries, or none when there is no cache or it was built with another model. */
    static Map<String, Cached> read(File f, String probe, java.io.PrintStream log)
            throws IOException {
        Map<String, Cached> out = new HashMap<>();
        if (!f.isFile()) {
            return out;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(f)))) {
            if (in.readInt() != MAGIC || in.readInt() != FORMAT) {
                log.println("[askamerica-mcp] catalog vectors: " + f + " is not a cache in "
                    + "this format; rebuilding");
                return out;
            }
            if (!probe.equals(in.readUTF())) {
                log.println("[askamerica-mcp] catalog vectors: embedding model changed; "
                    + "re-embedding every entry");
                return out;
            }
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String key = in.readUTF();
                String hash = in.readUTF();
                float[] v = new float[in.readInt()];
                for (int d = 0; d < v.length; d++) {
                    v[d] = in.readFloat();
                }
                out.put(key, new Cached(hash, v));
            }
        }
        return out;
    }

    /** Writes the cache beside its final name, then renames it into place. */
    static void write(File f, String probe, List<String[]> cat, List<String> hashes,
            float[][] vs) throws IOException {
        File dir = f.getParentFile();
        if (dir != null && !dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("cannot create " + dir);
        }
        File tmp = new File(f.getPath() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tmp)))) {
            out.writeInt(MAGIC);
            out.writeInt(FORMAT);
            out.writeUTF(probe);
            out.writeInt(cat.size());
            for (int i = 0; i < cat.size(); i++) {
                out.writeUTF(keyOf(cat.get(i)));
                out.writeUTF(hashes.get(i));
                out.writeInt(vs[i].length);
                for (float x : vs[i]) {
                    out.writeFloat(x);
                }
            }
        }
        Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
    }

    /** Test hook: forgets any built index. */
    static void resetForTest() {
        failedAtMillis = 0;
        entries = null;
        vectors = null;
        status = "not started";
    }
}
