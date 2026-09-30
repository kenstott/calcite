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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Durable copies of every report that passed QC, under {@code ~/.askamerica/reports/}: the
 * report instructions as JSON ({@code <id>.json}) and the rendered page ({@code <id>.html}).
 * The page's local http link and the in-memory last report both die with the engine process,
 * which Claude Desktop restarts freely; these files let a report be opened, re-served
 * ({@code restore_report}) or uploaded later.
 */
final class ReportStore {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern ID = Pattern.compile("[0-9]{8}-[0-9]{6}-[a-z0-9-]{1,40}");

    private ReportStore() {
    }

    /** A saved report, as {@code upload_report} needs it. */
    static final class Saved {
        final String id;
        final String title;
        final String question;
        final String html;
        final File json;

        Saved(String id, String title, String question, String html, File json) {
            this.id = id;
            this.title = title;
            this.question = question;
            this.html = html;
            this.json = json;
        }
    }

    static File dir() {
        return new File(new File(System.getProperty("user.home"), ".askamerica"), "reports");
    }

    /** Writes both files and returns the saved report; {@code report} is the tool's input. */
    static Saved save(String tool, String title, String question, JsonNode report, String html)
            throws IOException {
        return save(dir(), tool, title, question, report, html);
    }

    static Saved save(File dir, String tool, String title, String question, JsonNode report,
            String html) throws IOException {
        String stamp = new SimpleDateFormat("yyyyMMdd-HHmmss", Locale.ROOT).format(new Date());
        String id = stamp + "-" + slug(title);
        Path d = Files.createDirectories(dir.toPath());
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("id", id);
        doc.put("tool", tool);
        doc.put("title", title);
        doc.put("question", question);
        doc.set("report", report);
        Path json = d.resolve(id + ".json");
        Files.write(json, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(doc));
        Files.write(d.resolve(id + ".html"), html.getBytes(StandardCharsets.UTF_8));
        return new Saved(id, title, question, html, json.toFile());
    }

    /** Loads a report saved by {@link #save}; throws if the id is malformed or not on disk. */
    static Saved load(String id) throws IOException {
        return load(dir(), id);
    }

    static Saved load(File dir, String id) throws IOException {
        if (id == null || !ID.matcher(id).matches()) {
            throw new IllegalArgumentException("report_id '" + id + "' is not a saved report id "
                + "(yyyyMMdd-HHmmss-slug, as returned when the report was built)");
        }
        File json = new File(dir, id + ".json");
        File html = htmlFile(dir, id);
        if (!json.isFile() || !html.isFile()) {
            throw new IOException("No saved report '" + id + "' in " + dir);
        }
        JsonNode doc = MAPPER.readTree(json);
        return new Saved(id, doc.path("title").asText(null), doc.path("question").asText(null),
            new String(Files.readAllBytes(html.toPath()), StandardCharsets.UTF_8), json);
    }

    /** The rendered page of a saved report; {@code id} must come from {@link #save}. */
    static File htmlFile(File dir, String id) {
        return new File(dir, id + ".html");
    }

    /**
     * {@code file://} link to the saved page. The page is self-contained (inline styles,
     * {@code data:} images), so it opens with no server running — unlike the local http link,
     * it survives the engine process.
     */
    static String fileUrl(String id) {
        return fileUrl(dir(), id);
    }

    static String fileUrl(File dir, String id) {
        return htmlFile(dir, id).getAbsoluteFile().toURI().toString();
    }

    /** Saved reports' JSON documents, newest first, at most {@code limit}. */
    static List<JsonNode> list(int limit) throws IOException {
        return list(dir(), limit);
    }

    static List<JsonNode> list(File dir, int limit) throws IOException {
        File[] files = dir.listFiles((d, n) -> n.endsWith(".json")
            && ID.matcher(n.substring(0, n.length() - 5)).matches());
        List<JsonNode> out = new ArrayList<>();
        if (files == null) {
            return out;
        }
        // Ids start with a yyyyMMdd-HHmmss stamp, so name order is time order.
        Arrays.sort(files, (a, b) -> b.getName().compareTo(a.getName()));
        for (File f : files) {
            if (out.size() >= limit) {
                break;
            }
            out.add(MAPPER.readTree(f));
        }
        return out;
    }

    static String slug(String title) {
        String s = title == null ? "" : title.toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "-").replaceAll("^-+|-+$", "");
        if (s.length() > 40) {
            s = s.substring(0, 40).replaceAll("-+$", "");
        }
        return s.isEmpty() ? "report" : s;
    }
}
