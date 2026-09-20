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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Client for typesafe.ai's Jev "System One Model" — a typed-decision endpoint used here to
 * grade a single fact-check claim independently of the calling LLM's own self-assessment.
 * One request asks two questions in parallel: a {@code choice} among the candidate verdicts
 * and a {@code score} against the Washington Post Fact Checker's 0-4 Pinocchios rubric. See
 * {@code https://docs.typesafe.ai/api.md} for the wire format.
 *
 * <p>Fails closed: any transport error, non-200 response (after retrying 429/529 per the
 * docs' backoff guidance), or malformed response throws rather than returning a default
 * verdict — a silent fallback here would let an unscored claim through {@code publish_report}
 * looking exactly like a scored one.
 */
final class JevClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;
    private static final int MAX_ATTEMPTS = 3;

    private JevClient() { }

    /** API base — overridable (system property wins, then env, then prod). */
    private static String apiBase() {
        String p = System.getProperty("typesafeai.api.url");
        if (p != null && !p.isEmpty()) {
            return p;
        }
        String e = System.getenv("TYPESAFEAI_API_URL");
        return e != null && !e.isEmpty() ? e : "https://api.typesafe.ai";
    }

    private static String apiKey() {
        String p = System.getProperty("typesafeai.api.key");
        if (p != null && !p.isEmpty()) {
            return p;
        }
        return System.getenv("TYPESAFEAI_API_KEY");
    }

    /** Whether a key is configured, so callers can refuse with a clear message up front
     *  rather than failing deep inside an HTTP call. */
    static boolean isConfigured() {
        String k = apiKey();
        return k != null && !k.isEmpty();
    }

    /** Result of scoring one claim: the chosen verdict and its confidence, plus the
     *  Pinocchios count (0-4) and its confidence. Confidences are Jev's own calibrated
     *  probabilities, not a self-report from the text that produced them. */
    static final class ScoreResult {
        final String verdict;
        final double verdictConfidence;
        final int pinocchiosCount;
        final double pinocchiosConfidence;

        ScoreResult(String verdict, double verdictConfidence, int pinocchiosCount,
            double pinocchiosConfidence) {
            this.verdict = verdict;
            this.verdictConfidence = verdictConfidence;
            this.pinocchiosCount = pinocchiosCount;
            this.pinocchiosConfidence = pinocchiosConfidence;
        }
    }

    private static final String[] PINOCCHIOS_LEVELS = {
        "0 -- Geppetto Checkmark: the claim is fully accurate, in context, with nothing "
            + "misleading.",
        "1 -- some shading of the facts, selective framing, or an unimportant omission, but "
            + "nothing that changes the substance.",
        "2 -- significant omissions and/or exaggerations; some factual error may be present.",
        "3 -- significant factual error and/or obvious contradictions; approaches the "
            + "'whopper' territory.",
        "4 -- a whopper: a claim so contradicted by the evidence that it is essentially false."
    };

    /**
     * Score one claim: {@code assertion} is the statement under test, {@code evidence} is a
     * plain-text summary of what was found (the article's figure vs. the warehouse or
     * independent figure, and the sources), and {@code candidateVerdicts} are the allowed
     * choices (mirrors the {@code verdict} values {@code publish_report} already accepts,
     * e.g. "accurate", "not checkable here", "stale vintage", "misleading", "false").
     *
     * @throws IOException on any transport failure, non-2xx response after retries, or a
     *     response that doesn't carry both answers — never silently returns a default.
     */
    static ScoreResult scoreClaim(String assertion, String evidence,
        List<String> candidateVerdicts) throws IOException {
        if (!isConfigured()) {
            throw new IOException(
                "TYPESAFEAI_API_KEY is not configured -- score_claim cannot run without it.");
        }
        String state = "Claim: " + assertion + "\n\nEvidence: " + evidence;

        ObjectNode req = MAPPER.createObjectNode();
        req.put("state", state);
        req.put("model", "jev-latest");
        ObjectNode questions = MAPPER.createObjectNode();

        ObjectNode verdictQ = MAPPER.createObjectNode();
        verdictQ.put("type", "choice");
        verdictQ.put("instructions",
            "Given the claim and the evidence gathered against it, which verdict best fits? "
            + "Grade the claim itself, not whether it was accurately quoted or attributed.");
        ObjectNode verdictCriteria = MAPPER.createObjectNode();
        for (String v : candidateVerdicts) {
            verdictCriteria.putNull(v);
        }
        verdictQ.set("criteria", verdictCriteria);
        questions.set("verdict", verdictQ);

        ObjectNode pinocchiosQ = MAPPER.createObjectNode();
        pinocchiosQ.put("type", "score");
        pinocchiosQ.put("instructions",
            "Rate the claim's accuracy on the Washington Post Fact Checker's Pinocchios "
            + "scale, given the evidence.");
        com.fasterxml.jackson.databind.node.ArrayNode levels = MAPPER.createArrayNode();
        for (String level : PINOCCHIOS_LEVELS) {
            levels.add(level);
        }
        pinocchiosQ.set("criteria", levels);
        questions.set("pinocchios", pinocchiosQ);

        req.set("questions", questions);

        JsonNode resp = postWithRetry(req);
        JsonNode answers = resp.path("answers");
        JsonNode verdictA = answers.path("verdict");
        JsonNode pinocchiosA = answers.path("pinocchios");
        if (!verdictA.has("choice") || !pinocchiosA.has("score")) {
            throw new IOException(
                "typesafe.ai response missing 'verdict' or 'pinocchios' answer: " + resp);
        }
        return new ScoreResult(
            verdictA.path("choice").asText(),
            verdictA.path("confidence").asDouble(0.0),
            (int) Math.round(pinocchiosA.path("score").asDouble()),
            pinocchiosA.path("confidence").asDouble(0.0));
    }

    private static JsonNode postWithRetry(ObjectNode requestBody) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            HttpURLConnection c = null;
            try {
                URL url = java.net.URI.create(apiBase() + "/v1/systemone").toURL();
                c = (HttpURLConnection) url.openConnection();
                c.setRequestMethod("POST");
                c.setDoOutput(true);
                c.setConnectTimeout(CONNECT_TIMEOUT_MS);
                c.setReadTimeout(READ_TIMEOUT_MS);
                c.setRequestProperty("Authorization", "Bearer " + apiKey());
                c.setRequestProperty("Content-Type", "application/json");
                byte[] body = MAPPER.writeValueAsBytes(requestBody);
                try (OutputStream os = c.getOutputStream()) {
                    os.write(body);
                }
                int code = c.getResponseCode();
                if (code == 200) {
                    return MAPPER.readTree(readAll(c.getInputStream()));
                }
                String errBody = readAll(c.getErrorStream());
                if ((code == 429 || code == 529) && attempt < MAX_ATTEMPTS) {
                    last = new IOException("typesafe.ai returned " + code + ": " + errBody);
                    sleepBackoff(attempt);
                    continue;
                }
                throw new IOException("typesafe.ai returned " + code + ": " + errBody);
            } catch (IOException e) {
                last = e;
                if (attempt >= MAX_ATTEMPTS) {
                    throw e;
                }
                sleepBackoff(attempt);
            } finally {
                if (c != null) {
                    c.disconnect();
                }
            }
        }
        throw last != null ? last : new IOException("typesafe.ai call failed with no response");
    }

    private static void sleepBackoff(int attempt) throws IOException {
        try {
            Thread.sleep(500L * (1L << (attempt - 1)));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while backing off from typesafe.ai", ie);
        }
    }

    private static String readAll(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
