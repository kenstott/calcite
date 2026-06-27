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
package org.apache.calcite.adapter.file.similarity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * Bridge to the airgapped, vendored Python embedding server (`embed.py serve`).
 *
 * <p>Chosen over ONNX-in-Java (too finicky) for stability: a single long-lived
 * Python subprocess loads the model once and answers one JSON request per line on
 * stdin with one JSON response per line on stdout. Because it's the <em>same</em>
 * {@code embed.py} used by ETL, query vectors live in the stored vectors' space —
 * no model drift.
 *
 * <p>Configuration (system properties, set by the launcher — not env, so the
 * model-operand guard is satisfied):
 * <ul>
 *   <li>{@code calcite.embed.python} — Python executable (default {@code python3})</li>
 *   <li>{@code calcite.embed.script} — absolute path to {@code embed.py} (required)</li>
 * </ul>
 *
 * <p>The process is started lazily and restarted if it dies. {@link #embed} is
 * synchronized — one in-flight request at a time, which is fine for query-time use
 * (a query embeds a handful of short strings, not a per-row stream).
 */
public final class EmbeddingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingService.class);

  private static final EmbeddingService INSTANCE = new EmbeddingService();

  private final String python = System.getProperty("calcite.embed.python", "python3");
  private final String script = System.getProperty("calcite.embed.script", "");
  // A self-contained embedding-server executable (e.g. the hugot Go binary) that
  // speaks the same JSON-per-line protocol. Preferred over python+script when set.
  private final String command = System.getProperty("calcite.embed.command", "");
  // Self-contained embedder "home" directory laid out as bin/hugot-embed, lib/
  // (libonnxruntime.so), model/. This is what the jar extracts its bundled
  // resources into at startup; EmbeddingService then sets the child's env from it.
  private final String home = System.getProperty("calcite.embed.home", "");

  private Process proc;
  private BufferedWriter toProc;
  private BufferedReader fromProc;

  private EmbeddingService() {
  }

  /** Returns the process-wide singleton. */
  public static EmbeddingService get() {
    return INSTANCE;
  }

  private void ensureStarted() throws Exception {
    if (proc != null && proc.isAlive()) {
      return;
    }
    ProcessBuilder pb;
    if (command != null && !command.isEmpty()) {
      // Explicit server binary; env (ORT_LIB_PATH, EMBED_MODEL_PATH,
      // LD_LIBRARY_PATH) inherited from this JVM process.
      java.util.List<String> argv = new java.util.ArrayList<String>();
      for (String part : command.trim().split("\\s+")) {
        if (!part.isEmpty()) {
          argv.add(part);
        }
      }
      pb = new ProcessBuilder(argv);
    } else {
      // Self-contained bundle home: an explicit calcite.embed.home, or the
      // embedder bundled in the jar (extracted to a cache dir at startup).
      String h = (home != null && !home.isEmpty()) ? home : resolveBundledHome();
      if (h != null) {
        pb = processForHome(h);
      } else if (script != null && !script.isEmpty()) {
        pb = new ProcessBuilder(python, script, "serve");
      } else {
        throw new IllegalStateException(
            "no embedder configured: set calcite.embed.command, calcite.embed.home, "
            + "bundle embedder-bundle.zip, or set calcite.embed.script");
      }
    }
    pb.redirectErrorStream(false);
    proc = pb.start();
    toProc = new BufferedWriter(
        new OutputStreamWriter(proc.getOutputStream(), StandardCharsets.UTF_8));
    fromProc = new BufferedReader(
        new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));
    // Drain stderr (model-load progress, "serve ready", errors) so it can't block.
    final BufferedReader err = new BufferedReader(
        new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8));
    Thread drain = new Thread(new Runnable() {
      @Override public void run() {
        try {
          String line;
          while ((line = err.readLine()) != null) {
            LOGGER.debug("embed.py: {}", line);
          }
        } catch (Exception ignored) {
          // process ended
        }
      }
    }, "embed-py-stderr");
    drain.setDaemon(true);
    drain.start();
    LOGGER.info("Started embedding server: {}", pb.command());
  }

  /** Builds a process for a bundle home laid out as bin/hugot-embed, lib/, model/,
   *  deriving the child's ORT_LIB_PATH / EMBED_MODEL_PATH / LD_LIBRARY_PATH from it. */
  private static ProcessBuilder processForHome(String home) {
    java.io.File h = new java.io.File(home);
    String lib = new java.io.File(h, "lib").getAbsolutePath();
    ProcessBuilder pb =
        new ProcessBuilder(new java.io.File(h, "bin/hugot-embed").getAbsolutePath());
    java.util.Map<String, String> env = pb.environment();
    env.put("ORT_LIB_PATH", lib);
    env.put("EMBED_MODEL_PATH", new java.io.File(h, "model").getAbsolutePath());
    String prevLd = env.get("LD_LIBRARY_PATH");
    env.put("LD_LIBRARY_PATH",
        prevLd == null || prevLd.isEmpty() ? lib : lib + java.io.File.pathSeparator + prevLd);
    return pb;
  }

  /**
   * Extracts the embedder bundled in the jar (classpath resource
   * {@code /embedder-bundle.zip}, added at package time: bin/hugot-embed, lib/,
   * model/) into a cache directory and returns it as a home dir — making the
   * query-time embedder fully self-contained in the single jar. Idempotent; returns
   * null if no bundle is present (callers then fall back to embed.py).
   */
  private static String resolveBundledHome() {
    java.io.InputStream in =
        EmbeddingService.class.getResourceAsStream("/embedder-bundle.zip");
    if (in == null) {
      return null;
    }
    try {
      java.io.File cache = new java.io.File(
          System.getProperty("java.io.tmpdir"), "calcite-embedder");
      java.io.File marker = new java.io.File(cache, "bin/hugot-embed");
      if (!marker.exists()) {
        cache.mkdirs();
        try (java.util.zip.ZipInputStream zis = new java.util.zip.ZipInputStream(in)) {
          java.util.zip.ZipEntry e;
          byte[] buf = new byte[1 << 16];
          while ((e = zis.getNextEntry()) != null) {
            java.io.File out = new java.io.File(cache, e.getName());
            if (e.isDirectory()) {
              out.mkdirs();
            } else {
              java.io.File parent = out.getParentFile();
              if (parent != null) {
                parent.mkdirs();
              }
              try (java.io.OutputStream os = new java.io.FileOutputStream(out)) {
                int n;
                while ((n = zis.read(buf)) > 0) {
                  os.write(buf, 0, n);
                }
              }
            }
          }
        }
        marker.setExecutable(true, false);
      }
      return cache.getAbsolutePath();
    } catch (Exception ex) {
      LOGGER.warn("Failed to extract bundled embedder: {}", ex.getMessage());
      return null;
    } finally {
      try {
        in.close();
      } catch (Exception ignored) {
        // ignore
      }
    }
  }

  /**
   * Embeds the text with the standard model. The first call blocks while the
   * server loads the model. Throws on any failure (bad config, server error,
   * dimension mismatch) — callers decide how to surface it.
   *
   * @param text input text (may contain any characters; JSON-escaped on the wire)
   * @return the embedding vector
   */
  public synchronized double[] embed(String text) {
    try {
      ensureStarted();
      toProc.write("{\"text\":" + jsonString(text == null ? "" : text) + "}\n");
      toProc.flush();
      String resp = fromProc.readLine();
      if (resp == null) {
        proc = null;
        throw new IllegalStateException("embedding server closed the connection");
      }
      if (resp.contains("\"error\"")) {
        throw new RuntimeException("embedding server error: " + resp);
      }
      return parseEmbedding(resp);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      proc = null;
      throw new RuntimeException("embedding failed: " + e.getMessage(), e);
    }
  }

  private static double[] parseEmbedding(String json) {
    int lb = json.indexOf('[');
    int rb = json.lastIndexOf(']');
    if (lb < 0 || rb <= lb) {
      throw new RuntimeException("malformed embedding response: " + json);
    }
    String body = json.substring(lb + 1, rb).trim();
    if (body.isEmpty()) {
      return new double[0];
    }
    String[] parts = body.split(",");
    double[] v = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      v[i] = Double.parseDouble(parts[i].trim());
    }
    return v;
  }

  private static String jsonString(String s) {
    StringBuilder b = new StringBuilder(s.length() + 2);
    b.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
      case '"':
        b.append("\\\"");
        break;
      case '\\':
        b.append("\\\\");
        break;
      case '\n':
        b.append("\\n");
        break;
      case '\r':
        b.append("\\r");
        break;
      case '\t':
        b.append("\\t");
        break;
      default:
        if (c < 0x20) {
          b.append(String.format("\\u%04x", (int) c));
        } else {
          b.append(c);
        }
      }
    }
    b.append('"');
    return b.toString();
  }
}
