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
package org.apache.calcite.adapter.file.similarity;
// storage-provider-guard:ignore-file - audited: every filesystem operation here targets a
// genuinely-local artifact — the ONNX model and tokenizer extracted from the jar into a local
// cache dir, and the temp file used to make that extraction atomic. None of it is object-store
// data; ONNX Runtime loads the model by local path and cannot read a StorageProvider stream.

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * In-JVM query-time embedder: ONNX Runtime inference plus CLS pooling, no subprocess.
 *
 * <h2>Why CLS pooling specifically</h2>
 *
 * <p>The corpus is embedded by {@code govdata/scripts/vss-local.py} with
 * {@code SentenceTransformer("Snowflake/snowflake-arctic-embed-xs")}, and that model's
 * {@code 1_Pooling/config.json} declares {@code pooling_mode_cls_token: true}. Query vectors must
 * be produced the same way or they land in a different space. This is not theoretical: the
 * previously-attempted Go/hugot embedder mean-pools unconditionally, and measured against the same
 * model it scored cosine <b>0.886</b> against the CLS reference — high enough to return
 * plausible-looking results and wrong enough to mis-rank them, which is the failure mode this
 * class exists to avoid.
 *
 * <p>The ONNX export emits {@code last_hidden_state [batch, sequence, 384]} with no pooling head,
 * so pooling is this class's job: take token 0, then L2-normalize (matching
 * {@code normalize_embeddings=True} on the producer side).
 *
 * <h2>Model resolution</h2>
 *
 * <p>A directory containing {@code model.onnx} and {@code tokenizer.json}, from
 * {@code calcite.embed.model.dir}; otherwise the model bundled on the classpath under
 * {@code /embedder-model/}, extracted once into a cache directory. The bundled model is the int8
 * quantization — 21.9MB against 86MB for fp32, measured at cosine 0.9982-0.9986 versus fp32 CLS,
 * which is well inside the tolerance the retrieval path already has (the stored codes are
 * themselves quantized to binary sign codes plus int8 rerank vectors).
 */
public final class OnnxClsEmbedder implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnnxClsEmbedder.class);

  private static final String MODEL_RESOURCE_DIR = "/models/snowflake-arctic-embed-xs/";
  private static final String MODEL_FILE = "model.onnx";
  private static final String TOKENIZER_FILE = "tokenizer.json";
  private static final int EMBEDDING_DIM = 384;

  private final OrtEnvironment env;
  private final OrtSession session;
  private final BertWordPieceTokenizer tokenizer;
  private final boolean needsTokenTypeIds;

  private OnnxClsEmbedder(OrtEnvironment env, OrtSession session,
      BertWordPieceTokenizer tokenizer, boolean needsTokenTypeIds) {
    this.env = env;
    this.session = session;
    this.tokenizer = tokenizer;
    this.needsTokenTypeIds = needsTokenTypeIds;
  }

  /**
   * Opens the embedder, or returns null when no model is available.
   *
   * <p>Null rather than a throw: an absent model means semantic search is unavailable, which the
   * caller reports as such. What must never happen is a silent substitution of some other model.
   */
  public static OnnxClsEmbedder openOrNull() {
    try {
      File dir = resolveModelDir();
      if (dir == null) {
        LOGGER.debug("No embedder model found; semantic search will report it as unconfigured");
        return null;
      }
      File model = new File(dir, MODEL_FILE);
      File tokenizerJson = new File(dir, TOKENIZER_FILE);
      if (!model.isFile() || !tokenizerJson.isFile()) {
        throw new IllegalStateException("embedder model dir " + dir.getAbsolutePath()
            + " must contain both " + MODEL_FILE + " and " + TOKENIZER_FILE);
      }

      BertWordPieceTokenizer tok;
      try (InputStream in = Files.newInputStream(tokenizerJson.toPath())) {
        tok = BertWordPieceTokenizer.fromJson(in);
      }

      OrtEnvironment env = OrtEnvironment.getEnvironment();
      OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
      // One query at a time on a client device; more threads cost memory and buy nothing.
      opts.setIntraOpNumThreads(Math.min(4, Runtime.getRuntime().availableProcessors()));
      OrtSession session = env.createSession(model.getAbsolutePath(), opts);

      boolean tokenTypes = session.getInputNames().contains("token_type_ids");
      LOGGER.info("Embedder ready: {} ({} inputs, CLS pooling, {}-d)",
          model.getAbsolutePath(), session.getInputNames(), EMBEDDING_DIM);
      return new OnnxClsEmbedder(env, session, tok, tokenTypes);
    } catch (Exception e) {
      // Never fall back to a different pooling or model — a wrong vector space is worse than none.
      LOGGER.warn("Could not open the ONNX embedder: {}", e.toString());
      return null;
    }
  }

  /** Embeds one text into a unit-length CLS-pooled vector. */
  public synchronized double[] embed(String text) throws Exception {
    BertWordPieceTokenizer.Encoding enc = tokenizer.encode(text == null ? "" : text);
    int n = enc.inputIds.length;
    long[] shape = {1, n};

    Map<String, OnnxTensor> inputs = new HashMap<>();
    try {
      inputs.put("input_ids", OnnxTensor.createTensor(env, wrap(enc.inputIds), shape));
      inputs.put("attention_mask", OnnxTensor.createTensor(env, wrap(enc.attentionMask), shape));
      if (needsTokenTypeIds) {
        inputs.put("token_type_ids", OnnxTensor.createTensor(env, wrap(enc.tokenTypeIds), shape));
      }
      try (OrtSession.Result result = session.run(inputs)) {
        Object raw = result.get(0).getValue();
        if (!(raw instanceof float[][][])) {
          throw new IllegalStateException("expected last_hidden_state [batch, seq, dim], got "
              + raw.getClass().getName());
        }
        float[][] tokens = ((float[][][]) raw)[0];
        // CLS pooling: token 0 IS the sentence embedding for this model.
        float[] cls = tokens[0];
        if (cls.length != EMBEDDING_DIM) {
          throw new IllegalStateException(
              "model returned " + cls.length + " dims, expected " + EMBEDDING_DIM);
        }
        double norm = 0.0;
        for (float v : cls) {
          norm += (double) v * v;
        }
        norm = Math.sqrt(norm);
        if (norm == 0.0) {
          throw new IllegalStateException("model returned a zero vector");
        }
        double[] out = new double[EMBEDDING_DIM];
        for (int i = 0; i < EMBEDDING_DIM; i++) {
          out[i] = cls[i] / norm;
        }
        return out;
      }
    } finally {
      for (OnnxTensor t : inputs.values()) {
        t.close();
      }
    }
  }

  private static java.nio.LongBuffer wrap(long[] values) {
    java.nio.LongBuffer buf = java.nio.LongBuffer.allocate(values.length);
    buf.put(values);
    buf.rewind();
    return buf;
  }

  /**
   * An explicit {@code calcite.embed.model.dir}, else the classpath-bundled model extracted into
   * a cache dir. Extraction is idempotent and keyed by size, so a jar upgrade that ships a
   * different model replaces the cached copy instead of serving the old one.
   */
  private static File resolveModelDir() throws IOException {
    String configured = System.getProperty("calcite.embed.model.dir", "");
    if (!configured.isEmpty()) {
      return new File(configured);
    }
    if (OnnxClsEmbedder.class.getResource(MODEL_RESOURCE_DIR + MODEL_FILE) == null) {
      return null;
    }
    Path cache = Path.of(System.getProperty("user.home"), ".aperio", "embedder-model");
    Files.createDirectories(cache);
    extractIfStale(MODEL_FILE, cache);
    extractIfStale(TOKENIZER_FILE, cache);
    return cache.toFile();
  }

  private static void extractIfStale(String name, Path cacheDir) throws IOException {
    Path target = cacheDir.resolve(name);
    try (InputStream in = OnnxClsEmbedder.class.getResourceAsStream(MODEL_RESOURCE_DIR + name)) {
      if (in == null) {
        throw new IOException("bundled embedder resource missing: " + MODEL_RESOURCE_DIR + name);
      }
      if (Files.exists(target)) {
        try (InputStream probe =
                 OnnxClsEmbedder.class.getResourceAsStream(MODEL_RESOURCE_DIR + name)) {
          if (probe != null && Files.size(target) == countBytes(probe)) {
            return;
          }
        }
      }
      Path tmp = Files.createTempFile(cacheDir, name, ".tmp");
      Files.copy(in, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      LOGGER.info("Extracted bundled embedder resource {} -> {}", name, target);
    }
  }

  private static long countBytes(InputStream in) throws IOException {
    byte[] buf = new byte[1 << 16];
    long total = 0;
    int n;
    while ((n = in.read(buf)) != -1) {
      total += n;
    }
    return total;
  }

  @Override public void close() throws Exception {
    session.close();
  }
}
