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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

/**
 * ONNX Runtime-based embedding provider using sentence-transformers models.
 *
 * <p>This provider uses ONNX Runtime to run sentence-transformer models
 * like all-MiniLM-L6-v2 for generating text embeddings. It provides fast,
 * local embedding generation without requiring external APIs.
 *
 * <p>Model files can be loaded from:
 * <ul>
 *   <li>Classpath resources (bundled with application)</li>
 *   <li>Local filesystem paths</li>
 *   <li>Environment variable ONNX_MODEL_PATH</li>
 * </ul>
 */
public class ONNXEmbeddingProvider implements TextEmbeddingProvider {
  private static final Logger logger = LoggerFactory.getLogger(ONNXEmbeddingProvider.class);

  private static final int DEFAULT_EMBEDDING_DIMENSIONS = 384;
  private static final int DEFAULT_MAX_SEQUENCE_LENGTH = 128;
  private static final int DEFAULT_MAX_INPUT_LENGTH = 8192;

  private final OrtEnvironment env;
  private final OrtSession session;
  private final HuggingFaceTokenizer tokenizer;
  private final String modelPath;
  private final int embeddingDimensions;
  private final int maxSequenceLength;
  private final int maxInputLength;

  /**
   * Create ONNX embedding provider with default configuration.
   *
   * @param config Configuration map
   * @throws EmbeddingException if model loading fails
   */
  public ONNXEmbeddingProvider(Map<String, Object> config) throws EmbeddingException {
    if (config == null) {
      config = new HashMap<>();
    }

    this.embeddingDimensions = getIntConfig(config, "dimensions", DEFAULT_EMBEDDING_DIMENSIONS);
    this.maxSequenceLength = getIntConfig(config, "maxSequenceLength", DEFAULT_MAX_SEQUENCE_LENGTH);
    this.maxInputLength = getIntConfig(config, "maxInputLength", DEFAULT_MAX_INPUT_LENGTH);

    // Determine model and tokenizer paths
    this.modelPath = resolveModelPath(config);
    String tokenizerPath = resolveTokenizerPath(config);

    logger.info("Initializing ONNX embedding provider with model: {}", modelPath);

    // Initialize ONNX Runtime environment
    this.env = OrtEnvironment.getEnvironment();

    // Load ONNX model
    try {
      OrtSession.SessionOptions options = new OrtSession.SessionOptions();
      // Use CPU by default - can be configured for GPU
      this.session = env.createSession(modelPath, options);
      logger.info("Successfully loaded ONNX model from: {}", modelPath);
    } catch (OrtException e) {
      throw new EmbeddingException("Failed to load ONNX model from: " + modelPath, e);
    }

    // Load tokenizer
    try {
      this.tokenizer = HuggingFaceTokenizer.newInstance(tokenizerPath);
      logger.info("Successfully loaded tokenizer from: {}", tokenizerPath);
    } catch (Exception e) {
      try {
        session.close();
      } catch (OrtException ex) {
        logger.warn("Failed to close ONNX session during cleanup", ex);
      }
      throw new EmbeddingException("Failed to load tokenizer from: " + tokenizerPath, e);
    }
  }

  @Override public double[] generateEmbedding(String text) throws EmbeddingException {
    if (text == null || text.trim().isEmpty()) {
      return new double[embeddingDimensions];
    }

    // Truncate text if too long
    if (text.length() > maxInputLength) {
      text = text.substring(0, maxInputLength);
    }

    try {
      // Tokenize input
      Encoding encoding = tokenizer.encode(text);
      long[] inputIds = encoding.getIds();
      long[] attentionMask = encoding.getAttentionMask();

      // Truncate or pad to max sequence length
      inputIds = truncateOrPad(inputIds, maxSequenceLength);
      attentionMask = truncateOrPad(attentionMask, maxSequenceLength);

      // Reshape to [1, seq_len] for batch dimension
      long[][] inputIdsBatch = new long[][]{inputIds};
      long[][] attentionMaskBatch = new long[][]{attentionMask};

      // Create ONNX tensors
      Map<String, OnnxTensor> inputs = new HashMap<>();
      inputs.put("input_ids", OnnxTensor.createTensor(env, inputIdsBatch));
      inputs.put("attention_mask", OnnxTensor.createTensor(env, attentionMaskBatch));

      // Run inference
      OrtSession.Result result = session.run(inputs);

      // Get output tensor (last_hidden_state)
      // Shape: [batch_size, sequence_length, hidden_size]
      float[][][] output = (float[][][]) result.get(0).getValue();

      // Close tensors to free memory
      for (OnnxTensor tensor : inputs.values()) {
        tensor.close();
      }
      result.close();

      // Mean pooling over sequence dimension, weighted by attention mask
      double[] embedding = meanPooling(output[0], attentionMask);

      // L2 normalization for cosine similarity
      return normalizeVector(embedding);

    } catch (Exception e) {
      throw new EmbeddingException("Failed to generate embedding for text", e);
    }
  }

  @Override public List<double[]> generateEmbeddings(List<String> texts) throws EmbeddingException {
    if (texts == null || texts.isEmpty()) {
      return new ArrayList<>();
    }

    // For now, process sequentially
    // TODO: Implement true batch processing for better performance
    List<double[]> embeddings = new ArrayList<>(texts.size());
    for (String text : texts) {
      embeddings.add(generateEmbedding(text));
    }

    return embeddings;
  }

  @Override public CompletableFuture<double[]> generateEmbeddingAsync(String text) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbedding(text);
      } catch (EmbeddingException e) {
        throw new RuntimeException("Failed to generate embedding", e);
      }
    });
  }

  @Override public CompletableFuture<List<double[]>> generateEmbeddingsAsync(List<String> texts) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbeddings(texts);
      } catch (EmbeddingException e) {
        throw new RuntimeException("Failed to generate embeddings", e);
      }
    });
  }

  @Override public int getDimensions() {
    return embeddingDimensions;
  }

  @Override public int getMaxInputLength() {
    return maxInputLength;
  }

  @Override public boolean isAvailable() {
    return session != null && tokenizer != null;
  }

  @Override public String getProviderName() {
    return "ONNX Runtime (sentence-transformers)";
  }

  @Override public String getModelId() {
    return "sentence-transformers/all-MiniLM-L6-v2";
  }

  @Override public void close() {
    logger.info("Closing ONNX embedding provider");
    try {
      if (session != null) {
        session.close();
      }
      if (tokenizer != null) {
        tokenizer.close();
      }
    } catch (Exception e) {
      logger.warn("Error closing ONNX resources", e);
    }
  }

  @Override public double getCostPer1000Tokens() {
    return 0.0; // Local model, no API costs
  }

  @Override public boolean supportsBatchProcessing() {
    return true;
  }

  /**
   * Mean pooling over token embeddings, weighted by attention mask.
   */
  private double[] meanPooling(float[][] tokenEmbeddings, long[] attentionMask) {
    int seqLen = tokenEmbeddings.length;
    int hiddenSize = tokenEmbeddings[0].length;

    double[] pooled = new double[hiddenSize];
    double sumMask = 0;

    for (int i = 0; i < seqLen; i++) {
      if (attentionMask[i] == 1) {
        for (int j = 0; j < hiddenSize; j++) {
          pooled[j] += tokenEmbeddings[i][j];
        }
        sumMask += 1.0;
      }
    }

    // Average over non-masked tokens
    if (sumMask > 0) {
      for (int i = 0; i < hiddenSize; i++) {
        pooled[i] /= sumMask;
      }
    }

    return pooled;
  }

  /**
   * L2 normalization of vector for cosine similarity.
   */
  private double[] normalizeVector(double[] vector) {
    double norm = 0.0;
    for (double value : vector) {
      norm += value * value;
    }
    norm = Math.sqrt(norm);

    if (norm > 0) {
      double[] normalized = new double[vector.length];
      for (int i = 0; i < vector.length; i++) {
        normalized[i] = vector[i] / norm;
      }
      return normalized;
    }

    return vector;
  }

  /**
   * Truncate or pad array to target length.
   */
  private long[] truncateOrPad(long[] array, int targetLength) {
    if (array.length == targetLength) {
      return array;
    }

    long[] result = new long[targetLength];
    if (array.length > targetLength) {
      // Truncate
      System.arraycopy(array, 0, result, 0, targetLength);
    } else {
      // Pad with zeros
      System.arraycopy(array, 0, result, 0, array.length);
      // Rest is already zero-initialized
    }
    return result;
  }

  /**
   * Resolve model path from config or environment.
   */
  private String resolveModelPath(Map<String, Object> config) throws EmbeddingException {
    // 1. Check explicit config
    String configPath = (String) config.get("modelPath");
    if (configPath != null && Files.exists(Paths.get(configPath))) {
      return configPath;
    }

    // 2. Check environment variable
    String envPath = System.getenv("ONNX_MODEL_PATH");
    if (envPath != null && Files.exists(Paths.get(envPath))) {
      return envPath;
    }

    // 3. Try to load from classpath resources
    String resourcePath = "/models/all-MiniLM-L6-v2/model.onnx";
    try {
      Path tempModel = extractResourceToTemp(resourcePath, "model", ".onnx");
      if (tempModel != null) {
        return tempModel.toString();
      }
    } catch (IOException e) {
      logger.debug("Failed to extract model from classpath: {}", e.getMessage());
    }

    // 4. Default location in file adapter resources
    String defaultPath = "file/src/main/resources/models/all-MiniLM-L6-v2/model.onnx";
    if (Files.exists(Paths.get(defaultPath))) {
      return defaultPath;
    }

    throw new EmbeddingException(
        "ONNX model file not found. Please set ONNX_MODEL_PATH environment variable " +
        "or place model at: " + defaultPath);
  }

  /**
   * Resolve tokenizer path from config or environment.
   */
  private String resolveTokenizerPath(Map<String, Object> config) throws EmbeddingException {
    // 1. Check explicit config
    String configPath = (String) config.get("tokenizerPath");
    if (configPath != null && Files.exists(Paths.get(configPath))) {
      return configPath;
    }

    // 2. Check environment variable
    String envPath = System.getenv("ONNX_TOKENIZER_PATH");
    if (envPath != null && Files.exists(Paths.get(envPath))) {
      return envPath;
    }

    // 3. Try to load from classpath resources
    String resourcePath = "/models/all-MiniLM-L6-v2/tokenizer.json";
    try {
      Path tempTokenizer = extractResourceToTemp(resourcePath, "tokenizer", ".json");
      if (tempTokenizer != null) {
        return tempTokenizer.toString();
      }
    } catch (IOException e) {
      logger.debug("Failed to extract tokenizer from classpath: {}", e.getMessage());
    }

    // 4. Default location in file adapter resources
    String defaultPath = "file/src/main/resources/models/all-MiniLM-L6-v2/tokenizer.json";
    if (Files.exists(Paths.get(defaultPath))) {
      return defaultPath;
    }

    throw new EmbeddingException(
        "Tokenizer file not found. Please set ONNX_TOKENIZER_PATH environment variable " +
        "or place tokenizer at: " + defaultPath);
  }

  /**
   * Extract a resource from classpath to a temporary file.
   */
  private Path extractResourceToTemp(String resourcePath, String prefix, String suffix)
      throws IOException {
    try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
      if (in == null) {
        return null;
      }

      Path tempFile = Files.createTempFile(prefix, suffix);
      Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
      tempFile.toFile().deleteOnExit();

      return tempFile;
    }
  }

  /**
   * Get integer config value with default.
   */
  private int getIntConfig(Map<String, Object> config, String key, int defaultValue) {
    Object value = config.get(key);
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return defaultValue;
  }
}
