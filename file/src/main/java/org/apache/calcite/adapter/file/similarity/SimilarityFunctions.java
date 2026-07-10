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

import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;

/**
 * SQL functions for text similarity and vector operations.
 * These functions can be used in SQL queries to find similar documents
 * or text chunks based on their embeddings.
 *
 * All functions work with vectors represented as comma-separated strings
 * for compatibility with standard SQL types.
 */
public class SimilarityFunctions {

  /**
   * Computes cosine similarity between two vectors.
   * Handles various input types including:
   * - String representations (comma-separated values or DuckDB array format)
   * - Native arrays (float[], double[], etc.)
   * - Collections ({@code List<Float>}, {@code List<Double>}, etc.)
   * - Avro arrays (GenericData.Array)
   * - Any object with a string representation of a vector
   *
   * Returns a value between -1 and 1, where 1 means identical direction,
   * 0 means orthogonal, and -1 means opposite direction.
   *
   * @param vector1 First vector (any supported format)
   * @param vector2 Second vector (any supported format)
   * @return Cosine similarity score
   */
  public static double cosineSimilarity(Object vector1, Object vector2) {
    if (vector1 == null || vector2 == null) {
      return 0.0;
    }

    double[] v1 = extractFloatArray(vector1);
    double[] v2 = extractFloatArray(vector2);

    return computeCosineSimilarity(v1, v2);
  }

  /**
   * Cosine similarity for String arguments (called by Calcite for CHARACTER columns).
   *
   * @param vector1 First vector as string
   * @param vector2 Second vector as string
   * @return Cosine similarity score
   */
  public static double cosineSimilarity(String vector1, String vector2) {
    return cosineSimilarity((Object) vector1, (Object) vector2);
  }

  /**
   * Cosine similarity between two raw texts, each embedded with the standard model
   * via the vendored Python embedding server ({@link EmbeddingService}). Both
   * arguments are {@code String}, so the function validates cleanly (unlike the
   * array-typed cosine, which Calcite's reflective UDFs can't accept).
   *
   * <p>This is the end-to-end smoke test for the embedding server, and a useful
   * ad-hoc "are these two texts alike?" primitive. It is <em>not</em> scalable —
   * it embeds both sides on every call and cannot push down. For column search,
   * embed the query once and score against the pre-computed vectors in DuckDB.
   */
  public static double semanticSimilarity(String text1, String text2) {
    if (text1 == null || text2 == null) {
      return 0.0;
    }
    double[] v1 = EmbeddingService.get().embed(text1);
    double[] v2 = EmbeddingService.get().embed(text2);
    return computeCosineSimilarity(v1, v2);
  }

  /**
   * Embeds the text with the standard model and returns the vector as a
   * comma-separated {@code VARCHAR}. Returning a string (rather than an array)
   * is deliberate: Calcite's reflective UDFs reject {@code ARRAY} operands, but
   * accept {@code VARCHAR}, so this composes directly with {@link #cosineSimilarity}
   * and with a stored embedding column cast to text, e.g.
   * <pre>COSINE_SIMILARITY(EMBED('query text'), CAST(embedding AS VARCHAR))</pre>
   */
  public static String embedText(String text) {
    if (text == null) {
      return null;
    }
    double[] v = EmbeddingService.get().embed(text);
    StringBuilder sb = new StringBuilder(v.length * 12);
    for (int i = 0; i < v.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(v[i]);
    }
    return sb.toString();
  }

  /**
   * Core cosine similarity computation.
   */
  private static double computeCosineSimilarity(double[] v1, double[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;

    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }

    if (norm1 == 0.0 || norm2 == 0.0) {
      return 0.0;
    }

    return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
  }

  /**
   * Computes cosine distance between two vectors.
   * This is 1 - cosine_similarity, ranging from 0 (identical) to 2 (opposite).
   * Compatible with PostgreSQL's <-> operator behavior.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Cosine distance score
   */
  public static double cosineDistance(String vector1, String vector2) {
    return 1.0 - cosineSimilarity(vector1, vector2);
  }

  /**
   * Computes Euclidean distance between two vectors.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Euclidean distance
   */
  public static double euclideanDistance(String vector1, String vector2) {
    if (vector1 == null || vector2 == null) {
      return Double.MAX_VALUE;
    }

    double[] v1 = parseVector(vector1);
    double[] v2 = parseVector(vector2);

    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double sum = 0.0;
    for (int i = 0; i < v1.length; i++) {
      double diff = v1[i] - v2[i];
      sum += diff * diff;
    }

    return Math.sqrt(sum);
  }

  /**
   * Computes dot product between two vectors.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Dot product
   */
  public static double dotProduct(String vector1, String vector2) {
    if (vector1 == null || vector2 == null) {
      return 0.0;
    }

    double[] v1 = parseVector(vector1);
    double[] v2 = parseVector(vector2);

    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double product = 0.0;
    for (int i = 0; i < v1.length; i++) {
      product += v1[i] * v2[i];
    }

    return product;
  }

  /**
   * Finds whether two vectors are similar based on a threshold.
   * Uses cosine similarity with a default threshold of 0.7.
   *
   * @param vector1 First vector
   * @param vector2 Second vector
   * @param threshold Similarity threshold (default 0.7)
   * @return true if vectors are similar
   */
  public static boolean vectorsSimilar(String vector1, String vector2, Double threshold) {
    double thresh = threshold != null ? threshold : 0.7;
    return cosineSimilarity(vector1, vector2) >= thresh;
  }

  /**
   * Computes the magnitude (L2 norm) of a vector.
   *
   * @param vector Vector as comma-separated string
   * @return Vector magnitude
   */
  public static double vectorNorm(String vector) {
    if (vector == null) {
      return 0.0;
    }

    double[] v = parseVector(vector);
    double sum = 0.0;
    for (double val : v) {
      sum += val * val;
    }

    return Math.sqrt(sum);
  }

  /**
   * Normalizes a vector to unit length.
   *
   * @param vector Vector as comma-separated string
   * @return Normalized vector as comma-separated string
   */
  public static String normalizeVector(String vector) {
    if (vector == null) {
      return null;
    }

    double[] v = parseVector(vector);
    double norm = 0.0;

    for (double val : v) {
      norm += val * val;
    }
    norm = Math.sqrt(norm);

    if (norm == 0.0) {
      return vector;
    }

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < v.length; i++) {
      if (i > 0) {
        result.append(",");
      }
      result.append(v[i] / norm);
    }

    return result.toString();
  }

  /**
   * Compute text similarity between two texts.
   * This function generates embeddings on-the-fly and computes cosine similarity.
   *
   * Uses the default embedding provider (local TF-IDF with financial domain knowledge)
   * to generate semantic embeddings and compute similarity.
   *
   * @param text1 First text
   * @param text2 Second text
   * @return Cosine similarity between text embeddings
   */
  public static double textSimilarity(String text1, String text2) {
    if (text1 == null || text2 == null) {
      return 0.0;
    }

    // Use Jaccard similarity for text comparison
    return computeJaccardSimilarity(text1, text2);
  }

  /**
   * Parses a comma-separated string into a double array.
   */
  private static double[] parseVector(String vector) {
    String[] parts = vector.split(",");
    double[] result = new double[parts.length];

    for (int i = 0; i < parts.length; i++) {
      try {
        result[i] = Double.parseDouble(parts[i].trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid vector format at position " + i + ": " + parts[i]);
      }
    }

    return result;
  }

  /**
   * Extracts a float array from various input types.
   * Supports native arrays, Collections, Avro arrays, and string representations.
   * Also handles DuckDB array formats and JSON-like array strings.
   */
  private static double[] extractFloatArray(Object obj) {
    if (obj == null) {
      return new double[0];
    }

    // Handle string representation (comma-separated values or array notation)
    if (obj instanceof String) {
      String str = (String) obj;
      // Handle DuckDB array format: [1.0, 2.0, 3.0] or similar
      str = str.trim();
      if (str.startsWith("[") && str.endsWith("]")) {
        str = str.substring(1, str.length() - 1);
      }
      // Also handle parentheses format: (1.0, 2.0, 3.0)
      if (str.startsWith("(") && str.endsWith(")")) {
        str = str.substring(1, str.length() - 1);
      }
      return parseVector(str);
    }

    // Handle native arrays
    if (obj instanceof float[]) {
      float[] floats = (float[]) obj;
      double[] result = new double[floats.length];
      for (int i = 0; i < floats.length; i++) {
        result[i] = floats[i];
      }
      return result;
    }

    if (obj instanceof double[]) {
      return (double[]) obj;
    }

    // Handle Collections (List<Float>, List<Double>, etc.)
    if (obj instanceof java.util.List) {
      java.util.List<?> list = (java.util.List<?>) obj;
      double[] result = new double[list.size()];
      for (int i = 0; i < list.size(); i++) {
        Object item = list.get(i);
        if (item instanceof Number) {
          result[i] = ((Number) item).doubleValue();
        } else {
          throw new IllegalArgumentException(
              "List contains non-numeric element at position " + i + ": " + item);
        }
      }
      return result;
    }

    // Handle Avro GenericArray
    if (obj.getClass().getName().contains("GenericData$Array") ||
        obj.getClass().getName().contains("GenericArray")) {
      try {
        // Use reflection to handle Avro arrays without hard dependency
        java.lang.reflect.Method sizeMethod = obj.getClass().getMethod("size");
        java.lang.reflect.Method getMethod = obj.getClass().getMethod("get", int.class);

        int size = (Integer) sizeMethod.invoke(obj);
        double[] result = new double[size];

        for (int i = 0; i < size; i++) {
          Object item = getMethod.invoke(obj, i);
          if (item instanceof Number) {
            result[i] = ((Number) item).doubleValue();
          } else {
            throw new IllegalArgumentException(
                "Array contains non-numeric element at position " + i + ": " + item);
          }
        }
        return result;
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to extract Avro array: " + e.getMessage(), e);
      }
    }

    // Handle other array types through reflection
    if (obj.getClass().isArray()) {
      try {
        int length = java.lang.reflect.Array.getLength(obj);
        double[] result = new double[length];
        for (int i = 0; i < length; i++) {
          Object item = java.lang.reflect.Array.get(obj, i);
          if (item instanceof Number) {
            result[i] = ((Number) item).doubleValue();
          } else {
            throw new IllegalArgumentException(
                "Array contains non-numeric element at position " + i + ": " + item);
          }
        }
        return result;
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to extract array: " + e.getMessage(), e);
      }
    }

    // Last resort - try to parse as string
    try {
      return parseVector(obj.toString());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot convert object of type " + obj.getClass().getName() + " to float array: " + e.getMessage(), e);
    }
  }

  /**
   * Registers all similarity functions with a schema.
   */
  public static void registerFunctions(org.apache.calcite.schema.SchemaPlus schema) {
    // Vector operations
    schema.add("COSINE_SIMILARITY",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "cosineSimilarity"));
    // Text-to-text semantic similarity via the vendored Python embedding server
    // (standard model). (String,String) validates cleanly; exercises the server
    // end to end.
    schema.add("SEMANTIC_SIMILARITY",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "semanticSimilarity"));
    // EMBED(text) -> VARCHAR vector; composes with COSINE_SIMILARITY against a
    // stored embedding column cast to text (avoids ARRAY-typed UDF operands).
    schema.add("EMBED",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "embedText"));
    schema.add("COSINE_DISTANCE",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "cosineDistance"));
    schema.add("EUCLIDEAN_DISTANCE",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "euclideanDistance"));
    schema.add("DOT_PRODUCT",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "dotProduct"));
    schema.add("VECTORS_SIMILAR",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "vectorsSimilar"));
    schema.add("VECTOR_NORM",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "vectorNorm"));
    schema.add("NORMALIZE_VECTOR",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "normalizeVector"));

    // Text operations
    schema.add("TEXT_SIMILARITY",
        ScalarFunctionImpl.create(SimilarityFunctions.class, "textSimilarity"));

    // Path B semantic search over the quantized-code dataset (binary Hamming prefilter +
    // int8 rerank). Table function: SELECT * FROM TABLE(SEMANTIC_SEARCH('query text', 10)).
    schema.add("SEMANTIC_SEARCH",
        TableFunctionImpl.create(SemanticSearch.class, "SEMANTIC_SEARCH"));
  }

  /**
   * Compute simple Jaccard similarity as fallback.
   */
  private static double computeJaccardSimilarity(String text1, String text2) {
    String[] words1 = text1.toLowerCase().split("\\s+");
    String[] words2 = text2.toLowerCase().split("\\s+");

    java.util.Set<String> set1 = new java.util.HashSet<>();
    java.util.Collections.addAll(set1, words1);

    java.util.Set<String> set2 = new java.util.HashSet<>();
    java.util.Collections.addAll(set2, words2);

    java.util.Set<String> intersection = new java.util.HashSet<>(set1);
    intersection.retainAll(set2);

    java.util.Set<String> union = new java.util.HashSet<>(set1);
    union.addAll(set2);

    return union.isEmpty() ? 0.0 : (double) intersection.size() / union.size();
  }

}
