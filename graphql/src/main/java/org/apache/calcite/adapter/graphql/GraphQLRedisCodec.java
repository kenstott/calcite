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
package org.apache.calcite.adapter.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.lettuce.core.codec.RedisCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import graphql.ExecutionResult;

/**
 * Redis codec for GraphQL ExecutionResult objects.
 * Handles serialization and deserialization of GraphQL query results for Redis caching.
 */
public class GraphQLRedisCodec implements RedisCodec<String, ExecutionResult> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override public String decodeKey(ByteBuffer bytes) {
    return StandardCharsets.UTF_8.decode(bytes).toString();
  }

  @Override public ExecutionResult decodeValue(ByteBuffer bytes) {
    try {
      byte[] array = new byte[bytes.remaining()];
      bytes.get(array);
      return objectMapper.readValue(array, ExecutionResult.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode ExecutionResult", e);
    }
  }

  @Override public ByteBuffer encodeKey(String key) {
    return StandardCharsets.UTF_8.encode(key);
  }

  @Override public ByteBuffer encodeValue(ExecutionResult value) {
    try {
      byte[] jsonBytes = objectMapper.writeValueAsBytes(value);
      return ByteBuffer.wrap(jsonBytes);
    } catch (IOException e) {
      throw new RuntimeException("Failed to encode ExecutionResult", e);
    }
  }
}
