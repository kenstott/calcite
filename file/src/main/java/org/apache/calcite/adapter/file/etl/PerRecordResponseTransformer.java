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
package org.apache.calcite.adapter.file.etl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Extension of {@link ResponseTransformer} for transformers that can augment
 * a single already-parsed record in-place.
 *
 * <p>Implementing this interface enables the streaming raw-cache path in
 * {@code HttpSource}: instead of loading the entire cached file into a String,
 * applying {@link ResponseTransformer#transform}, and building a full
 * {@code List<Map>}, {@code HttpSource} will stream the JSON array one row
 * at a time and call {@link #transformRecord} for each.
 *
 * <p>Transformers that only inject dimension values into each row (the common
 * case) should implement this interface in addition to {@link ResponseTransformer}.
 *
 * <p>Transformers that fan-out (produce zero or many rows per source record —
 * e.g. flattening a parent record into a list of child rows) should override
 * {@link #transformRecordToMany} instead. The default implementation invokes
 * {@link #transformRecord} and returns the mutated single row.
 */
public interface PerRecordResponseTransformer extends ResponseTransformer {

  /**
   * Augments a single already-parsed row in-place.
   *
   * @param row     Mutable row map; modifications are applied in-place
   * @param context Request context including URL, parameters, headers, and dimension values
   */
  void transformRecord(Map<String, Object> row, RequestContext context);

  /**
   * Produces zero or more output rows from one source record. Default implementation
   * calls {@link #transformRecord} for 1:1 transformers.
   *
   * <p>Fan-out transformers (one parent record → many child rows) should override this
   * method, leaving {@link #transformRecord} unimplemented (throw or no-op). The streaming
   * raw-cache reader in {@code HttpSource} invokes this method to drain the cache without
   * materialising the entire response as a {@code String}.
   *
   * @param source  Source record map as parsed from the cached JSON array element
   * @param context Request context including URL, parameters, headers, and dimension values
   * @return List of output rows (empty allowed); the default returns the in-place
   *     mutated {@code source} as a singleton list
   */
  default List<Map<String, Object>> transformRecordToMany(
      Map<String, Object> source, RequestContext context) {
    transformRecord(source, context);
    return Collections.singletonList(source);
  }
}
