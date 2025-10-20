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
package org.apache.calcite.mcp.metadata;

/**
 * Enumeration of vector column relationship patterns.
 *
 * <p>These patterns describe how vector columns relate to source data:
 * <ul>
 *   <li>CO_LOCATED: Vector column lives directly in the source table</li>
 *   <li>FK_FORMAL: Separate vector table with formal FK constraint</li>
 *   <li>FK_LOGICAL: Logical reference via metadata (no FK constraint)</li>
 *   <li>MULTI_SOURCE: Union table with embedded source tracking (polymorphic)</li>
 * </ul>
 */
public enum VectorPattern {
  /** Pattern 1: Vector in same table as source data */
  CO_LOCATED,

  /** Pattern 2a: Formal FK constraint to source table */
  FK_FORMAL,

  /** Pattern 2b: Logical reference via metadata (no FK) */
  FK_LOGICAL,

  /** Pattern 3: Polymorphic source tracking (multiple source tables) */
  MULTI_SOURCE
}
