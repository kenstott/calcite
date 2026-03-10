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
package org.apache.calcite.adapter.file.etl.cache;

/**
 * Represents a single entry in a raw cache bundle index.
 *
 * <p>Entries are either <em>bundled</em> (packed into a .bin file at a known
 * byte offset) or <em>individual objects</em> (stored as separate S3 objects
 * for large files like shapefiles).
 */
public class BundleEntry {

  private final String bundleFile;
  private final long offset;
  private final long length;
  private final long timestamp;
  private final boolean individualObject;

  private BundleEntry(String bundleFile, long offset, long length,
      long timestamp, boolean individualObject) {
    this.bundleFile = bundleFile;
    this.offset = offset;
    this.length = length;
    this.timestamp = timestamp;
    this.individualObject = individualObject;
  }

  /** Creates a bundled entry (packed into a .bin file). */
  public static BundleEntry bundled(String bundleFile, long offset, long length,
      long timestamp) {
    return new BundleEntry(bundleFile, offset, length, timestamp, false);
  }

  /** Creates an individual object entry (stored as a separate S3 object). */
  public static BundleEntry individual(long length, long timestamp) {
    return new BundleEntry(null, -1, length, timestamp, true);
  }

  /** Bundle filename (e.g., "run-20260310T1423.bin"). Null for individual objects. */
  public String getBundleFile() {
    return bundleFile;
  }

  /** Byte offset within the bundle. -1 for individual objects. */
  public long getOffset() {
    return offset;
  }

  /** Byte length of this entry. */
  public long getLength() {
    return length;
  }

  /** Epoch seconds when this entry was cached. */
  public long getTimestamp() {
    return timestamp;
  }

  /** True if stored as a separate S3 object (large files). */
  public boolean isIndividualObject() {
    return individualObject;
  }

  /** True if packed into a bundle .bin file. */
  public boolean isBundled() {
    return !individualObject;
  }
}
