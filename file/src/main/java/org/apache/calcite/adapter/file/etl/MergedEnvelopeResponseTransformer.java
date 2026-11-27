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
package org.apache.calcite.adapter.file.etl;

/**
 * A {@link ResponseTransformer} whose {@link ResponseTransformer#transform} accepts, besides an
 * upstream response body, the {@code {"results":[...]}} envelope {@link HttpSource} writes when it
 * merges the records of a paginated source into one raw cache entry.
 *
 * <p>The records in the envelope are the elements of the upstream record array (the one named by
 * {@code response.dataPath}, or {@code results}, or a bare top-level array). Nothing else from the
 * upstream bodies is kept.
 *
 * <p>A transformer on a paginated, raw-cached JSON source that implements neither this interface
 * nor {@link PerRecordResponseTransformer} has its pages cached verbatim, and receives each of them
 * on replay exactly as it did when they were fetched.
 */
public interface MergedEnvelopeResponseTransformer extends ResponseTransformer {
}
