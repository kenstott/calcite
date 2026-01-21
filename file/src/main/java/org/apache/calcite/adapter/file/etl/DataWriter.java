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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Functional interface for writing data from the ETL pipeline.
 *
 * <p>Implementations can write data to any destination: databases, message queues,
 * custom file formats, streaming systems, etc.
 *
 * <p>This interface is used by {@link EtlPipeline} to write data for each
 * batch (dimension combination). If a custom DataWriter is supplied and
 * returns a non-negative value, the built-in MaterializationWriter is skipped.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * // Custom Kafka data writer
 * DataWriter kafkaWriter = (config, data, variables) -> {
 *     long count = 0;
 *     while (data.hasNext()) {
 *         Map<String, Object> record = data.next();
 *         kafkaProducer.send(new ProducerRecord<>(topic, record));
 *         count++;
 *     }
 *     return count;
 * };
 *
 * EtlPipeline pipeline = new EtlPipeline(config, materializeDir, null, kafkaWriter);
 * }</pre>
 *
 * @see EtlPipeline
 * @see TableLifecycleListener#writeData
 */
@FunctionalInterface
public interface DataWriter {

  /**
   * Writes data for a batch.
   *
   * @param config Pipeline configuration
   * @param data Iterator of records to write
   * @param variables Dimension values for this batch
   * @return Number of rows written, or -1 to use default MaterializationWriter
   * @throws IOException If data writing fails
   */
  long write(EtlPipelineConfig config, Iterator<Map<String, Object>> data,
      Map<String, String> variables) throws IOException;

  /**
   * Default writer that returns -1, indicating built-in MaterializationWriter should be used.
   */
  DataWriter DEFAULT = (config, data, variables) -> -1;
}
