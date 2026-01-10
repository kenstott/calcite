# Plan: Kafka Source Support for File Adapter

## Overview

Add Kafka as a streaming data source for the file adapter ETL pipeline, enabling continuous ingestion from Kafka topics into Parquet/Iceberg tables.

**Goal:** Declaratively consume Kafka topics and materialize to queryable tables with exactly-once semantics.

## Architecture Fit

Kafka is another source type within `partitionedTables`. The name refers to the **output** (hive-partitioned Parquet/Iceberg), not the input source.

```yaml
partitionedTables:           # Output: partitioned tables
  - name: events
    source:
      type: kafka            # Input: Kafka (or http, file, postgres, splunk, etc.)
    materialize:
      format: iceberg
      partition: [date]
```

### Generic Streaming Source Pattern

Kafka follows the same pattern as any streaming source with a "since" mechanism:

| Source | Sync Mechanism | Sync Field |
|--------|----------------|------------|
| Kafka | Consumer offset | partition:offset |
| Splunk | Timestamp | `_time` |
| PostgreSQL | Updated timestamp | `updated_at` |
| MongoDB | ObjectId / oplog | `_id` |
| REST API | Cursor | `next_cursor` |

**Common abstraction:**
1. Track last sync point (offset, timestamp, cursor)
2. Query for records since sync point
3. Materialize to Iceberg
4. Update sync point after successful commit

This plan implements Kafka first, but the sync tracking infrastructure is reusable for other streaming sources.

## Kafka Concepts

### KStream vs KTable

| Concept | Semantics | Use Case | Iceberg Mode |
|---------|-----------|----------|--------------|
| **KStream** | Append-only (INSERT) | Events, logs, transactions | Append |
| **KTable** | Changelog (UPSERT) | Entity state, latest value per key | Merge-on-read or Copy-on-write |

**KStream Example:** Credit card transactions - every transaction is a new row.

**KTable Example:** User profiles - only latest profile per user_id matters.

Reference: [Kafka Streams Concepts](https://docs.confluent.io/platform/current/streams/concepts.html)

### Message Formats

| Format | Schema | Registry Required |
|--------|--------|-------------------|
| JSON | Schemaless or JSON Schema | Optional |
| Avro | Embedded or Registry | Recommended |
| Protobuf | .proto files or Registry | Recommended |
| String | Plain text | No |

---

## Target YAML Configuration

```yaml
partitionedTables:
  - name: user_events
    source:
      type: kafka
      brokers: "kafka-1:9092,kafka-2:9092"
      topic: "user-events"
      consumerGroup: "file-adapter-user-events"

      # Message format
      format: avro                    # json, avro, protobuf, string
      schemaRegistry: "http://schema-registry:8081"

      # Consumption mode
      mode: kstream                   # kstream (append) or ktable (upsert)
      keyField: user_id               # For ktable mode: merge key

      # Starting position
      startOffset: earliest           # earliest, latest, or timestamp
      # startTimestamp: "2024-01-01T00:00:00Z"  # If startOffset=timestamp

      # Batching
      batchSize: 10000                # Records per file
      batchWindow: "5 minutes"        # Or time-based batching
      maxPollRecords: 500             # Kafka consumer config

    materialize:
      format: iceberg
      partition: [date, hour]

    # Incremental tracking (Kafka offset-based)
    incremental:
      mode: streaming
      commitAfter: iceberg            # Commit Kafka offset after Iceberg commit
```

---

## Phase 1: Basic Kafka Consumer

**Scope:** Consume JSON messages from Kafka, write to Parquet.

### 1.1 KafkaSource Implementation

- [ ] Create `KafkaSource` implementing `DataSource` interface
- [ ] Create `KafkaSourceConfig` for YAML parsing
- [ ] Basic consumer: poll, batch, return iterator

```java
public class KafkaSource implements DataSource {
  @Override
  public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
    // 1. Create consumer
    // 2. Poll records up to batchSize or batchWindow
    // 3. Deserialize to Map<String, Object>
    // 4. Return iterator
  }
}
```

### 1.2 Consumer Configuration

- [ ] `bootstrap.servers` from config
- [ ] `group.id` from config
- [ ] `auto.offset.reset` = earliest/latest
- [ ] `enable.auto.commit` = false (manual commit after Iceberg)
- [ ] `max.poll.records` from config

### 1.3 JSON Deserialization

- [ ] Parse JSON messages to `Map<String, Object>`
- [ ] Handle nested objects (flatten or keep nested)
- [ ] Handle arrays

### 1.4 Files to Create

| File | Purpose |
|------|---------|
| `KafkaSource.java` | DataSource implementation |
| `KafkaSourceConfig.java` | Configuration parsing |
| `KafkaConsumerFactory.java` | Consumer creation |

### 1.5 Dependencies

```gradle
implementation 'org.apache.kafka:kafka-clients:3.6.0'
```

---

## Phase 2: Sync Point Management & Exactly-Once

**Scope:** Track sync points (Kafka offsets), commit after Iceberg commit.

> **Note:** This phase builds generic sync tracking infrastructure reusable by other streaming sources (Splunk, databases, APIs).

### 2.1 Generic SyncPointStore

- [ ] Create `SyncPointStore` interface (not Kafka-specific)
- [ ] Store sync points in `.aperio/{schema}/sync_points.parquet`
- [ ] Schema: `source_name, sync_key, sync_value, committed_at`
- [ ] Query before consuming to resume from last position

```sql
-- Example: Kafka sync point
SELECT sync_value FROM sync_points
WHERE source_name = 'user_events' AND sync_key = 'partition:0'
-- Returns: '12345' (offset)

-- Example: Splunk sync point
SELECT sync_value FROM sync_points
WHERE source_name = 'splunk_logs' AND sync_key = 'timestamp'
-- Returns: '2024-01-15T10:00:00Z'
```

### 2.2 Kafka-Specific: Offset Tracking

- [ ] Store per-partition offsets: `sync_key = 'partition:{n}'`
- [ ] On startup: query sync points, seek consumer to offsets
- [ ] Handles partition reassignment

### 2.2 Commit Flow

```
1. Poll messages from Kafka
2. Transform and batch
3. Write to Iceberg (atomic commit)
4. IF Iceberg commit succeeds:
   - Store offsets to .aperio
   - Commit offsets to Kafka (optional, for monitoring)
5. IF Iceberg commit fails:
   - Don't commit offsets
   - Next poll resumes from last committed offset
```

### 2.3 Consumer Group Coordination

- [ ] Use Kafka consumer groups for partition assignment
- [ ] Multiple file adapter instances = parallel consumption
- [ ] Rebalance handling: commit offsets before rebalance

### 2.4 Files to Modify

| File | Change |
|------|--------|
| `KafkaSource.java` | Add offset tracking |
| `KafkaOffsetStore.java` | New: persist offsets to Parquet |
| `EtlPipeline.java` | Integrate offset commit after materialize |

---

## Phase 3: Schema Registry & Avro/Protobuf

**Scope:** Support Avro and Protobuf with Schema Registry.

### 3.1 Schema Registry Client

- [ ] Connect to Confluent Schema Registry
- [ ] Fetch schema by subject (topic-value, topic-key)
- [ ] Cache schemas locally

### 3.2 Avro Deserialization

- [ ] Use `KafkaAvroDeserializer`
- [ ] Convert Avro `GenericRecord` to `Map<String, Object>`
- [ ] Handle schema evolution (reader vs writer schema)

### 3.3 Protobuf Deserialization

- [ ] Use `KafkaProtobufDeserializer`
- [ ] Convert Protobuf `Message` to `Map<String, Object>`

### 3.4 Schema-to-Column Mapping

- [ ] Infer Parquet schema from Avro/Protobuf schema
- [ ] Map Avro types to Parquet types
- [ ] Handle logical types (timestamp, decimal, etc.)

### 3.5 Dependencies

```gradle
implementation 'io.confluent:kafka-avro-serializer:7.5.0'
implementation 'io.confluent:kafka-protobuf-serializer:7.5.0'
implementation 'io.confluent:kafka-schema-registry-client:7.5.0'
```

### 3.6 Files to Create

| File | Purpose |
|------|---------|
| `SchemaRegistryClient.java` | Schema Registry integration |
| `AvroRecordConverter.java` | Avro → Map conversion |
| `ProtobufRecordConverter.java` | Protobuf → Map conversion |

---

## Phase 4: KTable Support (Upsert Mode)

**Scope:** Support changelog semantics with merge-by-key.

### 4.1 KTable vs KStream Config

```yaml
source:
  type: kafka
  mode: ktable           # vs kstream
  keyField: user_id      # Merge key
```

### 4.2 Upsert Strategies

| Strategy | Description | Iceberg Support |
|----------|-------------|-----------------|
| **Merge-on-read** | Keep all versions, dedupe at query time | Yes (v2) |
| **Copy-on-write** | Rewrite files with merged data | Yes |
| **Delete+Insert** | Delete old, insert new | Yes |

### 4.3 Implementation Options

**Option A: Iceberg MERGE INTO**
```sql
MERGE INTO target USING source
ON target.key = source.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Option B: Append + Dedupe View**
- Append all records with `_kafka_offset` column
- Create view that selects latest per key
- Periodically compact

### 4.4 Tombstone Handling

- [ ] KTable tombstone = null value = DELETE
- [ ] Map to Iceberg delete operation
- [ ] Or: append with `_deleted = true` marker

### 4.5 Files to Modify

| File | Change |
|------|--------|
| `KafkaSource.java` | Add ktable mode |
| `IcebergMaterializationWriter.java` | Add merge/upsert support |
| `MaterializeConfig.java` | Add merge strategy config |

---

## Phase 5: Windowing & Time-Based Partitioning

**Scope:** Partition by event time, handle late arrivals.

### 5.1 Event Time Extraction

```yaml
source:
  type: kafka
  eventTimeField: timestamp      # Field in message
  eventTimeFormat: epoch_millis  # or ISO8601
```

### 5.2 Partition by Event Time

```yaml
materialize:
  partition: [date, hour]        # Derived from event time
```

### 5.3 Late Arrival Handling

- [ ] Allow writes to historical partitions
- [ ] Configurable lateness threshold
- [ ] Optionally route late events to separate table

### 5.4 Watermark Tracking

- [ ] Track max event time seen
- [ ] Use for triggering compaction
- [ ] Expose as metric

---

## Phase 6: Monitoring & Observability

### 6.1 Metrics

| Metric | Description |
|--------|-------------|
| `kafka_records_consumed` | Total records consumed |
| `kafka_lag` | Consumer lag per partition |
| `kafka_batch_size` | Records per batch |
| `iceberg_commits` | Successful Iceberg commits |
| `iceberg_commit_latency` | Time to commit |

### 6.2 Health Checks

- [ ] Consumer connected to brokers
- [ ] Schema Registry reachable
- [ ] Lag below threshold

### 6.3 Error Handling

| Error | Action |
|-------|--------|
| Broker unavailable | Retry with backoff |
| Deserialization error | Dead-letter queue or skip |
| Iceberg commit failure | Retry, don't commit offset |
| Schema not found | Fail or use default |

---

## Phase 7: Testing

### 7.1 Unit Tests

- [ ] `KafkaSourceConfigTest` - YAML parsing
- [ ] `KafkaSourceTest` - Mock consumer
- [ ] `AvroRecordConverterTest` - Type mapping

### 7.2 Integration Tests

- [ ] Embedded Kafka (Testcontainers)
- [ ] End-to-end: produce → consume → Iceberg
- [ ] Offset recovery after restart
- [ ] Consumer group rebalance

### 7.3 Performance Tests

- [ ] Throughput: records/second
- [ ] Latency: produce to queryable
- [ ] Backpressure handling

---

## Summary: KStream vs KTable Support

| Feature | KStream | KTable |
|---------|---------|--------|
| Semantics | Append (INSERT) | Upsert (INSERT/UPDATE/DELETE) |
| Key required | No | Yes |
| Iceberg operation | Append | Merge or Append+Dedupe |
| Log compaction safe | No | Yes |
| Use case | Events, logs | Entity state |
| Complexity | Lower | Higher |

**Recommendation:** Start with KStream (Phase 1-3), add KTable (Phase 4) later.

---

## Dependencies Summary

```gradle
// Kafka
implementation 'org.apache.kafka:kafka-clients:3.6.0'

// Schema Registry (optional)
implementation 'io.confluent:kafka-avro-serializer:7.5.0'
implementation 'io.confluent:kafka-protobuf-serializer:7.5.0'
implementation 'io.confluent:kafka-schema-registry-client:7.5.0'

// Testing
testImplementation 'org.testcontainers:kafka:1.19.0'
```

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Consumer lag accumulation | Monitor lag, scale consumers |
| Schema evolution breaks | Use compatibility checks in Registry |
| Exactly-once complexity | Test offset recovery thoroughly |
| KTable merge performance | Use Iceberg v2 merge-on-read |
| Dependency bloat | Make Schema Registry optional |

---

## Success Criteria

- [ ] Consume JSON from Kafka topic → Iceberg table
- [ ] Resume from last offset after restart
- [ ] Support Avro with Schema Registry
- [ ] KTable upsert mode works
- [ ] Multiple instances consume in parallel
- [ ] Lag < 1 minute under normal load

---

## References

- [Kafka Streams Concepts - Confluent](https://docs.confluent.io/platform/current/streams/concepts.html)
- [KStream vs KTable - Medium](https://medium.com/@kamini.velvet/kstream-vs-ktable-d36b3d4b10ea)
- [When to choose KTable or KStream](https://danlebrero.com/2018/10/08/when-to-choose-ktable-or-kstream-in-kafka-streams/)
- [Iceberg MERGE INTO](https://iceberg.apache.org/docs/latest/spark-writes/#merge-into)
