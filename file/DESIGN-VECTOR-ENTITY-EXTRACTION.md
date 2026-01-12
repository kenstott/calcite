# Vector-Based Entity Extraction - Design Document

## Overview

This document describes a system for extracting entities from vectorized text columns across multiple tables. Given that embeddings are generated using the same pre-trained model, vectors exist in a shared semantic space enabling cross-table entity resolution.

## Goals

1. **Manual Corpus Registration** - YAML-driven specification of schema.table.column containing embeddings
2. **Entity Discovery** - Cluster vectors to discover distinct entities
3. **Entity Table** - Maintain canonical entity records with centroid embeddings
4. **Vector-Entity Mapping** - Many-to-many join table linking source vectors to entities
5. **Incremental Updates** - Support adding new vectors and updating entity assignments

## Non-Goals

- Automatic discovery of embedding columns (requires explicit configuration)
- Real-time entity extraction (batch-oriented design)
- Training or fine-tuning embedding models
- Entity type classification (entities are unlabeled clusters)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Entity Extraction Pipeline                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │ YAML Config  │───▶│ Corpus       │───▶│ Unified      │          │
│  │ (sources)    │    │ Registry     │    │ Embedding    │          │
│  └──────────────┘    └──────────────┘    │ View         │          │
│                                          └──────┬───────┘          │
│                                                 │                   │
│                                                 ▼                   │
│                                          ┌──────────────┐          │
│                                          │ Clustering   │          │
│                                          │ Engine       │          │
│                                          │ (HDBSCAN)    │          │
│                                          └──────┬───────┘          │
│                                                 │                   │
│                      ┌──────────────────────────┼───────────────┐  │
│                      ▼                          ▼               ▼  │
│               ┌──────────────┐          ┌──────────────┐  ┌──────┐│
│               │ entities     │◀────────▶│ vector_      │  │source││
│               │ (canonical)  │   N:M    │ entity_map   │  │tables││
│               └──────────────┘          └──────────────┘  └──────┘│
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## Component 1: Corpus Registry (YAML Configuration)

### Purpose

Manually specify which schema.table.column combinations contain vectorized text that should participate in entity extraction.

### Schema Configuration

```yaml
# entity-extraction-config.yaml

entityExtraction:
  name: "financial_entities"
  version: "1.0"

  # Embedding model metadata (for documentation/validation)
  embeddingModel:
    name: "all-MiniLM-L6-v2"
    dimensions: 384
    # Optional: validate all sources have matching dimensions
    validateDimensions: true

  # Vector sources - manually specified schema.table.column
  sources:
    - id: "sec_filings"
      schema: "sec"
      table: "filing_embeddings"
      embeddingColumn: "content_embedding"
      idColumn: "filing_id"
      labelColumn: "registrant_name"
      # Optional: additional columns to include in entity metadata
      metadataColumns:
        - "cik"
        - "filing_date"
        - "form_type"
      # Optional: filter condition
      filter: "form_type IN ('10-K', '10-Q')"

    - id: "company_descriptions"
      schema: "reference"
      table: "companies"
      embeddingColumn: "description_embedding"
      idColumn: "company_id"
      labelColumn: "company_name"
      metadataColumns:
        - "ticker"
        - "industry"
        - "sector"

    - id: "news_mentions"
      schema: "news"
      table: "article_entities"
      embeddingColumn: "mention_embedding"
      idColumn: "mention_id"
      labelColumn: "entity_text"
      metadataColumns:
        - "article_id"
        - "mention_type"
        - "confidence"
      filter: "confidence > 0.7"

  # Clustering configuration
  clustering:
    algorithm: "hdbscan"  # or "kmeans", "dbscan"
    parameters:
      minClusterSize: 3
      minSamples: 2
      metric: "cosine"  # or "euclidean"
    # Threshold for soft assignment (vectors can match multiple entities)
    softAssignmentThreshold: 0.85
    # Maximum entities per vector (0 = unlimited)
    maxEntitiesPerVector: 5

  # Output configuration
  output:
    # Where to materialize entity tables
    location: "s3://bucket/entities/"
    # Entity table name
    entityTable: "entities"
    # Join table name
    mappingTable: "vector_entity_map"
    # Include centroid embedding in entity table
    includeCentroid: true
    # Format for materialized tables
    format: "parquet"  # or "iceberg"

  # Update strategy
  updates:
    mode: "incremental"  # or "full_rebuild"
    # For incremental: how to handle new vectors
    newVectorStrategy: "assign_nearest"  # or "recluster"
    # Trigger re-clustering if cluster quality degrades
    reclusterThreshold: 0.70
```

### Configuration Classes

```java
/**
 * Root configuration for entity extraction.
 */
public class EntityExtractionConfig {
    private String name;
    private String version;
    private EmbeddingModelConfig embeddingModel;
    private List<VectorSourceConfig> sources;
    private ClusteringConfig clustering;
    private OutputConfig output;
    private UpdateConfig updates;
}

/**
 * Configuration for a single vector source (schema.table.column).
 */
public class VectorSourceConfig {
    private String id;                    // Unique identifier for this source
    private String schema;                // Database schema name
    private String table;                 // Table name
    private String embeddingColumn;       // Column containing embedding vector
    private String idColumn;              // Primary key column
    private String labelColumn;           // Human-readable label column
    private List<String> metadataColumns; // Additional columns for entity metadata
    private String filter;                // Optional WHERE clause filter

    /**
     * Build SQL to extract vectors from this source.
     */
    public String buildExtractionQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT '").append(id).append("' AS source_id, ");
        sql.append(idColumn).append(" AS vector_id, ");
        sql.append(labelColumn).append(" AS label, ");
        sql.append(embeddingColumn).append(" AS embedding");

        if (metadataColumns != null && !metadataColumns.isEmpty()) {
            for (String col : metadataColumns) {
                sql.append(", ").append(col);
            }
        }

        sql.append(" FROM ");
        if (schema != null) {
            sql.append(schema).append(".");
        }
        sql.append(table);

        if (filter != null && !filter.isEmpty()) {
            sql.append(" WHERE ").append(filter);
        }

        return sql.toString();
    }
}

/**
 * Clustering algorithm configuration.
 */
public class ClusteringConfig {
    private String algorithm;             // hdbscan, kmeans, dbscan
    private Map<String, Object> parameters;
    private double softAssignmentThreshold;
    private int maxEntitiesPerVector;
}
```

---

## Component 2: Unified Embedding View

### Purpose

Create a single view combining all vectors from registered sources, enabling cross-source clustering.

### Implementation

```java
/**
 * Builds unified view of all embedding sources.
 */
public class UnifiedEmbeddingView {
    private final EntityExtractionConfig config;
    private final Connection connection;

    /**
     * Create CTE that unions all vector sources.
     */
    public String buildUnionCte() {
        List<String> sourceQueries = config.getSources().stream()
            .map(VectorSourceConfig::buildExtractionQuery)
            .collect(Collectors.toList());

        return "unified_embeddings AS (\n  " +
            String.join("\n  UNION ALL\n  ", sourceQueries) +
            "\n)";
    }

    /**
     * Materialize unified embeddings to temporary table for clustering.
     */
    public void materialize(String tempTableName) {
        String sql = String.format(
            "CREATE TEMPORARY TABLE %s AS\n" +
            "WITH %s\n" +
            "SELECT * FROM unified_embeddings",
            tempTableName,
            buildUnionCte()
        );
        execute(sql);
    }

    /**
     * Validate all sources have consistent embedding dimensions.
     */
    public void validateDimensions() {
        if (!config.getEmbeddingModel().isValidateDimensions()) {
            return;
        }

        int expectedDims = config.getEmbeddingModel().getDimensions();
        for (VectorSourceConfig source : config.getSources()) {
            int actualDims = getEmbeddingDimensions(source);
            if (actualDims != expectedDims) {
                throw new ConfigurationException(
                    "Source '%s' has %d dimensions, expected %d",
                    source.getId(), actualDims, expectedDims
                );
            }
        }
    }
}
```

---

## Component 3: Clustering Engine

### Purpose

Cluster unified embeddings to discover entities, computing cluster centroids and quality metrics.

### Implementation

```java
/**
 * Entity discovery via vector clustering.
 */
public class ClusteringEngine {
    private final ClusteringConfig config;

    /**
     * Execute clustering and return entity assignments.
     */
    public ClusteringResult cluster(String embeddingsTable) {
        switch (config.getAlgorithm().toLowerCase()) {
            case "hdbscan":
                return clusterHdbscan(embeddingsTable);
            case "kmeans":
                return clusterKmeans(embeddingsTable);
            case "dbscan":
                return clusterDbscan(embeddingsTable);
            default:
                throw new UnsupportedOperationException(
                    "Unknown clustering algorithm: " + config.getAlgorithm()
                );
        }
    }

    /**
     * HDBSCAN clustering via DuckDB.
     * Note: Requires custom UDF or external library.
     */
    private ClusteringResult clusterHdbscan(String embeddingsTable) {
        // Option 1: DuckDB with Python UDF
        // Option 2: Export to Python, cluster, import results
        // Option 3: Implement approximate HDBSCAN in SQL

        // For MVP, use connected components via similarity threshold
        return clusterViaSimilarityGraph(embeddingsTable);
    }

    /**
     * Approximate clustering via similarity graph connected components.
     * MVP approach that works purely in DuckDB.
     */
    private ClusteringResult clusterViaSimilarityGraph(String embeddingsTable) {
        double threshold = config.getSoftAssignmentThreshold();

        // Step 1: Build similarity edges above threshold
        String edgesSql = String.format("""
            CREATE TEMPORARY TABLE similarity_edges AS
            SELECT
                a.source_id || ':' || a.vector_id AS node_a,
                b.source_id || ':' || b.vector_id AS node_b,
                array_cosine_similarity(a.embedding, b.embedding) AS similarity
            FROM %s a, %s b
            WHERE a.source_id || ':' || a.vector_id < b.source_id || ':' || b.vector_id
              AND array_cosine_similarity(a.embedding, b.embedding) > %f
            """, embeddingsTable, embeddingsTable, threshold);

        // Step 2: Find connected components (iterative label propagation)
        // This is a simplified version; production would use proper graph algorithm
        String componentsSql = """
            CREATE TEMPORARY TABLE node_clusters AS
            WITH RECURSIVE components AS (
                -- Initialize: each node is its own component
                SELECT DISTINCT node_a AS node, node_a AS component
                FROM similarity_edges
                UNION
                SELECT DISTINCT node_b AS node, node_b AS component
                FROM similarity_edges

                UNION

                -- Propagate: adopt smallest component ID from neighbors
                SELECT c.node, MIN(LEAST(c.component, e.component))
                FROM components c
                JOIN (
                    SELECT node_a AS node, node_b AS neighbor FROM similarity_edges
                    UNION ALL
                    SELECT node_b AS node, node_a AS neighbor FROM similarity_edges
                ) edges ON c.node = edges.node
                JOIN components e ON edges.neighbor = e.node
                GROUP BY c.node
            )
            SELECT node, MIN(component) AS cluster_id
            FROM components
            GROUP BY node
            """;

        execute(edgesSql);
        execute(componentsSql);

        return new ClusteringResult("node_clusters");
    }
}
```

---

## Component 4: Entity Table Generator

### Purpose

Generate the canonical entity table with entity IDs, centroids, and metadata.

### Schema

```sql
CREATE TABLE entities (
    entity_id       INTEGER PRIMARY KEY,    -- Unique entity identifier
    canonical_name  VARCHAR,                -- Best label for this entity
    centroid        FLOAT[],                -- Mean embedding of cluster
    cluster_size    INTEGER,                -- Number of vectors in cluster
    quality_score   DOUBLE,                 -- Cluster cohesion metric
    source_count    INTEGER,                -- Number of distinct sources
    created_at      TIMESTAMP,              -- First discovered
    updated_at      TIMESTAMP,              -- Last modified
    metadata        JSON                    -- Aggregated source metadata
);
```

### Implementation

```java
/**
 * Generates and maintains the entity table.
 */
public class EntityTableGenerator {

    /**
     * Generate entity table from clustering results.
     */
    public void generateEntityTable(
            String embeddingsTable,
            String clusterTable,
            String outputTable) {

        String sql = String.format("""
            CREATE TABLE %s AS
            WITH clustered AS (
                SELECT
                    e.*,
                    c.cluster_id
                FROM %s e
                JOIN %s c ON e.source_id || ':' || e.vector_id = c.node
            ),
            entity_stats AS (
                SELECT
                    cluster_id,
                    -- Canonical name: most frequent label
                    MODE() WITHIN GROUP (ORDER BY label) AS canonical_name,
                    -- Centroid: mean of embeddings
                    list_aggregate(list(embedding), 'avg') AS centroid,
                    -- Cluster size
                    COUNT(*) AS cluster_size,
                    -- Source diversity
                    COUNT(DISTINCT source_id) AS source_count,
                    -- Quality: average pairwise similarity (sampled)
                    AVG(intra_cluster_similarity) AS quality_score
                FROM clustered
                GROUP BY cluster_id
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY cluster_size DESC) AS entity_id,
                canonical_name,
                centroid,
                cluster_size,
                quality_score,
                source_count,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM entity_stats
            WHERE cluster_id != -1  -- Exclude noise cluster
            """, outputTable, embeddingsTable, clusterTable);

        execute(sql);
    }
}
```

---

## Component 5: Vector-Entity Mapping Table

### Purpose

Many-to-many join table linking source vectors to entities, supporting 0..N entities per vector.

### Schema

```sql
CREATE TABLE vector_entity_map (
    source_id       VARCHAR,        -- Source identifier from config
    vector_id       VARCHAR,        -- Primary key in source table
    entity_id       INTEGER,        -- FK to entities.entity_id
    confidence      DOUBLE,         -- Similarity to entity centroid
    assignment_type VARCHAR,        -- 'primary', 'secondary', 'soft'
    created_at      TIMESTAMP,

    PRIMARY KEY (source_id, vector_id, entity_id),
    FOREIGN KEY (entity_id) REFERENCES entities(entity_id)
);
```

### Implementation

```java
/**
 * Generates vector-to-entity mapping with soft assignment.
 */
public class VectorEntityMapper {
    private final EntityExtractionConfig config;

    /**
     * Generate mapping table with soft assignments.
     */
    public void generateMapping(
            String embeddingsTable,
            String entityTable,
            String outputTable) {

        double threshold = config.getClustering().getSoftAssignmentThreshold();
        int maxEntities = config.getClustering().getMaxEntitiesPerVector();

        String sql = String.format("""
            CREATE TABLE %s AS
            WITH similarities AS (
                SELECT
                    e.source_id,
                    e.vector_id,
                    ent.entity_id,
                    array_cosine_similarity(e.embedding, ent.centroid) AS confidence
                FROM %s e
                CROSS JOIN %s ent
                WHERE array_cosine_similarity(e.embedding, ent.centroid) > %f
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_id, vector_id
                        ORDER BY confidence DESC
                    ) AS rank
                FROM similarities
            )
            SELECT
                source_id,
                vector_id,
                entity_id,
                confidence,
                CASE rank
                    WHEN 1 THEN 'primary'
                    ELSE 'secondary'
                END AS assignment_type,
                CURRENT_TIMESTAMP AS created_at
            FROM ranked
            WHERE rank <= %d OR %d = 0  -- maxEntities filter (0 = unlimited)
            """,
            outputTable, embeddingsTable, entityTable,
            threshold, maxEntities, maxEntities);

        execute(sql);
    }
}
```

---

## Component 6: Incremental Update Handler

### Purpose

Handle new vectors without full re-clustering, assigning them to nearest existing entities.

### Implementation

```java
/**
 * Handles incremental updates to entity mappings.
 */
public class IncrementalUpdateHandler {
    private final EntityExtractionConfig config;

    /**
     * Process new vectors since last update.
     */
    public UpdateResult processNewVectors() {
        // Step 1: Identify new vectors not yet in mapping table
        String newVectorsSql = """
            CREATE TEMPORARY TABLE new_vectors AS
            WITH existing AS (
                SELECT DISTINCT source_id, vector_id
                FROM vector_entity_map
            ),
            current AS (
                -- Union of all current source vectors
                %s
            )
            SELECT c.*
            FROM current c
            LEFT JOIN existing e
              ON c.source_id = e.source_id AND c.vector_id = e.vector_id
            WHERE e.source_id IS NULL
            """.formatted(buildUnionCte());

        execute(newVectorsSql);

        long newCount = getRowCount("new_vectors");
        if (newCount == 0) {
            return UpdateResult.noChanges();
        }

        // Step 2: Assign new vectors to nearest entities
        assignNewVectors("new_vectors");

        // Step 3: Check if re-clustering is needed
        if (shouldRecluster()) {
            return UpdateResult.reclusterRequired(newCount);
        }

        return UpdateResult.success(newCount);
    }

    /**
     * Assign new vectors to nearest existing entities.
     */
    private void assignNewVectors(String newVectorsTable) {
        double threshold = config.getClustering().getSoftAssignmentThreshold();

        String sql = String.format("""
            INSERT INTO vector_entity_map
            SELECT
                nv.source_id,
                nv.vector_id,
                e.entity_id,
                array_cosine_similarity(nv.embedding, e.centroid) AS confidence,
                'incremental' AS assignment_type,
                CURRENT_TIMESTAMP AS created_at
            FROM %s nv
            CROSS JOIN entities e
            WHERE array_cosine_similarity(nv.embedding, e.centroid) > %f
            """, newVectorsTable, threshold);

        execute(sql);
    }

    /**
     * Check if cluster quality has degraded enough to warrant re-clustering.
     */
    private boolean shouldRecluster() {
        double threshold = config.getUpdates().getReclusterThreshold();

        // Calculate current average cluster quality
        String sql = """
            SELECT AVG(quality_score) FROM entities
            """;

        double avgQuality = executeScalar(sql);
        return avgQuality < threshold;
    }
}
```

---

## Pipeline Orchestration

### Full Pipeline

```java
/**
 * Orchestrates the complete entity extraction pipeline.
 */
public class EntityExtractionPipeline {
    private final EntityExtractionConfig config;
    private final UnifiedEmbeddingView embeddingView;
    private final ClusteringEngine clusteringEngine;
    private final EntityTableGenerator entityGenerator;
    private final VectorEntityMapper vectorMapper;
    private final IncrementalUpdateHandler updateHandler;

    /**
     * Execute full entity extraction pipeline.
     */
    public PipelineResult execute(ExecutionMode mode) {
        logger.info("Starting entity extraction pipeline: {}", config.getName());

        // Step 1: Validate configuration
        embeddingView.validateDimensions();

        // Step 2: Build unified embedding view
        String embeddingsTable = "temp_unified_embeddings";
        embeddingView.materialize(embeddingsTable);

        long vectorCount = getRowCount(embeddingsTable);
        logger.info("Unified {} vectors from {} sources",
            vectorCount, config.getSources().size());

        if (mode == ExecutionMode.INCREMENTAL) {
            // Incremental: assign new vectors to existing entities
            UpdateResult result = updateHandler.processNewVectors();
            if (!result.isReclusterRequired()) {
                return PipelineResult.incremental(result);
            }
            logger.info("Re-clustering triggered due to quality degradation");
        }

        // Step 3: Cluster vectors to discover entities
        ClusteringResult clusters = clusteringEngine.cluster(embeddingsTable);
        logger.info("Discovered {} clusters ({} noise vectors)",
            clusters.getClusterCount(), clusters.getNoiseCount());

        // Step 4: Generate entity table
        String entityTable = config.getOutput().getEntityTable();
        entityGenerator.generateEntityTable(
            embeddingsTable, clusters.getTable(), entityTable);

        // Step 5: Generate vector-entity mappings
        String mappingTable = config.getOutput().getMappingTable();
        vectorMapper.generateMapping(
            embeddingsTable, entityTable, mappingTable);

        // Step 6: Materialize to output location
        materializeOutput(entityTable, mappingTable);

        return PipelineResult.success(
            getRowCount(entityTable),
            getRowCount(mappingTable)
        );
    }
}
```

---

## Query Patterns

### Find all entities for a vector

```sql
SELECT
    e.entity_id,
    e.canonical_name,
    m.confidence,
    m.assignment_type
FROM vector_entity_map m
JOIN entities e ON m.entity_id = e.entity_id
WHERE m.source_id = 'sec_filings'
  AND m.vector_id = '12345'
ORDER BY m.confidence DESC;
```

### Find all vectors for an entity

```sql
SELECT
    m.source_id,
    m.vector_id,
    m.confidence
FROM vector_entity_map m
WHERE m.entity_id = 42
ORDER BY m.confidence DESC;
```

### Cross-table entity join

```sql
-- Find SEC filings and news articles about the same entity
SELECT
    f.filing_id,
    f.registrant_name,
    a.article_id,
    a.headline,
    e.canonical_name AS matched_entity
FROM sec.filing_embeddings f
JOIN vector_entity_map mf
  ON mf.source_id = 'sec_filings' AND mf.vector_id = f.filing_id
JOIN vector_entity_map ma
  ON ma.entity_id = mf.entity_id AND ma.source_id = 'news_mentions'
JOIN news.article_entities a
  ON a.mention_id = ma.vector_id
JOIN entities e ON e.entity_id = mf.entity_id
WHERE mf.assignment_type = 'primary'
  AND ma.assignment_type = 'primary';
```

---

## Implementation Phases

### Phase 1: Configuration & Corpus Registry
**Deliverables:**
- `EntityExtractionConfig` and related config classes
- YAML parser for entity extraction configuration
- `VectorSourceConfig.buildExtractionQuery()` implementation
- Configuration validation (schema exists, columns exist, dimensions match)
- Unit tests for configuration parsing

**Acceptance Criteria:**
- Can parse entity extraction YAML config
- Validates all referenced tables/columns exist
- Validates embedding dimensions match declared model

### Phase 2: Unified Embedding View
**Deliverables:**
- `UnifiedEmbeddingView` class
- Union query builder for multiple sources
- Dimension validation across sources
- Materialization to temporary table
- Integration tests with multiple source tables

**Acceptance Criteria:**
- Can union vectors from multiple schema.table.column sources
- Handles NULL embeddings gracefully
- Applies filter conditions correctly
- Materializes to queryable temp table

### Phase 3: Clustering Engine (MVP)
**Deliverables:**
- `ClusteringEngine` with similarity-graph approach
- Connected components algorithm in SQL
- Cluster centroid computation
- Quality score calculation
- Integration tests with synthetic data

**Acceptance Criteria:**
- Discovers clusters from unified embeddings
- Handles noise vectors (no cluster)
- Computes meaningful centroids
- Works with 10K+ vectors in reasonable time (<1 min)

### Phase 4: Entity Table Generation
**Deliverables:**
- `EntityTableGenerator` class
- Entity schema with centroid, metadata, quality metrics
- Canonical name selection logic
- Integration tests

**Acceptance Criteria:**
- Generates entity table from clustering results
- Canonical name reflects most common label
- Centroid is accurate mean of cluster vectors
- Quality score reflects cluster cohesion

### Phase 5: Vector-Entity Mapping
**Deliverables:**
- `VectorEntityMapper` class
- Soft assignment with configurable threshold
- Max entities per vector limiting
- Primary/secondary assignment labeling
- Integration tests

**Acceptance Criteria:**
- Maps vectors to 0..N entities based on threshold
- Respects maxEntitiesPerVector config
- Labels primary vs secondary assignments correctly
- Handles vectors with no matching entity (below threshold)

### Phase 6: Incremental Updates
**Deliverables:**
- `IncrementalUpdateHandler` class
- New vector detection
- Assignment to existing entities
- Re-cluster trigger logic
- Integration tests for incremental scenarios

**Acceptance Criteria:**
- Detects vectors not yet in mapping table
- Assigns new vectors to nearest entities
- Triggers re-cluster when quality degrades
- Handles first-run (no existing entities) correctly

### Phase 7: Pipeline Orchestration & Output
**Deliverables:**
- `EntityExtractionPipeline` orchestrator
- Output materialization (Parquet/Iceberg)
- Progress logging and metrics
- Error handling and recovery
- End-to-end integration tests

**Acceptance Criteria:**
- Full pipeline executes successfully
- Outputs to configured location
- Handles errors gracefully with meaningful messages
- Provides execution metrics

---

## Design Decisions

### 1. Why manual corpus specification?

**Decision:** Require explicit YAML configuration of embedding columns.

**Rationale:**
- Automatic discovery would require scanning all columns for vector types
- False positives (non-semantic vectors like PCA features)
- Different embedding models produce incompatible vectors
- Explicit configuration ensures intentionality and correctness

### 2. Why soft assignment (0..N entities per vector)?

**Decision:** Allow vectors to map to multiple entities with confidence scores.

**Rationale:**
- Text may mention multiple entities (compound mentions)
- Ambiguous mentions shouldn't be forced into one cluster
- Confidence scores enable downstream filtering
- Zero entities = noise or out-of-vocabulary

### 3. Why similarity-graph clustering instead of HDBSCAN?

**Decision:** MVP uses connected components on similarity graph.

**Rationale:**
- Pure SQL implementation (no Python dependency)
- Works entirely within DuckDB
- Simpler to debug and understand
- Can upgrade to proper HDBSCAN in future phase

**Trade-offs:**
- Less sophisticated cluster detection
- May merge clusters that HDBSCAN would separate
- Requires tuning similarity threshold carefully

### 4. Why materialized entity tables instead of views?

**Decision:** Materialize entities and mappings to Parquet/Iceberg.

**Rationale:**
- Clustering is expensive; avoid recomputing
- Enables incremental updates
- Join performance benefits from materialization
- Supports version history via Iceberg

---

## Future Enhancements

1. **HDBSCAN Integration** - Python UDF or external service for better clustering
2. **Entity Type Classification** - Supervised learning to label entity types
3. **Entity Merging UI** - Human-in-the-loop to merge/split clusters
4. **Real-time Assignment** - Stream new vectors through assignment API
5. **Cross-Model Support** - Map embeddings from different models via projection
6. **Active Learning** - Use human corrections to improve clustering

---

## Appendix: DuckDB Vector Operations

```sql
-- Cosine similarity
SELECT array_cosine_similarity(vec1, vec2) AS similarity;

-- Euclidean distance
SELECT array_distance(vec1, vec2) AS distance;

-- Vector mean (centroid)
SELECT list_aggregate(list(embedding), 'avg') AS centroid
FROM embeddings
GROUP BY cluster_id;

-- Dimension validation
SELECT array_length(embedding) AS dims FROM embeddings LIMIT 1;

-- HNSW index for fast similarity search (requires vss extension)
INSTALL vss;
LOAD vss;
CREATE INDEX embedding_idx ON embeddings USING HNSW (embedding);
```
