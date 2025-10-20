# Aperio Refpack MCP Server

Model Context Protocol (MCP) server for Apache Calcite, enabling Claude Code and other MCP clients to discover schemas, query data, and perform semantic search on vector embeddings.

## Overview

This module provides a JSON-RPC stdio server that exposes Calcite's capabilities through the Model Context Protocol. It works with any Calcite model.json configuration and provides automatic discovery of vector embeddings with intelligent pattern detection.

## Features

- **Universal Calcite Support**: Works with any model.json configuration
- **JDBC-Based Discovery**: Rich metadata from DatabaseMetaData
- **Vector Search**: Semantic similarity search with 4 relationship patterns
- **Pattern Auto-Detection**: Automatically detects vector column relationships
- **LLM-Friendly**: Designed for Claude Code and other AI assistants

## Vector Relationship Patterns

The server automatically detects and handles 4 vector column patterns:

### Pattern 1: Co-Located
Vector column lives directly in the source table.
```sql
CREATE TABLE mda_sections (
  section_id VARCHAR,
  full_text VARCHAR,
  embedding VARCHAR  -- [VECTOR dimension=384 provider=local model=tf-idf]
);
```

### Pattern 2a: Formal FK
Separate vector table with formal FK constraint (auto-discovered via JDBC).
```sql
CREATE TABLE mda_embeddings (
  section_id VARCHAR,
  embedding VARCHAR,  -- [VECTOR dimension=384 provider=local model=tf-idf]
  FOREIGN KEY (section_id) REFERENCES mda_sections(section_id)
);
```

### Pattern 2b: Logical FK
Logical reference without formal FK constraint (via metadata).
```sql
-- [VECTOR dimension=384 provider=local model=tf-idf source_table=mda_sections source_id_col=section_id]
```

### Pattern 3: Multi-Source
Union table with polymorphic source tracking.
```sql
CREATE TABLE vectorized_blobs (
  source_table VARCHAR,   -- 'mda_sections', 'footnotes', etc.
  source_id VARCHAR,
  embedding VARCHAR  -- [VECTOR dimension=384 provider=local model=tf-idf source_table_col=source_table source_id_col=source_id]
);
```

## MCP Tools

### Discovery
- `list_schemas()` - List all schemas
- `list_tables(schema, include_comments)` - List tables in schema
- `describe_table(schema, table, include_comments)` - Get column details with vector metadata

### Query
- `query_data(sql, limit)` - Execute SQL query
- `sample_table(schema, table, limit)` - Sample rows from table

### Vector Search
- `semantic_search(schema, table, query_text, limit, threshold, source_table_filter, include_source)` - Semantic similarity search
- `list_vector_sources(schema, table)` - List source tables for Pattern 3

## Usage

### Running the Server

```bash
java -jar aperio-refpack-mcp-server.jar \
  --jdbc-url "jdbc:calcite:model=/path/to/model.json;lex=ORACLE;unquotedCasing=TO_LOWER"
```

### Claude Desktop Configuration

Add to `~/.config/claude-desktop/mcp-servers.json`:

```json
{
  "mcpServers": {
    "calcite-govdata": {
      "command": "java",
      "args": [
        "-Xmx2g",
        "-jar", "/path/to/aperio-refpack-mcp-server.jar",
        "--jdbc-url", "jdbc:calcite:model=/path/to/model.json;lex=ORACLE;unquotedCasing=TO_LOWER"
      ],
      "env": {
        "GOVDATA_CACHE_DIR": "s3://govdata-production-cache",
        "GOVDATA_PARQUET_DIR": "s3://govdata-parquet"
      }
    }
  }
}
```

## Example Queries

### Pattern 1: Co-located Vector Search
```json
{
  "method": "semantic_search",
  "params": {
    "schema": "sec",
    "table": "mda_sections",
    "query_text": "climate risk",
    "limit": 10,
    "threshold": 0.7
  }
}
```

### Pattern 3: Multi-Source with Filter
```json
{
  "method": "semantic_search",
  "params": {
    "schema": "sec",
    "table": "vectorized_blobs",
    "query_text": "supply chain disruptions",
    "source_table_filter": "footnotes",
    "limit": 10
  }
}
```

### List Available Sources
```json
{
  "method": "list_vector_sources",
  "params": {
    "schema": "sec",
    "table": "vectorized_blobs"
  }
}
```

## Building

```bash
./gradlew :aperio-refpack-mcp-server:build
```

## Dependencies

- Calcite core, file, govdata modules
- Gson 2.11.0 for JSON processing
- Log4j 2.23.1 for logging
- Avatica for JDBC

## License

Apache License 2.0
