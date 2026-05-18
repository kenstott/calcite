# Splunk JDBC Driver

Query Splunk using standard SQL from DBeaver, DataGrip, Tableau, or any JDBC client.

## Download

Build the shadow JAR from this repo:

```bash
./gradlew :splunk:shadowJar
# Output: splunk/build/libs/sih-splunk-*.jar
```

## Connect

**JDBC URL format:**
```
jdbc:splunk:url=<splunk-url>;[user=<user>;password=<pass>|token=<token>]
```

**Driver class:** `org.apache.calcite.adapter.splunk.SplunkDriver`

### Authentication examples

```
# Username/password
jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme

# Token
jdbc:splunk:url=https://localhost:8089;token=eyJhbGciOiJIUzI1NiI...

# Environment variables (no credentials in URL)
jdbc:splunk:
```

## Connection parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `url` | Splunk management URL | `$SPLUNK_URL` |
| `user` | Username | `$SPLUNK_USERNAME` |
| `password` | Password | `$SPLUNK_PASSWORD` |
| `token` | Bearer token | `$SPLUNK_TOKEN` |
| `app` | Splunk app context | `Splunk_SA_CIM` |
| `datamodelFilter` | Glob or regex filter on data model names | (all) |
| `datamodelCacheTtl` | Cache TTL in seconds (`-1` = forever, `0` = disabled) | `300` |
| `ssl` | Enable SSL verification | `true` |

## Environment variables

| Variable | Description |
|----------|-------------|
| `SPLUNK_URL` | Splunk management URL |
| `SPLUNK_USERNAME` | Username |
| `SPLUNK_PASSWORD` | Password |
| `SPLUNK_TOKEN` | Bearer token |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme`
3. **Driver JAR:** add `sih-splunk-*.jar`
4. **Driver class:** `org.apache.calcite.adapter.splunk.SplunkDriver`

## Sample queries

```sql
-- List all discovered tables (data models)
SELECT table_name FROM information_schema.tables;

-- Query authentication events
SELECT _time, user, action, app
FROM authentication.authentication
WHERE action = 'failure'
ORDER BY _time DESC
FETCH FIRST 100 ROWS ONLY;

-- Network traffic summary
SELECT src_ip, dest_ip, COUNT(*) AS connections
FROM network_traffic.network_traffic
GROUP BY src_ip, dest_ip
ORDER BY connections DESC;
```

## Data model discovery

Tables are discovered automatically from Splunk data models. Use `datamodelFilter` to limit which models are loaded:

```
# Load only authentication and web models
jdbc:splunk:url=https://localhost:8089;token=xxx;datamodelFilter=Authentication,Web

# Load models matching a pattern
jdbc:splunk:url=https://localhost:8089;token=xxx;datamodelFilter=auth*
```
