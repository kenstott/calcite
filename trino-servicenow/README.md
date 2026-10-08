# trino-servicenow

Trino connector for a ServiceNow instance, built on the generic `trino-calcite-base` over the
[`servicenow`](../servicenow/README.md) adapter. **Read-only.** Not affiliated with or endorsed by
ServiceNow, Inc.

**Status: not run against a ServiceNow instance.** The connector's catalog-property and JDBC-URL
mapping is unit-tested (`./gradlew :trino-servicenow:test`, JDK 25 toolchain). The in-process
Trino test (`TestServiceNowConnector`, tag `integration`) skips itself without credentials and has
never been run. It is not in the publish workflow's matrix.

## Configure a catalog

`etc/catalog/servicenow.properties`:

```properties
connector.name=servicenow
instance-url=https://dev12345.service-now.com
username=svc_trino
password=...
# optional: only these tables (default: every table in sys_db_object, thousands)
tables=incident,task,sys_user
```

| Property | Description | Required |
|----------|-------------|----------|
| `instance-url` | Instance URL | yes |
| `auth-type` | only `basic` is implemented (default) | no |
| `username`, `password` | Integration user | yes |
| `tables` | Comma-separated tables to expose | no |
| `exclude-column-types` | Field types whose columns are left out | no |
| `schema` | Schema name (default `servicenow`) | no |
| `page-size`, `max-concurrent-requests`, `max-retries`, `max-retry-wait-seconds` | Request tuning | no |
| `catalog-cache-directory`, `catalog-cache-ttl-minutes` | Catalog cache | no |
| `pushdown-verification` | Path of a pushdown verification record | no |
| `trust-pushdown` | Pushdown entries to trust without a record | no |

Table and column names are ServiceNow's, already lower case, so `case-insensitive-name-matching`
is not required (unlike the Salesforce connector). A reference field `caller_id` is two columns,
`caller_id` (sys_id) and `caller_id__display`. Empty values are NULL. Filters are evaluated by
Trino/Calcite unless a pushdown entry is verified; see the adapter README.
