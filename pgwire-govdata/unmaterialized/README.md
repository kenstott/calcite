# Unmaterialized tables

Tables a govdata schema YAML declares but the warehouse does not hold yet (their ETL has not run),
so no complete catalog walk lists them and the shipped catalog cache cannot contain them.

One empty file per table, named `<schema>.<table>`, so changes adding different tables never touch
the same file:

```bash
touch pgwire-govdata/unmaterialized/<schema>.<table>
```

Add an entry only after confirming the warehouse has no Iceberg table for it; a table that is
materialized but missing from the cache means the cache is stale: regenerate it instead.

Regenerating the cache here (a complete walk written beside `model.json`) deletes the entry of every
table the walk found. `check_catalog_cache_coverage.py` fails on a declared table that is neither
cached nor listed, on a listed table the cache holds, and on a listed table no YAML declares.
