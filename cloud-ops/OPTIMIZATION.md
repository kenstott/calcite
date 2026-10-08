# What the Cloud Ops adapter pushes down

Most of a query runs in Calcite, not in the cloud. The adapter lists resources through each
provider's inventory API, builds full-width rows, and lets Calcite filter, join and aggregate them.
A few parts of a query do change which API calls are made. This page says which.

| Part of the query | Effect on cloud API calls |
|-------------------|---------------------------|
| `cloud_provider = '...'` | Only the named providers are called |
| `account_id = '...'` | Only the named accounts, subscriptions or projects are queried |
| `region = '...'` on `kubernetes_clusters` | AWS: EKS is asked in that region only. Azure: added to the Resource Graph query |
| `cluster_name`, `application` filters on `kubernetes_clusters` | Azure: added to the Resource Graph query. AWS: applied to the rows after they are fetched |
| Any other filter | None |
| Column list on `storage_resources` | AWS: per-bucket calls for unselected columns are skipped |
| Column list on any other table | None |
| `ORDER BY` | None. The adapter sorts the combined rows itself |
| `LIMIT` / `OFFSET` | None, except a row cap on `kubernetes_clusters` when the query has no `ORDER BY` and no `WHERE` |

The rest of the page gives the detail and the source file for each row.

## Filters

Every table is a Calcite `ProjectableFilterableTable`. Calcite hands `scan` the `WHERE` conjuncts,
and the adapter leaves all of them in the list, so Calcite evaluates every filter again on the rows
that come back. `AbstractCloudOpsTable.applyFilters` returns its input unchanged.

`util/CloudOpsFilterHandler` reads the filters only to decide what to fetch. It recognises a
column compared with a literal (`=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`), `IS NULL`,
`IS NOT NULL`, `IN`, and an `OR` of two such terms.

**Provider and account selection** (`AbstractCloudOpsTable.scan`, all tables):

- `cloud_provider = 'aws'` limits the scan to AWS. Without such a filter every configured
  provider is called.
- `account_id = '...'` replaces the configured account list. The same list goes to every provider
  being called. AWS skips an id it has no credentials for; Azure and GCP pass the ids to their
  APIs as given. Combine an `account_id` filter with a `cloud_provider` filter.

```sql
-- Calls AWS only, and only account 111111111111
SELECT instance_id, state
FROM cloud.compute_resources
WHERE cloud_provider = 'aws' AND account_id = '111111111111';
```

**`kubernetes_clusters` only.** This is the one table whose providers receive the filter handler.

- AWS (`AWSProvider.executeKubernetesClusterQuery`): `region = '...'` skips every other region.
  Filters on `application` and `cluster_name` are applied to the fetched clusters.
- Azure (`AzureProvider.buildKubernetesClusterKql`): filters on `account_id`, `region`,
  `cluster_name` and `application` are appended to the KQL as a `| where` clause.
- GCP: the filters are logged and not used. `listClusters` is called for all locations.

The other seven tables call the provider with the account list and nothing else.

### Known limitation: OR

`CloudOpsFilterHandler.extractFieldFilters` records the two sides of an `OR` as separate
constraints, and the code that uses them treats constraints as cumulative. `WHERE cloud_provider =
'aws' OR region = 'eastus'` therefore selects AWS only, and the Azure rows in `eastus` are never
fetched. Calcite cannot restore rows the adapter did not fetch. Until this is fixed, do not put
`cloud_provider`, `account_id`, or (on `kubernetes_clusters`) `region`, `cluster_name` or
`application` inside an `OR` with a different column.

## Projection

`scan` receives the selected column ordinals. Providers still return whole rows; the adapter
converts them to the declared column types, sorts if asked, and trims to the selected columns last
(`CloudOpsProjectionHandler.projectRows`).

One provider path uses the column list to make fewer calls. `AWSProvider.executeStorageResourceQuery`
lists buckets once, then makes a per-bucket call only for columns the query selects:

| Selected column | S3 / CloudWatch call per bucket |
|-----------------|---------------------------------|
| `region`, `size_bytes` | `GetBucketLocation` |
| `application`, `tags` | `GetBucketTagging` |
| `encryption_enabled`, `encryption_type`, `encryption_key_type` | `GetBucketEncryption` |
| `public_access_enabled`, `public_access_level` | `GetPublicAccessBlock` |
| `versioning_enabled` | `GetBucketVersioning` |
| `replication_type` | `GetBucketReplication` |
| `lifecycle_rules_count` | `GetBucketLifecycleConfiguration` |
| `size_bytes` | CloudWatch `GetMetricStatistics` |

`SELECT *` makes all of them. A column used only in `WHERE` counts as selected, because Calcite
adds filter columns to the list it passes to `scan`.

```sql
-- One ListBuckets call per account, no per-bucket calls
SELECT resource_name, created_date
FROM cloud.storage_resources
WHERE cloud_provider = 'aws';
```

There is no projection in Azure KQL: `CloudOpsProjectionHandler.buildAzureKqlProjectClause` returns
null and every Resource Graph query projects its full column set. The GCP `fields` parameter the
handler can build is logged and not sent.

## ORDER BY, LIMIT and OFFSET

`AbstractCloudOpsTable.toRel` registers the planner rule `CloudOpsSortScanRule`. The rule replaces a
`Sort` that sits directly on a cloud-ops table scan with a `CloudOpsSortedScan`, which calls the
table's six-argument `scan` with the sort order, offset and fetch.

The rule does not fire when:

- a `WHERE` lies between the sort and the scan, or the scan already carries filters;
- `OFFSET` or `FETCH` is a parameter (`LIMIT ?`) and not a literal.

In those cases Calcite sorts and limits above an ordinary scan.

When the rule fires, the table collects the rows of all providers, sorts them once
(`CloudOpsSortHandler.sortRows`), applies offset and fetch once, then projects. No sort is ever
sent to a cloud API. A provider's ordering of strings and nulls is not SQL's, so a provider that
sorted and truncated could keep the wrong rows.

```sql
EXPLAIN PLAN FOR
SELECT cluster_name, node_count
FROM cloud.kubernetes_clusters
ORDER BY node_count DESC
LIMIT 5;
-- The plan contains CloudOpsSortedScan(..., sort=[...], fetch=[5]) when the rule fired.
```

**Row cap.** Providers are told to return at most `offset + fetch` rows only when any rows will do:
there is a fetch, no sort, and no filter. Only the `kubernetes_clusters` providers use the cap:

- Azure adds `| take N` to the KQL when N is below 1000.
- AWS stops listing EKS clusters in a region once it has N names, then trims.
- GCP lists every cluster and trims.

The other tables ignore the cap and fetch everything. The table still applies offset and fetch to
the combined rows in every case.

## Caching

`util/CloudOpsCacheManager` is a Caffeine cache with a time-to-live and a 1000-entry limit. The
providers consult it for every Azure Resource Graph query, for the Kubernetes queries of all three
providers, and for the AWS S3 listing.

It does not carry results from one query to the next. Each table scan constructs a new provider
(`new AWSProvider(config.aws)` and its Azure and GCP counterparts), and that constructor creates
its own cache with a fixed five-minute lifetime. The cache is discarded with the provider when the
scan ends.

The settings `cache.enabled`, `cache.ttlMinutes` and `cache.debugMode` are read by
`CloudOpsSchemaFactory` and stored on `CloudOpsConfig`. No table or provider reads them, so they
change nothing today. `util/CloudOpsCacheValidator`, which would build a cache from them, is called
only from tests.

Expect every query to call the cloud APIs.

## Seeing what happened

Set the logger `org.apache.calcite.adapter.ops` to `DEBUG`. Each scan logs the filters, column
ordinals, sort, offset and fetch it received, then the row count, sort fields and provider row cap
it used. The AWS S3 path logs which per-bucket calls it made.
