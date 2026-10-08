# Cloud Ops SQL Query Examples

Queries for inventory, security review and cost questions across Azure, AWS and GCP.

The queries name tables without a schema, which works when `cloud` is the connection's default
schema (it is with `jdbc:cloudops:`, and with a model file that sets `defaultSchema`). Names are
lower-case: quote them, or connect with `unquotedCasing=TO_LOWER` as these examples assume. See
the [README](README.md#identifier-case).

Table and column names here were checked against the table classes. The functions used are from
Calcite's standard operator table (`TIMESTAMPDIFF`, `LISTAGG`). The queries have not been executed
as a suite against live clouds.

A column that one cloud does not fill is null for that cloud's rows. Before reading a percentage as
a compliance figure, check the per-cloud notes in [docs/SCHEMA.md](docs/SCHEMA.md); a few columns
are constants for some services.

## Basic Resource Discovery

### List All Resources by Cloud Provider

```sql
-- Get count of resources by provider
SELECT
  'kubernetes_clusters' as resource_type,
  cloud_provider,
  COUNT(*) as count
FROM kubernetes_clusters
GROUP BY cloud_provider

UNION ALL

SELECT
  'storage_resources' as resource_type,
  cloud_provider,
  COUNT(*) as count
FROM storage_resources
GROUP BY cloud_provider

UNION ALL

SELECT
  'compute_resources' as resource_type,
  cloud_provider,
  COUNT(*) as count
FROM compute_resources
GROUP BY cloud_provider

ORDER BY resource_type, cloud_provider;
```

### Find Resources by Application

```sql
-- Find all resources for a specific application
SELECT
  cloud_provider,
  'kubernetes' as resource_type,
  cluster_name as resource_name,
  region,
  application
FROM kubernetes_clusters
WHERE application = 'MyApp'

UNION ALL

SELECT
  cloud_provider,
  'storage' as resource_type,
  resource_name,
  region,
  application
FROM storage_resources
WHERE application = 'MyApp'

ORDER BY cloud_provider, resource_type;
```

## Security Compliance Queries

### Kubernetes Security Posture

```sql
-- Kubernetes security compliance summary
SELECT
  cloud_provider,
  COUNT(*) as total_clusters,

  -- RBAC Compliance
  SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as rbac_enabled_count,
  ROUND(100.0 * SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as rbac_compliance_pct,

  -- Network Security
  SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) as private_cluster_count,
  ROUND(100.0 * SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) / COUNT(*), 1) as private_cluster_pct,

  -- Encryption
  SUM(CASE WHEN encryption_at_rest_enabled = true THEN 1 ELSE 0 END) as encryption_enabled_count,
  ROUND(100.0 * SUM(CASE WHEN encryption_at_rest_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_pct

FROM kubernetes_clusters
GROUP BY cloud_provider
ORDER BY cloud_provider;
```

### Storage Encryption Analysis

```sql
-- Storage encryption compliance
SELECT
  cloud_provider,
  storage_type,
  COUNT(*) as total_resources,

  -- Encryption Status
  SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted_count,
  SUM(CASE WHEN encryption_enabled = false OR encryption_enabled IS NULL THEN 1 ELSE 0 END) as unencrypted_count,

  -- Encryption Key Management
  SUM(CASE WHEN encryption_key_type = 'customer-managed' THEN 1 ELSE 0 END) as customer_managed_keys,
  SUM(CASE WHEN encryption_key_type = 'service-managed' THEN 1 ELSE 0 END) as service_managed_keys,

  -- Compliance Percentage
  ROUND(100.0 * SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_compliance_pct

FROM storage_resources
GROUP BY cloud_provider, storage_type
ORDER BY cloud_provider, storage_type;
```

### Public Access Analysis

```sql
-- Resources with public access exposure
SELECT
  s.cloud_provider,
  s.application,
  s.resource_name,
  s.storage_type,
  s.public_access_enabled,
  s.public_access_level,
  s.https_only,

  -- Risk Assessment Factors
  CASE
    WHEN s.public_access_enabled = true AND s.https_only = false THEN 'HIGH RISK'
    WHEN s.public_access_enabled = true AND s.https_only = true THEN 'MEDIUM RISK'
    ELSE 'LOW RISK'
  END as risk_level

FROM storage_resources s
WHERE s.public_access_enabled = true
ORDER BY
  CASE
    WHEN s.public_access_enabled = true AND s.https_only = false THEN 1
    WHEN s.public_access_enabled = true AND s.https_only = true THEN 2
    ELSE 3
  END,
  s.cloud_provider, s.application;
```

## Cost and Resource Optimization

### Orphaned Resources Detection

```sql
-- Resources with no application tag or label.
-- The adapter reports those as 'Untagged/Orphaned'.
SELECT
  cloud_provider,
  'storage' as resource_type,
  resource_name,
  region,
  storage_type,
  created_date,
  TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) as days_old
FROM storage_resources
WHERE application = 'Untagged/Orphaned'

UNION ALL

SELECT
  cloud_provider,
  'compute' as resource_type,
  instance_name as resource_name,
  region,
  instance_type as storage_type,
  launch_time as created_date,
  TIMESTAMPDIFF(DAY, launch_time, CURRENT_TIMESTAMP) as days_old
FROM compute_resources
WHERE application = 'Untagged/Orphaned'

ORDER BY days_old DESC, cloud_provider;
```

### Resource Distribution by Region

```sql
-- Resource counts per provider and region
SELECT
  cloud_provider,
  region,
  SUM(CASE WHEN resource_type = 'kubernetes' THEN 1 ELSE 0 END) as kubernetes_clusters,
  SUM(CASE WHEN resource_type = 'storage' THEN 1 ELSE 0 END) as storage_resources,
  SUM(CASE WHEN resource_type = 'compute' THEN 1 ELSE 0 END) as compute_instances,
  SUM(CASE WHEN resource_type = 'network' THEN 1 ELSE 0 END) as network_resources,
  COUNT(*) as total_resources
FROM (
  SELECT cloud_provider, region, 'kubernetes' as resource_type FROM kubernetes_clusters
  UNION ALL
  SELECT cloud_provider, region, 'storage' as resource_type FROM storage_resources
  UNION ALL
  SELECT cloud_provider, region, 'compute' as resource_type FROM compute_resources
  UNION ALL
  SELECT cloud_provider, region, 'network' as resource_type FROM network_resources
) all_resources
GROUP BY cloud_provider, region
ORDER BY cloud_provider, total_resources DESC;
```

For GCP, `compute_resources.region` holds the zone, and `network_resources.region` is filled for
subnets only.

## Cross-Cloud Application Analysis

### Multi-Cloud Application Footprint

```sql
-- Applications deployed across multiple cloud providers
WITH app_clouds AS (
  SELECT application, cloud_provider, COUNT(*) as resource_count
  FROM (
    SELECT application, cloud_provider FROM kubernetes_clusters WHERE application != 'Untagged/Orphaned'
    UNION ALL
    SELECT application, cloud_provider FROM storage_resources WHERE application != 'Untagged/Orphaned'
    UNION ALL
    SELECT application, cloud_provider FROM compute_resources WHERE application != 'Untagged/Orphaned'
  ) all_resources
  GROUP BY application, cloud_provider
),
app_summary AS (
  SELECT
    application,
    COUNT(DISTINCT cloud_provider) as cloud_count,
    SUM(resource_count) as total_resources,
    LISTAGG(cloud_provider, ', ') as cloud_providers
  FROM app_clouds
  GROUP BY application
)

SELECT *
FROM app_summary
WHERE cloud_count > 1
ORDER BY cloud_count DESC, total_resources DESC;
```

### Application Resource Inventory

```sql
-- Detailed inventory for a specific application
WITH app_inventory AS (
  SELECT
    'Kubernetes' as service_type,
    cloud_provider,
    region,
    cluster_name as resource_name,
    kubernetes_version as version_info,
    CASE WHEN rbac_enabled = true THEN 'Compliant' ELSE 'Non-Compliant' END as security_status
  FROM kubernetes_clusters
  WHERE application = 'ProductionApp'

  UNION ALL

  SELECT
    'Storage' as service_type,
    cloud_provider,
    region,
    resource_name,
    storage_type as version_info,
    CASE WHEN encryption_enabled = true THEN 'Encrypted' ELSE 'Unencrypted' END as security_status
  FROM storage_resources
  WHERE application = 'ProductionApp'

  UNION ALL

  SELECT
    'Compute' as service_type,
    cloud_provider,
    region,
    instance_name as resource_name,
    instance_type as version_info,
    CASE WHEN disk_encryption_enabled = true THEN 'Encrypted' ELSE 'Unencrypted' END as security_status
  FROM compute_resources
  WHERE application = 'ProductionApp'
)

SELECT
  service_type,
  cloud_provider,
  COUNT(*) as resource_count,
  COUNT(DISTINCT region) as region_count,
  SUM(CASE WHEN security_status LIKE '%Compliant' OR security_status = 'Encrypted' THEN 1 ELSE 0 END) as secure_resources
FROM app_inventory
GROUP BY service_type, cloud_provider
ORDER BY service_type, cloud_provider;
```

## Database and Storage Analytics

### Database Security Compliance

```sql
-- Database security posture analysis
SELECT
  cloud_provider,
  database_type,
  COUNT(*) as total_databases,

  -- Encryption Analysis
  SUM(CASE WHEN encrypted = true THEN 1 ELSE 0 END) as encrypted_count,
  ROUND(100.0 * SUM(CASE WHEN encrypted = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_pct,

  -- Public Access Analysis
  SUM(CASE WHEN publicly_accessible = true THEN 1 ELSE 0 END) as public_accessible_count,
  ROUND(100.0 * SUM(CASE WHEN publicly_accessible = true THEN 1 ELSE 0 END) / COUNT(*), 1) as public_access_pct,

  -- Backup Analysis (for GCP Cloud SQL the column is a count of retained backups, not days)
  SUM(CASE WHEN backup_retention_days > 0 THEN 1 ELSE 0 END) as backup_enabled_count,
  AVG(backup_retention_days) as avg_backup_retention_days

FROM database_resources
GROUP BY cloud_provider, database_type
ORDER BY cloud_provider, database_type;
```

### Storage Lifecycle Management

```sql
-- Storage lifecycle and data management analysis
SELECT
  cloud_provider,
  storage_type,

  -- Lifecycle Management
  COUNT(*) as total_resources,
  SUM(CASE WHEN lifecycle_rules_count > 0 THEN 1 ELSE 0 END) as with_lifecycle_rules,
  AVG(lifecycle_rules_count) as avg_lifecycle_rules,

  -- Data Protection
  SUM(CASE WHEN versioning_enabled = true THEN 1 ELSE 0 END) as versioning_enabled_count,
  SUM(CASE WHEN soft_delete_enabled = true THEN 1 ELSE 0 END) as soft_delete_enabled_count,
  AVG(soft_delete_retention_days) as avg_soft_delete_retention,

  -- Access Patterns
  SUM(CASE WHEN access_tier = 'Hot' OR access_tier = 'Standard' THEN 1 ELSE 0 END) as hot_tier_count,
  SUM(CASE WHEN access_tier = 'Cool' OR access_tier = 'Infrequent' THEN 1 ELSE 0 END) as cool_tier_count,
  SUM(CASE WHEN access_tier = 'Archive' OR access_tier = 'Glacier' THEN 1 ELSE 0 END) as archive_tier_count

FROM storage_resources
GROUP BY cloud_provider, storage_type
ORDER BY cloud_provider, total_resources DESC;
```

## IAM and Access Management

### IAM Resource Analysis

```sql
-- IAM resource distribution and security analysis
SELECT
  cloud_provider,
  iam_resource_type,
  COUNT(*) as total_resources,

  -- Activity Analysis
  SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_count,
  SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) as inactive_count,

  -- MFA Analysis (where applicable)
  SUM(CASE WHEN mfa_enabled = true THEN 1 ELSE 0 END) as mfa_enabled_count,
  SUM(CASE WHEN access_key_count > 0 THEN 1 ELSE 0 END) as with_access_keys,
  AVG(access_key_count) as avg_access_keys,

  -- Security Concerns
  SUM(CASE WHEN active_access_keys > 1 THEN 1 ELSE 0 END) as multiple_keys_count

FROM iam_resources
GROUP BY cloud_provider, iam_resource_type
ORDER BY cloud_provider, iam_resource_type;
```

### Privileged Access Review

```sql
-- Find IAM resources that may need review
SELECT
  cloud_provider,
  iam_resource,
  iam_resource_type,
  application,

  -- Risk Factors
  is_active,
  mfa_enabled,
  access_key_count,
  active_access_keys,

  -- Age Analysis
  create_date,
  password_last_used,
  TIMESTAMPDIFF(DAY, password_last_used, CURRENT_TIMESTAMP) as days_since_last_use,

  -- Risk Score
  CASE
    WHEN is_active = true AND mfa_enabled = false AND access_key_count > 1 THEN 'HIGH'
    WHEN is_active = true AND (mfa_enabled = false OR access_key_count > 1) THEN 'MEDIUM'
    WHEN is_active = false THEN 'LOW'
    ELSE 'REVIEW'
  END as risk_level

FROM iam_resources
WHERE iam_resource_type IN ('IAM User', 'ServiceAccount', 'Managed Identity')
ORDER BY
  CASE
    WHEN is_active = true AND mfa_enabled = false AND access_key_count > 1 THEN 1
    WHEN is_active = true AND (mfa_enabled = false OR access_key_count > 1) THEN 2
    ELSE 3
  END,
  days_since_last_use DESC NULLS LAST;
```

## Network Security Analysis

### Network Security Posture

```sql
-- Network security configuration analysis
SELECT
  cloud_provider,
  network_resource_type,
  COUNT(*) as total_resources,

  -- Security Analysis
  SUM(CASE WHEN has_open_ingress = true THEN 1 ELSE 0 END) as open_ingress_count,
  SUM(CASE WHEN rule_count = 0 THEN 1 ELSE 0 END) as no_rules_count,
  SUM(CASE WHEN is_default = true THEN 1 ELSE 0 END) as default_resources,

  -- Configuration Distribution
  AVG(rule_count) as avg_rule_count,
  MAX(rule_count) as max_rule_count

FROM network_resources
GROUP BY cloud_provider, network_resource_type
ORDER BY cloud_provider, network_resource_type;
```

## Performance and Monitoring Queries

### Resource Age and Lifecycle

```sql
-- Resource age analysis for lifecycle management
SELECT
  cloud_provider,
  'Kubernetes' as resource_type,
  cluster_name as resource_name,
  application,
  created_date,
  TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) as age_days,
  CASE
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 365 THEN 'Very Old (>1 year)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 180 THEN 'Old (6-12 months)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 90 THEN 'Mature (3-6 months)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 30 THEN 'Recent (1-3 months)'
    ELSE 'New (<1 month)'
  END as age_category
FROM kubernetes_clusters
WHERE created_date IS NOT NULL

UNION ALL

SELECT
  cloud_provider,
  'Storage' as resource_type,
  resource_name,
  application,
  created_date,
  TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) as age_days,
  CASE
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 365 THEN 'Very Old (>1 year)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 180 THEN 'Old (6-12 months)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 90 THEN 'Mature (3-6 months)'
    WHEN TIMESTAMPDIFF(DAY, created_date, CURRENT_TIMESTAMP) > 30 THEN 'Recent (1-3 months)'
    ELSE 'New (<1 month)'
  END as age_category
FROM storage_resources
WHERE created_date IS NOT NULL

ORDER BY age_days DESC;
```

## Custom Reports

### Executive Dashboard Query

```sql
-- One-row summary across all clouds
SELECT
  (SELECT COUNT(*) FROM kubernetes_clusters) as total_k8s_clusters,
  (SELECT COUNT(DISTINCT cloud_provider) FROM kubernetes_clusters) as kubernetes_providers,
  (SELECT COUNT(*) FROM storage_resources) as total_storage_resources,
  (SELECT COUNT(DISTINCT cloud_provider) FROM storage_resources) as storage_providers,
  (SELECT COUNT(*) FROM compute_resources) as total_compute_instances,
  (SELECT COUNT(DISTINCT cloud_provider) FROM compute_resources) as compute_providers,
  (SELECT SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) FROM kubernetes_clusters) as k8s_rbac_enabled,
  (SELECT SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) FROM storage_resources) as storage_encrypted
FROM (VALUES (1)) as t(x);
```

Each scan of a table calls the cloud APIs, and nothing is cached between scans. A query with
several subqueries on the same table can list its resources several times.
