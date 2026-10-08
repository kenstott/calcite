# Permissions the Cloud Ops adapter needs

The adapter only reads. Give it read-only credentials in each cloud. A missing permission fails
the query that needs it with the cloud's own error message; it does not return an empty table.

This file keeps its original name. It now covers [AWS](#aws), [Azure](#azure) and [GCP](#gcp).

## AWS

### Simplest: the managed ReadOnlyAccess policy

The AWS-managed policy `ReadOnlyAccess` covers every action below.

```bash
aws iam attach-user-policy \
  --user-name <user> \
  --policy-arn arn:aws:iam::aws:policy/ReadOnlyAccess
```

### Minimal: a custom policy

The list matches the AWS SDK calls in `provider/AWSProvider.java`. Several S3 actions are named
differently from the API call that needs them; the table after the policy gives the mapping.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudOpsReadOnly",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeRegions",
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeAddresses",

        "eks:ListClusters",
        "eks:DescribeCluster",
        "eks:ListNodegroups",
        "eks:DescribeNodegroup",
        "eks:ListAddons",

        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation",
        "s3:GetBucketTagging",
        "s3:GetEncryptionConfiguration",
        "s3:GetBucketPublicAccessBlock",
        "s3:GetBucketVersioning",
        "s3:GetReplicationConfiguration",
        "s3:GetLifecycleConfiguration",
        "cloudwatch:GetMetricStatistics",

        "iam:ListUsers",
        "iam:ListUserTags",
        "iam:ListAccessKeys",
        "iam:ListMFADevices",
        "iam:ListRoles",
        "iam:ListRoleTags",
        "iam:ListPolicies",
        "iam:ListPolicyTags",
        "iam:ListInstanceProfiles",

        "rds:DescribeDBInstances",
        "rds:DescribeDBClusters",

        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "dynamodb:ListTagsOfResource",
        "dynamodb:DescribeContinuousBackups",

        "elasticache:DescribeCacheClusters",
        "elasticache:DescribeReplicationGroups",
        "elasticache:ListTagsForResource",

        "ecr:DescribeRepositories",
        "ecr:ListTagsForResource",
        "ecr:GetLifecyclePolicy"
      ],
      "Resource": "*"
    }
  ]
}
```

When `aws.roleArn` is set, the access key's own principal also needs `sts:AssumeRole` on each
role, and the policy above belongs on the roles.

### Which table needs which action

| Table | Actions |
|-------|---------|
| every regional table, when `aws.region` is absent or `all` | `ec2:DescribeRegions` |
| `compute_resources` | `ec2:DescribeInstances`, `ec2:DescribeVolumes` |
| `compute_security_groups` | `ec2:DescribeInstances` |
| `network_resources` | `ec2:DescribeVpcs`, `ec2:DescribeSecurityGroups`, `ec2:DescribeAddresses`, `ec2:DescribeSubnets` |
| `kubernetes_clusters` | `eks:ListClusters`, `eks:DescribeCluster`, `eks:ListNodegroups`, `eks:DescribeNodegroup`, `eks:ListAddons` |
| `storage_resources` | the `s3:` actions and `cloudwatch:GetMetricStatistics` |
| `iam_resources` | the `iam:` actions |
| `database_resources` | the `rds:`, `dynamodb:` and `elasticache:` actions |
| `container_registries` | the `ecr:` actions |

`storage_resources` makes a per-bucket call only for the columns a query selects, so a query that
leaves a column out does not need that column's action:

| Column | SDK call | IAM action |
|--------|----------|------------|
| any | `ListBuckets` | `s3:ListAllMyBuckets` |
| `region`, `size_bytes` | `GetBucketLocation` | `s3:GetBucketLocation` |
| `application`, `tags` | `GetBucketTagging` | `s3:GetBucketTagging` |
| `encryption_enabled`, `encryption_type`, `encryption_key_type` | `GetBucketEncryption` | `s3:GetEncryptionConfiguration` |
| `public_access_enabled`, `public_access_level` | `GetPublicAccessBlock` | `s3:GetBucketPublicAccessBlock` |
| `versioning_enabled` | `GetBucketVersioning` | `s3:GetBucketVersioning` |
| `replication_type` | `GetBucketReplication` | `s3:GetReplicationConfiguration` |
| `lifecycle_rules_count` | `GetBucketLifecycleConfiguration` | `s3:GetLifecycleConfiguration` |
| `size_bytes` | CloudWatch `GetMetricStatistics` | `cloudwatch:GetMetricStatistics` |

`SELECT *` needs all of them.

## Azure

Assign the built-in **Reader** role on each subscription in `azure.subscriptionIds`.

```bash
az role assignment create --assignee <app-id> --role Reader \
  --scope /subscriptions/<subscription-id>
```

Reader covers both things the adapter does:

- Azure Resource Graph queries, which every table uses. Resource Graph returns only resources the
  caller can read.
- Two Azure Resource Manager reads per storage account, used by `storage_resources` for
  versioning, soft delete and lifecycle rules: `<account>/blobServices/default` and
  `<account>/managementPolicies/default`.

`scripts/New-CloudOpsAzureCredentials.ps1` creates an app registration, assigns Reader, and writes
the credentials to an env file.

## GCP

Grant the service account the project-level **Viewer** role (`roles/viewer`) on each project in
`gcp.projectIds`, and enable the APIs the adapter calls:

| Table | API | Service to enable |
|-------|-----|-------------------|
| `kubernetes_clusters` | Kubernetes Engine: list clusters | `container.googleapis.com` |
| `storage_resources` | Cloud Storage: list buckets | `storage.googleapis.com` |
| `compute_resources` | Compute Engine: aggregated instance list | `compute.googleapis.com` |
| `network_resources` | Compute Engine: networks, firewalls, aggregated subnetworks | `compute.googleapis.com` |
| `iam_resources` | IAM: service accounts and their user-managed keys | `iam.googleapis.com` |
| `database_resources` | Cloud SQL Admin: instances | `sqladmin.googleapis.com` |
| `container_registries` | Artifact Registry: locations and repositories | `artifactregistry.googleapis.com` |

```bash
gcloud services enable container.googleapis.com storage.googleapis.com compute.googleapis.com \
  iam.googleapis.com sqladmin.googleapis.com artifactregistry.googleapis.com --project PROJECT_ID
```

The adapter does not call the Cloud Asset or Cloud Resource Manager APIs. `compute_security_groups`
returns no GCP rows.

## Checking the result

Run the live column audit ([TESTING.md](TESTING.md#live-column-audit)). It reads every table from
every configured cloud and fails on the first table that cannot be read, with the cloud's error.
