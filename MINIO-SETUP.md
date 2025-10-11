# MinIO Local S3 Server Setup

This setup provides a local S3-compatible storage server using MinIO for testing the govdata adapter with S3 storage.

## Prerequisites

1. **Docker Desktop** - Must be running
2. **MinIO Client (mc)** - Install with: `brew install minio/stable/mc`

## Quick Start

### 1. Start MinIO Server

```bash
./minio-setup.sh
```

This script will:
- Start MinIO container using Docker Compose
- Configure MinIO client with local endpoint
- Create `govdata-parquet` bucket
- Display connection information

### 2. Access MinIO Console

Open in browser: http://localhost:9001

Login credentials:
- Username: `minioadmin`
- Password: `minioadmin`

### 3. Configure Environment for govdata

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_OVERRIDE=http://localhost:9000
export GOVDATA_PARQUET_DIR=s3://govdata-parquet
export GOVDATA_CACHE_DIR=/Volumes/T9/govdata-production-cache
```

### 4. Test with sqlline

```bash
./sqlline --model=djia-production-model.json
```

## Manual Operations

### Start MinIO

```bash
docker-compose -f docker-compose-minio.yml up -d
```

### Stop MinIO

```bash
docker-compose -f docker-compose-minio.yml down
```

### View Logs

```bash
docker logs -f calcite-minio
```

### List Buckets

```bash
mc ls local-minio
```

### List Objects in Bucket

```bash
mc ls local-minio/govdata-parquet
```

### Copy Files to MinIO

```bash
mc cp /path/to/file.parquet local-minio/govdata-parquet/path/
```

### Copy Directory Recursively

```bash
mc cp --recursive /Volumes/T9/govdata-production/source=sec/ local-minio/govdata-parquet/source=sec/
```

## Storage Location

MinIO stores all data in `/Volumes/T9/minio-data/`

This directory structure mirrors S3 buckets:
```
/Volumes/T9/minio-data/
└── govdata-parquet/          # Bucket name
    ├── source=sec/            # Your data
    ├── source=econ/
    └── source=geo/
```

## Connection Details

- **API Endpoint**: http://localhost:9000
- **Console**: http://localhost:9001
- **Access Key**: minioadmin
- **Secret Key**: minioadmin
- **Default Bucket**: govdata-parquet

## Troubleshooting

### MinIO container won't start

Check if ports are already in use:
```bash
lsof -i :9000
lsof -i :9001
```

### Permission errors

Ensure `/Volumes/T9/minio-data` is writable:
```bash
mkdir -p /Volumes/T9/minio-data
chmod 755 /Volumes/T9/minio-data
```

### Reset MinIO completely

```bash
docker-compose -f docker-compose-minio.yml down
rm -rf /Volumes/T9/minio-data
./minio-setup.sh
```

## Migration from Local Files to MinIO

To migrate existing parquet files to MinIO:

```bash
# Start MinIO
./minio-setup.sh

# Copy SEC data
mc cp --recursive /Volumes/T9/govdata-production/source=sec/ \
  local-minio/govdata-parquet/source=sec/

# Copy ECON data
mc cp --recursive /Volumes/T9/govdata-production/source=econ/ \
  local-minio/govdata-parquet/source=econ/

# Copy GEO data
mc cp --recursive /Volumes/T9/govdata-production/source=geo/ \
  local-minio/govdata-parquet/source=geo/
```

## Model Configuration for S3

Update `djia-production-model.json` to use S3:

```json
{
  "operand": {
    "dataSource": "sec",
    "directory": "s3://govdata-parquet",
    "storageType": "s3",
    "storageConfig": {
      "region": "us-east-1",
      "endpoint": "http://localhost:9000",
      "pathStyleAccess": true
    }
  }
}
```

## AWS CLI Configuration

You can also use AWS CLI with MinIO:

```bash
aws configure --profile minio
# Access Key: minioadmin
# Secret Key: minioadmin
# Region: us-east-1

# List objects
aws s3 ls s3://govdata-parquet/ --profile minio --endpoint-url http://localhost:9000

# Copy file
aws s3 cp file.parquet s3://govdata-parquet/path/ --profile minio --endpoint-url http://localhost:9000
```
