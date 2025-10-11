#!/bin/bash
# MinIO Local S3 Setup Script

set -e

MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET_NAME="govdata-parquet"

echo "=== MinIO Local S3 Setup ==="
echo ""

# Check if MinIO container is running
if ! docker ps | grep -q calcite-minio; then
    echo "Starting MinIO container..."
    docker-compose -f docker-compose-minio.yml up -d
    echo "Waiting for MinIO to be ready..."
    sleep 5
else
    echo "MinIO container already running"
fi

# Install mc (MinIO Client) if not present
if ! command -v mc &> /dev/null; then
    echo ""
    echo "MinIO Client (mc) not found. Install it with:"
    echo "  brew install minio/stable/mc"
    echo ""
    exit 1
fi

# Configure MinIO alias
echo "Configuring MinIO client alias..."
mc alias set local-minio $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Create bucket if it doesn't exist
if mc ls local-minio/$BUCKET_NAME &> /dev/null; then
    echo "Bucket '$BUCKET_NAME' already exists"
else
    echo "Creating bucket '$BUCKET_NAME'..."
    mc mb local-minio/$BUCKET_NAME
    echo "Bucket created successfully"
fi

# Set bucket to public-read (for testing)
echo "Setting bucket policy..."
mc anonymous set download local-minio/$BUCKET_NAME

echo ""
echo "=== MinIO Setup Complete ==="
echo ""
echo "MinIO API Endpoint:  $MINIO_ENDPOINT"
echo "MinIO Console:       http://localhost:9001"
echo "Access Key:          $MINIO_ACCESS_KEY"
echo "Secret Key:          $MINIO_SECRET_KEY"
echo "Bucket Name:         $BUCKET_NAME"
echo "Storage Location:    /Volumes/T9/minio-data"
echo ""
echo "To use with govdata, set environment variables:"
echo "  export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY"
echo "  export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY"
echo "  export AWS_ENDPOINT_OVERRIDE=$MINIO_ENDPOINT"
echo ""
echo "To stop MinIO:"
echo "  docker-compose -f docker-compose-minio.yml down"
echo ""
