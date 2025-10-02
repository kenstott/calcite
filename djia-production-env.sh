#!/bin/bash
# DJIA Production Data Loading Environment Configuration
# Source this file before running the production data loading: source djia-production-env.sh

# ================================
# REQUIRED: AWS S3 Configuration
# ================================
export AWS_REGION="us-east-1"  # Change to your preferred AWS region
export AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"  # Set your AWS access key
export AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"  # Set your AWS secret key
export GOVDATA_S3_BUCKET="your-govdata-bucket"  # Set your S3 bucket name

# ================================
# REQUIRED: Government Data APIs
# ================================
# FRED API Key (free registration at https://fred.stlouisfed.org/docs/api/api_key.html)
export FRED_API_KEY="e0ea47affdc2e77a721f447d9ee0460e"

# BLS API Key (free registration at https://www.bls.gov/developers/api_signature_v2.html)
export BLS_API_KEY="2195EDB1-5226-4670-9274-FE859D5830DB"

# BEA API Key (free registration at https://apps.bea.gov/API/signup/)
export BEA_API_KEY="2195EDB1-5226-4670-9274-FE859D5830DB"

# Census API Key (free registration at https://api.census.gov/data/key_signup.html)
export CENSUS_API_KEY="3c9dec420bd08721863e0ba8fd64fa8fb43b802b"

# Additional API Keys from test environment
export FBI_API_KEY="your-fbi-api-key-here"
export NHTSA_API_KEY="your-nhtsa-api-key-here"
export FEMA_API_KEY="your-fema-api-key-here"
export OPENALEX_API_KEY="your-openalex-api-key-here"

# HUD Credentials (both token and basic auth for compatibility)
export HUD_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiOGQwMzY3ZTY0YzIzZWM2ZTNlMzA2NWE3MTk5NDEzNjhkNTA3YWIzZjFkZjFlNDdjZDgwODY1NmJlMTU5YzJhZDkyZGE0N2Y5NjgzNTI0MDMiLCJpYXQiOjE3NTc3NDkxNzUuOTg5MjIzLCJuYmYiOjE3NTc3NDkxNzUuOTg5MjI1LCJleHAiOjIwNzMyODE5NzUuOTg0MTY5LCJzdWIiOiIxMDg3NzQiLCJzY29wZXMiOltdfQ.d_j2J0GPtj53RCa-Qv0P7PyL3hk7U8cJo5q49au9flega2eh-su_2KiMiMMNRcI50rnL3OfbT1c0PeeCrA4zQw"
export HUD_USERNAME="ken@kenstott.com"
export HUD_PASSWORD="5:5e-FhB84hK8Dg"

# Alpha Vantage API Key (from test environment)
export ALPHA_VANTAGE_KEY="QT54JSOV2AGS77ZZ"

# ================================
# REQUIRED: Data Directories
# ================================
# Local cache directory for raw downloaded data
export GOVDATA_CACHE_DIR="/tmp/govdata-production-cache"

# S3 parquet directory for processed data persistence
export GOVDATA_PARQUET_DIR="s3://${GOVDATA_S3_BUCKET}/govdata-production"

# ================================
# CALCITE JDBC Configuration
# ================================
export CALCITE_MODEL_PATH="$(pwd)/djia-production-model.json"
export JDBC_URL="jdbc:calcite:model=${CALCITE_MODEL_PATH}"

# ================================
# JVM Configuration for Large Data Processing
# ================================
export JAVA_OPTS="-Xmx8g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# ================================
# Data Processing Configuration
# ================================
# Override default timeout for production use
export GOVDATA_DOWNLOAD_TIMEOUT_MINUTES=2147483647  # Max integer value = no timeout

# Enable DuckDB execution engine for performance
export CALCITE_EXECUTION_ENGINE="DUCKDB"

# ================================
# Create Required Directories
# ================================
mkdir -p "${GOVDATA_CACHE_DIR}"
# Note: GOVDATA_PARQUET_DIR is S3 URL - no local directory creation needed

echo "================================"
echo "DJIA Production Environment Configured"
echo "================================"
echo "Cache Directory: ${GOVDATA_CACHE_DIR}"
echo "Parquet Directory: ${GOVDATA_PARQUET_DIR}"
echo "S3 Bucket: ${GOVDATA_S3_BUCKET}"
echo "AWS Region: ${AWS_REGION}"
echo "Model Path: ${CALCITE_MODEL_PATH}"
echo "JDBC URL: ${JDBC_URL}"
echo ""
echo "Data Sources Configured:"
echo "  ✓ SEC: DJIA companies (2010-present, excludes 424B forms)"
echo "  ✓ ECON: FRED, BLS, BEA, Treasury (2010-present)"
echo "  ✓ GEO: Census TIGER/Line, demographic data"
echo ""
echo "Next Steps:"
echo "1. Update API keys and AWS credentials in this file"
echo "2. Run: java -cp 'calcite-*:file-*:govdata-*' sqlline.SqlLine"
echo "3. In sqlline: !connect \$JDBC_URL \"\" \"\" org.apache.calcite.jdbc.Driver"
echo "4. Execute verification queries from djia-production-verification.sql"