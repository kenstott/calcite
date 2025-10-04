#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ================================
# REQUIRED: AWS S3 Configuration
# ================================
export AWS_REGION="us-east-1"  # Change to your preferred AWS region
export AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"  # Set your AWS access key
export AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"  # Set your AWS secret key
export GOVDATA_S3_BUCKET="your-bucket-name"  # Set your S3 bucket name

# ================================
# REQUIRED: Government Data APIs
# ================================
# FRED API Key (free registration at https://fred.stlouisfed.org/docs/api/api_key.html)
export FRED_API_KEY="your-fred-api-key"

# BLS API Key (free registration at https://www.bls.gov/developers/api_signature_v2.html)
export BLS_API_KEY="your-bls-api-key"

# BEA API Key (free registration at https://apps.bea.gov/API/signup/)
export BEA_API_KEY="your-bea-api-key"

# Census API Key (free registration at https://api.census.gov/data/key_signup.html)
export CENSUS_API_KEY="your-census-api-key"

# Additional API Keys from test environment
export FBI_API_KEY="your-fbi-api-key-here"
export NHTSA_API_KEY="your-nhtsa-api-key-here"
export FEMA_API_KEY="your-fema-api-key-here"
export OPENALEX_API_KEY="your-openalex-api-key-here"

# HUD Credentials (both token and basic auth for compatibility)
export HUD_TOKEN="your-hud-jwt-token"
export HUD_USERNAME="your-hud-username"
export HUD_PASSWORD="your-hud-password"

# Alpha Vantage API Key (from test environment)
export ALPHA_VANTAGE_KEY="your-alpha-vantage-key"

# ================================
# REQUIRED: Data Directories
# ================================
# Local cache directory for raw downloaded data
export GOVDATA_CACHE_DIR="/tmp/govdata-production-cache"

# Parquet directory logical path (actual storage in S3 configured via s3Config)
export GOVDATA_PARQUET_DIR="govdata-production"

# ================================
# CALCITE JDBC Configuration
# ================================
export CALCITE_MODEL_PATH="$(pwd)/djia-production-model.json"
export JDBC_URL="jdbc:calcite:model=${CALCITE_MODEL_PATH}"

# ================================
# JVM Configuration for Large Data Processing
# ================================
export JAVA_OPTS="-Xmx8g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200  -Dorg.apache.calcite.adapter.govdata.level=DEBUG"

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
echo "1. Verify API keys and AWS credentials above are correct"
echo "2. Build: ./gradlew assemble"
echo "3. Run: java \${JAVA_OPTS} -cp \"build/libs/*:file/build/libs/*:govdata/build/libs/*\" sqlline.SqlLine"
echo "4. Connect: !connect \${JDBC_URL}"
echo ""
echo "Data auto-loads on connection (first run may take hours)"
