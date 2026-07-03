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
set -e

# Determine script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# Load environment file if present (default to .env.prod for production)
ENV_FILE="${ENV_FILE:-.env.prod}"
if [[ -f "$GOVDATA_HOME/$ENV_FILE" ]]; then
    echo "Loading environment from $ENV_FILE"
    set -a
    source "$GOVDATA_HOME/$ENV_FILE"
    set +a
elif [[ -f "$ENV_FILE" ]]; then
    echo "Loading environment from $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
fi

# JVM options (4GB heap for 8GB+ devices, adjust as needed)
JVM_OPTS="${JVM_OPTS:--Xmx4g -Xms1g}"

# Additional JVM flags for better GC and crash diagnostics
JVM_FLAGS="-XX:+HeapDumpOnOutOfMemoryError \
           -XX:HeapDumpPath=$GOVDATA_HOME/runs/heapdump.hprof \
           -XX:ErrorFile=$GOVDATA_HOME/runs/hs_err_pid%p.log"

# Build classpath
if [[ -d "$GOVDATA_HOME/build/libs" ]]; then
    CLASSPATH="$GOVDATA_HOME/build/libs/*"
elif [[ -f "$GOVDATA_HOME/calcite-govdata-all.jar" ]]; then
    CLASSPATH="$GOVDATA_HOME/calcite-govdata-all.jar"
else
    echo "Error: Cannot find govdata JAR files."
    echo "Run './gradlew :govdata:shadowJar' first."
    exit 2
fi

# Create runs directory if needed
mkdir -p "$GOVDATA_HOME/runs"

# Run ETL
echo "Starting ETL Runner..."
echo "JVM_OPTS: $JVM_OPTS"
echo "CLASSPATH: $CLASSPATH"
echo ""

exec java $JVM_OPTS $JVM_FLAGS -cp "$CLASSPATH" \
    org.apache.calcite.adapter.govdata.etl.EtlRunner "$@"
