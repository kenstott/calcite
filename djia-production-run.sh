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

# Force Java 17 for Hadoop compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# Load environment configuration
source "$(dirname "$0")/djia-production-env.sh"

# Build SqlLine classpath JAR if it doesn't exist
if [ ! -f "build/libs/sqllineClasspath.jar" ]; then
  echo "Building SqlLine classpath..."
  ./gradlew buildSqllineClasspath
fi

# Run SqlLine using the classpath JAR
exec java ${JAVA_OPTS} \
  -Dlog4j.configurationFile=govdata/src/test/resources/log4j2.xml \
  -jar build/libs/sqllineClasspath.jar \
  -u "jdbc:calcite:model=/Users/kennethstott/calcite/djia-production-model.json;lex=ORACLE;unquotedCasing=TO_LOWER" \
  -n "" -p ""
