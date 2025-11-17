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
echo "Fixing BeaDataDownloader lambda expressions..."

# Fix lambda at line 538-544 (iterateWithParameters)
# This uses a custom iterateWithParameters method, needs different handling

# Fix lambda at line 646-670 (regional income conversion)
sed -i.bak '646s/(year, vars) ->/(cacheKey) ->/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '647a\          // Extract parameters from cache key\
          Map<String, String> vars = cacheKey.getParameters();\
          int year = Integer.parseInt(cacheKey.getParameter("year"));' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '670s/cacheManifest.markParquetConverted(tableName, year, fullVars, fullParquetPath);/cacheManifest.markParquetConverted(cacheKey, fullParquetPath);/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java

# Fix lambda at line 804-817 (ITA download)
sed -i.bak '804s/(year, vars) ->/(cacheKey) ->/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '805a\          // Extract parameters from cache key\
          Map<String, String> vars = cacheKey.getParameters();\
          int year = Integer.parseInt(cacheKey.getParameter("year"));' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '820s/cacheManifest.markCached(tableName, year, fullVars, cachedPath, fileSize);/cacheManifest.markCached(cacheKey, cachedPath, fileSize, year == java.time.LocalDate.now().getYear() ? System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24) : Long.MAX_VALUE, year == java.time.LocalDate.now().getYear() ? "current_year_daily" : "historical_immutable");/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java

# Fix lambda at line 857-872 (ITA conversion)
sed -i.bak '857s/(year, vars) ->/(cacheKey) ->/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '858a\          // Extract parameters from cache key\
          Map<String, String> vars = cacheKey.getParameters();' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '873s/cacheManifest.markParquetConverted(tableName, year, fullVars, fullParquetPath);/cacheManifest.markParquetConverted(cacheKey, fullParquetPath);/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java

# Fix lambda at line 908-922 (Industry GDP download)
sed -i.bak '908s/(year, vars) ->/(cacheKey) ->/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '909a\          // Extract parameters from cache key\
          Map<String, String> vars = cacheKey.getParameters();\
          int year = Integer.parseInt(cacheKey.getParameter("year"));' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java

# Fix lambda at line 954-970 (Industry GDP conversion)
sed -i.bak '954s/(year, vars) ->/(cacheKey) ->/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '955a\          // Extract parameters from cache key\
          Map<String, String> vars = cacheKey.getParameters();\
          int year = Integer.parseInt(cacheKey.getParameter("year"));' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java
sed -i.bak '971s/cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);/cacheManifest.markParquetConverted(cacheKey, fullParquetPath);/' govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java

echo "BeaDataDownloader lambda expressions fixed"
echo "Fixes completed!"
