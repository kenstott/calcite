#!/usr/bin/env bash
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
# Sourced by every script that loads a credentials file (.env.prod, .env.dq, .env.test, .env.sh).
#
# source_env_file FILE
#   Exports every assignment in FILE (CR-tolerant: these files are edited on Windows) with
#   xtrace switched off for the duration, then restores the caller's previous xtrace setting.
#   Under `bash -x` / `set -x` each `export NAME=value` the sourcing runs would otherwise be
#   written to the trace with its secret value. Callers must not pass a secret as a command-line
#   argument or echo it for the same reason.
source_env_file() {
  local _file="$1" _xtrace=0
  case $- in *x*) _xtrace=1; set +x ;; esac
  set -a
  # shellcheck disable=SC1090
  source <(tr -d '\r' < "$_file")
  set +a
  if [ "$_xtrace" = 1 ]; then set -x; fi
  return 0
}
