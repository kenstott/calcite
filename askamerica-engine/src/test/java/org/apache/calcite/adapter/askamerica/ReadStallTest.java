/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A bare "Read timed out" becomes an error naming the object, waits, retries and remedy. */
@Tag("unit")
class ReadStallTest {
  private static final String DUCKDB_TIMEOUT =
      "IO Error: Unable to connect to URL "
      + "\"https://acct.r2.cloudflarestorage.com/govdata-parquet-v1/law/lobbying_activities/"
      + "metadata/v12.metadata.json?X-Amz-Signature=abcdef123&X-Amz-Credential=AKIASECRET\": "
      + "Read timed out (Read timed out)";

  @Test void duckdbTimeoutNamesObjectWithoutSecrets() {
    String m = McpServer.compactErrorMessage(new SQLException(DUCKDB_TIMEOUT), 33_000);
    assertTrue(m.contains("Object: https://acct.r2.cloudflarestorage.com/govdata-parquet-v1/law/"
        + "lobbying_activities/metadata/v12.metadata.json."), m);
    assertFalse(m.contains("abcdef123"), m);
    assertFalse(m.contains("AKIASECRET"), m);
    assertFalse(m.contains("X-Amz"), m);
    assertTrue(m.contains("One read waits 12s"), m);
    assertTrue(m.contains("retries each read 6 times"), m);
    assertTrue(m.contains("Time spent on this call: 33s"), m);
    assertTrue(m.contains("narrow it with a year or state filter"), m);
    assertTrue(m.contains("Cause: "), m);
  }

  @Test void describeTableStepIsNamed() {
    Exception e = new ReadStall.StepFailure("describe_table step 'columns'", 31_000,
        new SQLException(DUCKDB_TIMEOUT));
    String m = McpServer.compactErrorMessage(e);
    assertTrue(m.contains("Stalled step: describe_table step 'columns'"), m);
    assertTrue(m.contains("Time spent on this call: 31s"), m);
  }

  @Test void unknownObjectIsSaidNotGuessed() {
    String m = McpServer.compactErrorMessage(new RuntimeException("wrapped",
        new SocketTimeoutException("Read timed out")));
    assertTrue(m.contains("Object: object unknown."), m);
    assertFalse(m.contains("Time spent"), m);
  }

  @Test void fileNameIsUsedWhenNoUrl() {
    assertEquals("/data/law/part-0.parquet",
        ReadStall.objectIn("IO Error: could not read /data/law/part-0.parquet: Read timed out"));
  }

  @Test void userInfoAndQueryAreStripped() {
    assertEquals("https://h/b/k", ReadStall.stripUrl("https://user:pw@h/b/k?sig=1#f"));
    assertNull(ReadStall.describe(new RuntimeException("Binder Error: no such column"), 1));
  }
}
