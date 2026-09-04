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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * How a fetch failure is classified decides whether a run reports it. A transport failure must
 * never resolve to SKIP: an unreachable host says nothing about the data, and skipping it lets a
 * whole feed go dark behind a "0 failed" run — which is what happened when USDA FIA stopped
 * answering and every state unit skipped on {@code Connection reset}.
 */
@Tag("unit")
public class EtlPipelineErrorActionTest {

  private static EtlPipelineConfig.ErrorHandlingConfig.ErrorAction classify(Throwable t)
      throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("determineErrorAction",
        Throwable.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    return (EtlPipelineConfig.ErrorHandlingConfig.ErrorAction) m.invoke(null, t, defaults());
  }

  private static EtlPipelineConfig.ErrorHandlingConfig defaults() {
    return EtlPipelineConfig.ErrorHandlingConfig.fromMap(new java.util.HashMap<String, Object>());
  }

  @Test void connectionResetIsNotSkipped() throws Exception {
    EtlPipelineConfig.ErrorHandlingConfig.ErrorAction action =
        classify(new SocketException("Connection reset"));
    assertNotEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, action,
        "an unreachable host must not be skipped as if the API had answered");
  }

  @Test void unreachableHostVariantsAllClassifyTransient() throws Exception {
    EtlPipelineConfig.ErrorHandlingConfig.ErrorAction expected =
        defaults().getTransientErrorAction();
    for (Throwable t : new Throwable[] {
        new SocketException("Connection reset"),
        new ConnectException("Connection refused"),
        new UnknownHostException("apps.fs.usda.gov"),
        new SocketTimeoutException("Read timed out"),
        new javax.net.ssl.SSLException("handshake failure")}) {
      assertEquals(expected, classify(t),
          t.getClass().getSimpleName() + " is a transport failure, not an API answer");
    }
  }

  /** The transport check walks the cause chain — the pipeline wraps fetch errors in IOException. */
  @Test void wrappedTransportFailureIsStillTransient() throws Exception {
    EtlPipelineConfig.ErrorHandlingConfig.ErrorAction expected =
        defaults().getTransientErrorAction();
    Throwable wrapped =
        new IOException("fetch failed", new SocketException("Connection reset"));
    assertEquals(expected, classify(wrapped), "a wrapped transport failure must still classify");
  }

  /** HTTP-status classification is unchanged: 404 still skips, 401/403 still fail. */
  @Test void httpStatusClassificationIsUnchanged() throws Exception {
    EtlPipelineConfig.ErrorHandlingConfig defaults = defaults();
    assertEquals(defaults.getNotFoundAction(), classify(new IOException("HTTP 404 from x")));
    assertEquals(defaults.getAuthErrorAction(), classify(new IOException("HTTP 403 from x")));
    assertEquals(defaults.getTransientErrorAction(), classify(new IOException("HTTP 503 from x")));
  }
}
