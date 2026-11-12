/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketException;

import javax.net.ssl.SSLHandshakeException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A response body the server cuts off mid-stream (OpenFEMA's "Premature EOF", the Census flows
 * API's "Connection reset") is a transport fault on an idempotent GET and is retried, while an
 * ordinary failure still is not.
 */
@Tag("unit")
class HttpSourceTruncatedBodyRetryTest {

  private static boolean shouldRetry(IOException e) throws Exception {
    HttpSource source = new HttpSource(
        HttpSourceConfig.builder().url("https://api.example.com/data").build());
    Method m = HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class,
        HttpSourceConfig.RateLimitConfig.class);
    m.setAccessible(true);
    return (Boolean) m.invoke(source, e, HttpSourceConfig.RateLimitConfig.defaults());
  }

  @Test void prematureEofIsRetried() throws Exception {
    assertTrue(shouldRetry(new IOException("Premature EOF")));
  }

  @Test void eofExceptionIsRetried() throws Exception {
    assertTrue(shouldRetry(new EOFException()));
  }

  @Test void connectionResetIsRetried() throws Exception {
    assertTrue(shouldRetry(new SocketException("Connection reset")));
  }

  @Test void tlsStreamTruncatedMidRecordIsRetried() throws Exception {
    assertTrue(shouldRetry(new IOException("BUFFER_UNDERFLOW with EOF, 7481 bytes non decrypted.")));
  }

  @Test void handshakeTerminatedByRemoteHostWithResetCauseIsRetried() throws Exception {
    SSLHandshakeException e =
        new SSLHandshakeException("Remote host terminated the handshake");
    e.initCause(new SocketException("Connection reset"));
    assertTrue(shouldRetry(e));
  }

  @Test void handshakeTerminatedByRemoteHostIsRetried() throws Exception {
    assertTrue(shouldRetry(new SSLHandshakeException("Remote host terminated the handshake")));
  }

  @Test void resetNestedInCauseChainIsRetried() throws Exception {
    IOException outer = new IOException("request failed", new IOException("wrapped",
        new SocketException("Connection reset")));
    assertTrue(shouldRetry(outer));
  }

  @Test void otherHandshakeFailureIsNotRetried() throws Exception {
    SSLHandshakeException e = new SSLHandshakeException("PKIX path building failed");
    e.initCause(new java.security.cert.CertificateException("untrusted"));
    assertFalse(shouldRetry(e));
  }

  @Test void configuredStatusIsStillRetried() throws Exception {
    assertTrue(shouldRetry(new IOException("HTTP 503: unavailable")));
  }

  @Test void gatewayTimeoutIsRetriedWhenNotDeclared() throws Exception {
    HttpSource source = new HttpSource(
        HttpSourceConfig.builder().url("https://api.example.com/data").build());
    Method m = HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class,
        HttpSourceConfig.RateLimitConfig.class);
    m.setAccessible(true);
    HttpSourceConfig.RateLimitConfig declared = HttpSourceConfig.RateLimitConfig.fromMap(
        java.util.Collections.<String, Object>singletonMap("retryOn",
            java.util.Arrays.asList(403, 429, 503)));
    assertTrue((Boolean) m.invoke(source,
        new IOException("HTTP 504: <title>504 Gateway Time-out</title>"), declared));
    assertTrue((Boolean) m.invoke(source, new IOException("HTTP 502: bad gateway"), declared));
    assertTrue((Boolean) m.invoke(source, new IOException("HTTP 403: Forbidden"), declared));
    assertFalse((Boolean) m.invoke(source, new IOException("HTTP 404: nf"), declared));
  }

  @Test void unrelatedFailureIsNotRetried() throws Exception {
    assertFalse(shouldRetry(new IOException("HTTP 404: not found")));
    assertFalse(shouldRetry(new IOException("Unexpected character in response")));
  }
}
