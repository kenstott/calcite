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
package org.apache.calcite.adapter.file.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A put or HEAD that times out, or arrives truncated, is repeated; one that never succeeds fails. */
@Tag("unit")
class S3StorageProviderWriteRetryTest {

  private static Map<String, Object> config() {
    Map<String, Object> config = new HashMap<>();
    config.put("directory", "s3://some-bucket/prefix");
    return config;
  }

  private static ApiCallTimeoutException timeout() {
    return ApiCallTimeoutException.create(90000);
  }

  private static S3Exception incompleteBody() {
    return (S3Exception) S3Exception.builder().statusCode(400)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("IncompleteBody").build()).build();
  }

  @Test void putTimingOutTwiceIsRepeatedAndSucceeds() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(timeout()).thenThrow(incompleteBody())
        .thenReturn(PutObjectResponse.builder().build());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.transientRetryBackoffMillis = 0;

    provider.writeFile("s3://some-bucket/prefix/t/.resume/k.done", new byte[] {'1'});

    verify(client, times(3)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test void putAlwaysTimingOutFailsAfterTheBoundedAttempts() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(timeout());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.transientRetryBackoffMillis = 0;

    IOException e = assertThrows(IOException.class,
        () -> provider.writeFile("s3://some-bucket/prefix/t/.resume/k.done", new byte[] {'1'}));
    assertTrue(e.getMessage().contains("Failed to write file to S3"));
    verify(client, times(5)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test void permanentErrorIsNotRepeated() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(403).build());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.transientRetryBackoffMillis = 0;

    assertThrows(IOException.class,
        () -> provider.writeFile("s3://some-bucket/prefix/t/.resume/k.done", new byte[] {'1'}));
    verify(client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
  }

  @Test void existsTimingOutOnceIsRepeated() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(timeout()).thenReturn(HeadObjectResponse.builder().build());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.transientRetryBackoffMillis = 0;

    assertTrue(provider.exists("s3://some-bucket/prefix/t/.resume/k.done"));
    verify(client, times(2)).headObject(any(HeadObjectRequest.class));
  }
}
