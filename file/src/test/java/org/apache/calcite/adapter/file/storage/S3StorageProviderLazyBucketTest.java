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

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Constructing an {@link S3StorageProvider} must not touch the network: a bucket-exists HEAD at
 * construction blocked every server start (up to the SDK's 90s call timeout when the endpoint was
 * unreachable) although the start only reads (govdata-ops#985). The bucket is checked once,
 * before the first write.
 */
@Tag("unit")
class S3StorageProviderLazyBucketTest {

  private static Map<String, Object> config() {
    Map<String, Object> config = new HashMap<>();
    config.put("directory", "s3://some-bucket/prefix");
    return config;
  }

  @Test void constructionAndReadsMakeNoBucketCall() throws Exception {
    S3Client client = mock(S3Client.class);

    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());

    verify(client, never()).headBucket(any(HeadBucketRequest.class));
    assertEquals(0, provider.bucketCheckCount());
  }

  @Test void bucketIsCheckedOnceBeforeTheFirstWrite() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.putObject(any(PutObjectRequest.class),
        any(software.amazon.awssdk.core.sync.RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());

    provider.writeFile("s3://some-bucket/prefix/a.txt", new byte[] {1});
    provider.writeFile("s3://some-bucket/prefix/b.txt", new byte[] {2});

    verify(client, times(1)).headBucket(any(HeadBucketRequest.class));
    assertEquals(1, provider.bucketCheckCount());
  }
}
