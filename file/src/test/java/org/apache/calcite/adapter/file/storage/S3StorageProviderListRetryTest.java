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

import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A LIST page that times out is requested again; a page that never answers fails the listing. */
@Tag("unit")
class S3StorageProviderListRetryTest {

  private static Map<String, Object> config() {
    Map<String, Object> config = new HashMap<>();
    config.put("directory", "s3://some-bucket/prefix");
    return config;
  }

  private static ApiCallTimeoutException timeout() {
    return ApiCallTimeoutException.create(90000);
  }

  @Test void pageTimingOutOnceIsRetriedAndTheListingCompletes() throws Exception {
    S3Client client = mock(S3Client.class);
    ListObjectsV2Response page = ListObjectsV2Response.builder()
        .contents(S3Object.builder().key("prefix/t/data/a.parquet").size(1L).build())
        .isTruncated(false).build();
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenThrow(timeout()).thenReturn(page);
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.listRetryBackoffMillis = 0;

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://some-bucket/prefix/t/", true);

    assertEquals(1, entries.size());
    verify(client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test void pageAlwaysTimingOutFailsTheListingInsteadOfReturningNothing() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(timeout());
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());
    provider.listRetryBackoffMillis = 0;

    assertThrows(IOException.class,
        () -> provider.listFiles("s3://some-bucket/prefix/t/", true));
    verify(client, times(5)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test void namePrefixListingSendsTheKeyPrefixAndDelimiterToTheStore() throws Exception {
    S3Client client = mock(S3Client.class);
    ListObjectsV2Response page = ListObjectsV2Response.builder()
        .contents(S3Object.builder().key("prefix/t/metadata/v3.metadata.json").size(1L).build())
        .isTruncated(false).build();
    org.mockito.ArgumentCaptor<ListObjectsV2Request> captor =
        org.mockito.ArgumentCaptor.forClass(ListObjectsV2Request.class);
    when(client.listObjectsV2(captor.capture())).thenReturn(page);
    S3StorageProvider provider = S3StorageProvider.forTesting(client, config());

    List<StorageProvider.FileEntry> entries =
        provider.listFilesWithNamePrefix("s3://some-bucket/prefix/t/metadata", "v");

    assertEquals(1, entries.size());
    assertEquals("prefix/t/metadata/v", captor.getValue().prefix());
    assertEquals("/", captor.getValue().delimiter());
  }
}
