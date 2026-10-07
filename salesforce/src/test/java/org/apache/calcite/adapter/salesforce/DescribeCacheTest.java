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
package org.apache.calcite.adapter.salesforce;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link DescribeCache}.
 */
class DescribeCacheTest {

  @TempDir Path directory;

  private final AtomicInteger loads = new AtomicInteger();

  private String load(String sObjectType) {
    return "{\"name\":\"" + sObjectType + "\",\"load\":" + loads.incrementAndGet() + "}";
  }

  private static List<Path> files(Path root) throws IOException {
    try (Stream<Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).collect(Collectors.toList());
    }
  }

  @Test void describeSurvivesANewCacheInstance() throws IOException {
    String first = new DescribeCache(directory, "org", Duration.ofHours(1))
        .get("Account", this::load);
    // A new instance stands in for a restarted process
    String second = new DescribeCache(directory, "org", Duration.ofHours(1))
        .get("Account", this::load);
    assertThat(second, equalTo(first));
    assertThat(loads.get(), equalTo(1));
  }

  @Test void expiredDescribeIsReloaded() throws IOException {
    DescribeCache cache = new DescribeCache(directory, "org", Duration.ofHours(1));
    cache.get("Account", this::load);
    Path file = files(directory).get(0);
    Files.setLastModifiedTime(file,
        FileTime.fromMillis(System.currentTimeMillis() - Duration.ofHours(2).toMillis()));
    assertThat(cache.get("Account", this::load),
        equalTo("{\"name\":\"Account\",\"load\":2}"));
    // The reload replaced the file, so the next read is served from disk again
    cache.get("Account", this::load);
    assertThat(loads.get(), equalTo(2));
  }

  @Test void scopesDoNotShareDescribes() throws IOException {
    new DescribeCache(directory, "org-a", Duration.ofHours(1)).get("Account", this::load);
    new DescribeCache(directory, "org-b", Duration.ofHours(1)).get("Account", this::load);
    assertThat(loads.get(), equalTo(2));
    assertThat(files(directory).size(), equalTo(2));
  }

  @Test void zeroTimeToLiveKeepsNothingOnDisk() throws IOException {
    DescribeCache cache = new DescribeCache(directory, "org", Duration.ZERO);
    cache.get("Account", this::load);
    cache.get("Account", this::load);
    assertThat(loads.get(), equalTo(2));
    assertThat(files(directory).size(), equalTo(0));
  }

  @Test void failedLoadWritesNothing() throws IOException {
    DescribeCache cache = new DescribeCache(directory, "org", Duration.ofHours(1));
    assertThrows(IOException.class,
        () -> cache.get("Account", name -> {
          throw new IOException("describe failed");
        }));
    assertThat(files(directory).size(), equalTo(0));
  }

  @Test void nameThatIsNotAnSObjectIsRejected() {
    DescribeCache cache = new DescribeCache(directory, "org", Duration.ofHours(1));
    assertThrows(IllegalArgumentException.class,
        () -> cache.get("../Account", this::load));
  }

  @Test void negativeTimeToLiveIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new DescribeCache(directory, "org", Duration.ofMinutes(-1)));
  }
}
