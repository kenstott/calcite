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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A host cool-down mid-run stops the accession run once instead of failing every accession. */
@Tag("unit")
@ResourceLock("DocumentETLProcessor.loggerConfig")
class DocumentETLProcessorCooldownTest {
  private static final int ACCESSIONS = 2000;
  private static final int REFUSED_AFTER = 10;

  /** Processor whose limiter starts refusing after {@link #REFUSED_AFTER} accessions. */
  private static final class RefusingProcessor extends DocumentETLProcessor {
    final AtomicInteger calls = new AtomicInteger();

    RefusingProcessor() {
      super(HttpSourceConfig.builder().url("http://localhost/").build(), new LocalFileStorageProvider(), "/tmp", "/tmp",
          null);
    }

    @Override public DocumentETLResult processAccession(String cik, String accession,
        String formType, String filingDate) {
      if (calls.incrementAndGet() > REFUSED_AFTER) {
        throw new CrossProcessRateLimiter.HostCooldownException(
            "sec.gov refused this host with HTTP 403/429; no requests until later");
      }
      return new DocumentETLResult(1, 0, 0, new ArrayList<String>(), new ArrayList<String>(), 0);
    }
  }

  private static List<DocumentETLProcessor.AccessionRef> accessions() {
    List<DocumentETLProcessor.AccessionRef> refs =
        new ArrayList<DocumentETLProcessor.AccessionRef>();
    for (int i = 0; i < ACCESSIONS; i++) {
      refs.add(new DocumentETLProcessor.AccessionRef("0000000001", "0000000001-26-" + i, "10-K",
          "2026-01-01"));
    }
    return refs;
  }

  /**
   * Counts the host cool-down ERROR events logged by DocumentETLProcessor. The logger is shared
   * by every test in the JVM and tests run concurrently, so other classes' errors must not count.
   */
  private static final class ErrorCounter extends AbstractAppender {
    final AtomicInteger errors = new AtomicInteger();

    ErrorCounter() {
      super("cooldown-test-" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY);
    }

    @Override public void append(LogEvent event) {
      if (event.getLevel() == Level.ERROR
          && event.getMessage().getFormattedMessage().startsWith("Host cool-down:")) {
        errors.incrementAndGet();
      }
    }
  }

  private static org.apache.logging.log4j.core.Logger processorLogger() {
    // The first SLF4J lookup in the JVM starts the log4j context on whichever thread gets there
    // first, and starting it replaces the configuration, which stops any appender attached
    // before that. Wait until the context is fully started before attaching the appender.
    org.slf4j.LoggerFactory.getLogger(DocumentETLProcessor.class);
    org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)
        org.apache.logging.log4j.LogManager.getLogger(DocumentETLProcessor.class);
    LoggerContext ctx = logger.getContext();
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
    while (!ctx.isStarted()) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("log4j context did not start: " + ctx.getState());
      }
      Thread.yield();
    }
    return logger;
  }

  private static ErrorCounter capture() {
    ErrorCounter appender = new ErrorCounter();
    appender.start();
    processorLogger().addAppender(appender);
    return appender;
  }

  private static void release(ErrorCounter appender) {
    processorLogger().removeAppender(appender);
    appender.stop();
  }

  @Test void parallelRunStopsOnceAndReportsNotAttempted() {
    ErrorCounter appender = capture();
    try {
      RefusingProcessor processor = new RefusingProcessor();
      CrossProcessRateLimiter.HostCooldownException e =
          assertThrows(CrossProcessRateLimiter.HostCooldownException.class,
              () -> processor.processAccessionsParallel(accessions(), 4));
      assertTrue(e.getMessage().contains("refused this host with HTTP 403/429"));
      assertEquals(1, appender.errors.get());
      // Tasks queued behind the first refusal never reach processAccession.
      assertTrue(processor.calls.get() < ACCESSIONS / 2, "calls=" + processor.calls.get());
    } finally {
      release(appender);
    }
  }

  @Test void serialRunStopsOnceAndLogsOneLine() {
    ErrorCounter appender = capture();
    try {
      RefusingProcessor processor = new RefusingProcessor();
      assertThrows(CrossProcessRateLimiter.HostCooldownException.class,
          () -> processor.processAccessions(accessions()));
      assertEquals(REFUSED_AFTER + 1, processor.calls.get());
      assertEquals(1, appender.errors.get());
    } finally {
      release(appender);
    }
  }
}
