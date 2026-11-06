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
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A host cool-down mid-run stops the accession run once instead of failing every accession. */
@Tag("unit")
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

  /** Counts ERROR events logged by DocumentETLProcessor. */
  private static final class ErrorCounter extends AbstractAppender {
    final AtomicInteger errors = new AtomicInteger();

    ErrorCounter() {
      super("cooldown-test", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override public void append(LogEvent event) {
      if (event.getLevel() == Level.ERROR) {
        errors.incrementAndGet();
      }
    }
  }

  private static ErrorCounter capture() {
    LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
    Configuration cfg = ctx.getConfiguration();
    ErrorCounter appender = new ErrorCounter();
    appender.start();
    cfg.addAppender(appender);
    cfg.getLoggerConfig(DocumentETLProcessor.class.getName()).addAppender(appender, null, null);
    ctx.updateLoggers();
    return appender;
  }

  private static void release(ErrorCounter appender) {
    LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
    ctx.getConfiguration().getLoggerConfig(DocumentETLProcessor.class.getName())
        .removeAppender(appender.getName());
    ctx.updateLoggers();
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
