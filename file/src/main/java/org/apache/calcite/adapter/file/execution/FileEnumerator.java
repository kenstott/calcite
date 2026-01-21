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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.FileReaderV2;
import org.apache.calcite.adapter.file.FileRowConverter;
import org.apache.calcite.linq4j.Enumerator;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.jsoup.select.Elements;

import java.util.Iterator;

/**
 * Wraps {@link FileReaderV2} and {@link FileRowConverter}, enumerates tr DOM
 * elements as table rows.
 */
public class FileEnumerator implements Enumerator<Object> {
  private final Iterator<Elements> iterator;
  private final FileRowConverter converter;
  private final int[] fields;
  private @Nullable Object current;

  public FileEnumerator(Iterator<Elements> iterator, FileRowConverter converter) {
    this(iterator, converter, identityList(converter.width()));
  }

  public FileEnumerator(Iterator<Elements> iterator, FileRowConverter converter,
      int[] fields) {
    this.iterator = iterator;
    this.converter = converter;
    this.fields = fields;
  }

  @Override public Object current() {
    if (current == null) {
      this.moveNext();
    }
    return current;
  }

  @Override public boolean moveNext() {
    try {
      if (this.iterator.hasNext()) {
        final Elements row = this.iterator.next();
        current = this.converter.toRow(row, this.fields);
        return true;
      } else {
        current = null;
        return false;
      }
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // required by linq4j Enumerator interface
  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  // required by linq4j Enumerator interface
  @Override public void close() {
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  private static int[] identityList(int n) {
    int[] integers = new int[n];

    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }

    return integers;
  }

}
