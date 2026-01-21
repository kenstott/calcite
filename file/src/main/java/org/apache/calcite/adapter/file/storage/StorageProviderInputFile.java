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

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Parquet InputFile implementation that uses StorageProvider to access files.
 * This enables Parquet to read from S3, HTTP, and other storage systems via StorageProvider.
 */
public class StorageProviderInputFile implements InputFile {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageProviderInputFile.class);

  private final StorageProvider storageProvider;
  private final String path;
  private Long cachedLength;

  public StorageProviderInputFile(StorageProvider storageProvider, String path) {
    this.storageProvider = storageProvider;
    this.path = path;
  }

  @Override public long getLength() throws IOException {
    if (cachedLength == null) {
      // Get file size from storage provider
      LOGGER.debug("Getting file length for: {}", path);
      StorageProvider.FileMetadata metadata = storageProvider.getMetadata(path);
      cachedLength = metadata.getSize();
    }
    return cachedLength;
  }

  @Override public SeekableInputStream newStream() throws IOException {
    LOGGER.debug("Opening stream for: {}", path);
    return new StorageProviderSeekableInputStream(storageProvider, path);
  }

  /**
   * SeekableInputStream implementation using StorageProvider.
   * Parquet requires seekable streams to read file metadata and data pages.
   */
  private static class StorageProviderSeekableInputStream extends SeekableInputStream {
    private final StorageProvider storageProvider;
    private final String path;
    private byte[] cachedContent;
    private int position;

    StorageProviderSeekableInputStream(StorageProvider storageProvider, String path) throws IOException {
      this.storageProvider = storageProvider;
      this.path = path;
      this.position = 0;

      // Read entire file into memory
      // For large files, a more sophisticated approach with range requests would be better
      // but this works for now
      try (InputStream in = storageProvider.openInputStream(path)) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(chunk)) != -1) {
          buffer.write(chunk, 0, bytesRead);
        }
        cachedContent = buffer.toByteArray();
      }
    }

    @Override public long getPos() {
      return position;
    }

    @Override public void seek(long newPos) {
      if (newPos < 0 || newPos > cachedContent.length) {
        throw new IllegalArgumentException("Invalid seek position: " + newPos);
      }
      position = (int) newPos;
    }

    @Override public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (position + len > cachedContent.length) {
        throw new EOFException("End of stream reached");
      }
      System.arraycopy(cachedContent, position, bytes, start, len);
      position += len;
    }

    @Override public int read(ByteBuffer buf) throws IOException {
      if (position >= cachedContent.length) {
        return -1;
      }
      int len = Math.min(buf.remaining(), cachedContent.length - position);
      buf.put(cachedContent, position, len);
      position += len;
      return len;
    }

    @Override public void readFully(ByteBuffer buf) throws IOException {
      int len = buf.remaining();
      if (position + len > cachedContent.length) {
        throw new EOFException("End of stream reached");
      }
      buf.put(cachedContent, position, len);
      position += len;
    }

    @Override public int read() throws IOException {
      if (position >= cachedContent.length) {
        return -1;
      }
      return cachedContent[position++] & 0xFF;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      if (position >= cachedContent.length) {
        return -1;
      }
      int actualLen = Math.min(len, cachedContent.length - position);
      System.arraycopy(cachedContent, position, b, off, actualLen);
      position += actualLen;
      return actualLen;
    }

    @Override public void close() {
      // Nothing to close, content is in memory
    }
  }
}
