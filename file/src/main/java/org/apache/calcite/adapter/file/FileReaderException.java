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
package org.apache.calcite.adapter.file;

import java.sql.SQLException;

/**
 * Indicates that the FileReader failed, with the SQLState of the failure mode (FILE-151).
 */
public class FileReaderException extends SQLException {
  /** The document has no table the source names (no table, no match for the selector). */
  public static final String TABLE_NOT_FOUND = "42S02";
  /** The source's selector or index names something other than one table. */
  public static final String CONFIGURATION = "42000";
  /** The source could not be read. */
  public static final String FILE_ACCESS = "HY000";
  /** The source's storage location could not be reached. */
  public static final String STORAGE_CONNECT = "08001";

  FileReaderException(String message, String sqlState) {
    super(message, sqlState);
  }

  FileReaderException(String message, String sqlState, Throwable cause) {
    super(message, sqlState, cause);
  }
}
