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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Storage provider for Apache Iceberg tables.
 * This provider treats Iceberg tables as "files" in the storage abstraction.
 */
public class IcebergStorageProvider implements StorageProvider {
  private final Map<String, Object> config;
  private @Nullable Catalog catalog;

  public IcebergStorageProvider(Map<String, Object> config) {
    this.config = config;
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    List<FileEntry> entries = new ArrayList<>();

    // Initialize catalog if needed
    if (catalog == null) {
      String catalogType = (String) config.getOrDefault("catalogType", "hadoop");
      catalog = IcebergCatalogManager.getCatalogForProvider(catalogType, config);
    }

    // List tables in the catalog
    // Path format: /namespace or /namespace/table
    if (path.equals("/") || path.isEmpty()) {
      // List all namespaces as directories if catalog supports namespaces
      if (catalog instanceof SupportsNamespaces) {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        for (Namespace namespace : nsCatalog.listNamespaces()) {
          entries.add(
              new FileEntry(
              "/" + namespace.toString(),
              namespace.toString(),
              true,  // isDirectory
              0,
              System.currentTimeMillis()));
        }
      }
    } else {
      // Parse namespace from path
      String cleanPath = path.startsWith("/") ? path.substring(1) : path;
      String[] parts = cleanPath.split("/");

      if (parts.length == 1) {
        // List tables in namespace
        Namespace namespace = Namespace.of(parts[0]);
        for (TableIdentifier tableId : catalog.listTables(namespace)) {
          String tablePath = "/" + namespace + "/" + tableId.name();
          entries.add(
              new FileEntry(
              tablePath,
              tableId.name(),
              false,  // not a directory
              0,  // size unknown
              System.currentTimeMillis()));
        }
      }
    }

    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    // For Iceberg tables, return basic metadata
    return new FileMetadata(
        path,
        0,  // size not applicable
        System.currentTimeMillis(),
        "application/x-iceberg-table",
        null); // no etag
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Iceberg tables don't have a byte stream representation
    // Return empty stream or throw exception
    return new ByteArrayInputStream(new byte[0]);
  }

  @Override public Reader openReader(String path) throws IOException {
    // Iceberg tables don't have a text representation
    // Return empty reader
    return new StringReader("");
  }

  @Override public boolean exists(String path) throws IOException {
    if (catalog == null) {
      String catalogType = (String) config.getOrDefault("catalogType", "hadoop");
      catalog = IcebergCatalogManager.getCatalogForProvider(catalogType, config);
    }

    // Parse path to check if table exists
    String cleanPath = path.startsWith("/") ? path.substring(1) : path;
    String[] parts = cleanPath.split("/");

    if (parts.length == 2) {
      // Check if table exists
      Namespace namespace = Namespace.of(parts[0]);
      TableIdentifier tableId = TableIdentifier.of(namespace, parts[1]);
      return catalog.tableExists(tableId);
    } else if (parts.length == 1) {
      // Check if namespace exists
      try {
        Namespace namespace = Namespace.of(parts[0]);
        // Try to list tables in namespace - if it works, namespace exists
        catalog.listTables(namespace);
        return true;
      } catch (Exception e) {
        return false;
      }
    }

    return false;
  }

  @Override public boolean isDirectory(String path) throws IOException {
    // Namespaces are directories, tables are files
    String cleanPath = path.startsWith("/") ? path.substring(1) : path;
    String[] parts = cleanPath.split("/");
    return parts.length == 1;  // Single part = namespace = directory
  }

  @Override public String getStorageType() {
    return "iceberg";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    // Simple path resolution
    if (relativePath.startsWith("/")) {
      return relativePath;
    }

    if (basePath.endsWith("/")) {
      return basePath + relativePath;
    } else {
      return basePath + "/" + relativePath;
    }
  }
}
