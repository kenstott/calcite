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
package org.apache.calcite.adapter.sharepoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for SharePoint List API providers.
 * Abstracts the underlying API (Microsoft Graph or SharePoint REST)
 * to allow dual API support for both cloud and on-premises deployments.
 */
public interface SharePointListApiProvider {

  /**
   * Gets all lists available in the SharePoint site.
   *
   * @return List of SharePoint list information
   * @throws IOException if API call fails
   */
  List<SharePointListInfo> getListsInSite() throws IOException;

  /**
   * Gets the schema (columns) for a specific list.
   *
   * @param listId The ID of the list
   * @return Schema information for the list
   * @throws IOException if API call fails
   */
  ListSchemaInfo getListSchema(String listId) throws IOException;

  /**
   * Gets items from a SharePoint list with optional filtering and pagination.
   *
   * @param listId The ID of the list
   * @param filter OData filter expression (optional)
   * @param select Comma-separated list of fields to select (optional)
   * @param orderBy OData orderby expression (optional)
   * @param top Maximum number of items to return
   * @param skip Number of items to skip for pagination
   * @return List of items as maps of field names to values
   * @throws IOException if API call fails
   */
  List<Map<String, Object>> getListItems(String listId, String filter,
      String select, String orderBy, int top, int skip) throws IOException;

  /**
   * Gets a single item from a SharePoint list.
   *
   * @param listId The ID of the list
   * @param itemId The ID of the item
   * @return Item as a map of field names to values
   * @throws IOException if API call fails
   */
  Map<String, Object> getListItem(String listId, String itemId) throws IOException;

  /**
   * Creates a new item in a SharePoint list.
   *
   * @param listId The ID of the list
   * @param item Map of field names to values for the new item
   * @return ID of the created item
   * @throws IOException if API call fails
   */
  String createListItem(String listId, Map<String, Object> item) throws IOException;

  /**
   * Updates an existing item in a SharePoint list.
   *
   * @param listId The ID of the list
   * @param itemId The ID of the item to update
   * @param updates Map of field names to updated values
   * @throws IOException if API call fails
   */
  void updateListItem(String listId, String itemId, Map<String, Object> updates)
      throws IOException;

  /**
   * Deletes an item from a SharePoint list.
   *
   * @param listId The ID of the list
   * @param itemId The ID of the item to delete
   * @throws IOException if API call fails
   */
  void deleteListItem(String listId, String itemId) throws IOException;

  /**
   * Gets the API type this provider uses.
   *
   * @return "graph" for Microsoft Graph API, "rest" for SharePoint REST API
   */
  String getApiType();

  /**
   * Closes any resources held by this provider.
   */
  void close();

  /**
   * Simple data class for SharePoint list information.
   */
  class SharePointListInfo {
    private final String id;
    private final String name;
    private final String displayName;
    private final String description;

    public SharePointListInfo(String id, String name, String displayName, String description) {
      this.id = id;
      this.name = name;
      this.displayName = displayName;
      this.description = description;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getDisplayName() { return displayName; }
    public String getDescription() { return description; }
  }

  /**
   * Simple data class for list schema information.
   */
  class ListSchemaInfo {
    private final String name;
    private final String displayName;
    private final List<SharePointColumn> columns;

    public ListSchemaInfo(String name, String displayName, List<SharePointColumn> columns) {
      this.name = name;
      this.displayName = displayName;
      this.columns = columns;
    }

    public String getName() { return name; }
    public String getDisplayName() { return displayName; }
    public List<SharePointColumn> getColumns() { return columns; }
  }
}
