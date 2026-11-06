# pgwire-jira

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that serves a **Jira Cloud** site
(`JiraSchemaFactory`) over the PostgreSQL wire protocol, with `INSERT`, `UPDATE` and `DELETE`
allowed.

**Status: not yet run against a live site.** See the [adapter README](../jira/README.md).

This project contributes only [model.json](model.json). The server, the airgap bundling and the
launcher come from `pgwire-calcite`. The release builds `pgwire-jira-<version>-<os>.tar.gz`.

## Configure

Edit `model/model.json` in the extracted bundle:

- `baseUrl`: `https://<site>.atlassian.net`.
- `email`, `apiToken`: the account and an API token created at
  <https://id.atlassian.com/manage-profile/security/api-tokens>. Every query runs as that account.
- `forms`: the forms served as tables. The [adapter README](../jira/README.md) describes their
  shape; `_jira_forms` and `_jira_fields` list what can be chosen.
