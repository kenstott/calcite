# Jira adapter

Serves a Jira Cloud site's issues as SQL tables, read with JQL through REST v3 and written
through it. Package `org.apache.calcite.adapter.jira`; factory `JiraSchemaFactory`; JDBC URL
prefix `jdbc:jira:`.

**Status: not yet run against a live site.** The unit tests run the real client against
`FakeJira`, a local server that plays the documented REST v3.

## Operands

| Operand | Meaning |
|---|---|
| `baseUrl` | The site, `https://<site>.atlassian.net`. |
| `email`, `apiToken` | The account and its API token (basic auth), checked with `GET /rest/api/3/myself` at startup. |
| `forms` | The tables served: a list (or its JSON text) of `{"name", "project", "issueType", "fields": [{"field", "column"}]}`. |
| `pageSize` | Issues per search request; default 100. |
| `maxConcurrentRequests` | Default 4. |
| `maxRetries`, `maxRetryWaitSeconds` | Retries of a 429 within `Retry-After`; defaults 3 and 60. |
| `requestTimeoutSeconds` | Default 60. |

A form is one project (by key) and one issue type (by name or id). Its table has the fixed
columns `key`, `id`, `summary`, `status`, `created` and `updated`, then one column per field in
`fields`. A field is named by its id (`customfield_10016`) or its name (`Story Points`). Its column
is `column` when given; otherwise it is the field name snake-cased (`story_points`). A user field's
display name is a read-only field of its own, `<user field id>.displayName` (default column
`<column>_name`). Two fields that give one column are
refused, and so is a field whose column is a fixed one.

## Tables always served

- `_jira_forms(project_key, project_name, issue_type, issue_type_id, subtask, name)`: every
  project and issue type the account can create issues of. `name` is the default table name.
- `_jira_fields(project_key, issue_type, field_id, field_name, column_name, data_type, required,
  read_only, ordinal_position)`: the create-screen fields of one issue type, each user field
  followed by its `.displayName`. `read_only` marks a field no write may set. Query it with
  `WHERE project_key = '<key>' AND issue_type = '<issue type>'`.
- `issue_links(link_id, link_type, outward_key, inward_key)`: every link where either end is an
  issue of a served form, listed once. `outward_key` and `inward_key` are the link's `outwardIssue` and
  `inwardIssue`.

## Values

| Jira field | SQL value |
|---|---|
| text | text |
| rich text (description, paragraph fields) | plain text, one line per block; written as one paragraph per line |
| number | `DOUBLE` |
| date / datetime | `DATE` / `TIMESTAMP` (UTC) |
| select list | the option's value |
| cascading select | `parent / child` |
| priority, status, resolution, component, version | the name |
| user | the account id; its `.displayName` field holds the display name |
| labels, multi-selects, multi-user pickers | a JSON array of those values, read and written as text (pgwire has no array type) |
| anything else | its JSON text, read-only |

## What reaches Jira

- **Searches:** a form is read with `POST /search/jql`, paged by `nextPageToken`, asking only for
  the selected fields. A `LIMIT` stops the pages.
- **Keys:** `key = '<key>'` or `key IN (...)` fetches those issues with `POST /issue/bulkfetch`,
  so a key that doesn't exist returns no row instead of a JQL error.
- **Date and number comparisons** are sent as JQL. JQL compares datetimes to the minute and in
  the account's time zone, so a datetime bound is widened by a day.
- **Not sent:** equality on text, options, statuses and users. JQL rejects a value that names
  nothing, where SQL would just return no row.
- **ORDER BY** is evaluated by Calcite, and so is every filter, sent or not.

## Writes

- **INSERT** creates issues 50 per request (`POST /issue/bulk`) and reports their keys, so
  `RETURNING` works. It refuses a value for `status`, `key`, `id`, `created` or `updated`.
- **UPDATE** writes the changed fields with one `PUT /issue/<key>` per issue. A changed `status`
  is applied as the workflow transition that reaches it; if no transition does, the statement is
  refused and the error names the statuses that are reachable.
- **DELETE** deletes one issue at a time. If any issue has subtasks, the statement is refused
  before anything is deleted, and the error names the subtasks.
- **`issue_links`:** INSERT creates a link and reports its id, DELETE removes one, and UPDATE is
  refused.

There are no transactions. If a statement fails part way, what was already written stays
written, and the error says how many rows that was.

## Data Center

The REST version, the authorization and the rich-text format are set in one place
(`JiraFlavor`). Supporting Data Center (REST v2, a personal access token) means adding a second
flavour there, not a second code path.
