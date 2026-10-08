# ServiceNow adapter

Reads a ServiceNow instance's tables through the Table API (`/api/now/table`) as SQL tables.
**Read-only**: there is no INSERT, UPDATE or DELETE.

## Status: nothing here has run against a ServiceNow instance

No live instance was available when this was written. What that means in practice:

- The unit tests (`./gradlew :servicenow:test`) run the real client code against a local HTTP
  server that plays the documented Table API over hand-written fixtures. The fixtures are derived
  from ServiceNow's documentation, not captured from an instance; their README
  (`src/test/resources/servicenow/doc-derived/README.md`) lists 19 response-shape assumptions that
  only a live instance can confirm. When reality differs, the adapter fails with a message naming
  the table, field and value; it does not adapt.
- The live tests (`ServiceNowIntegrationTest`, `ServiceNowProbe`, `PushdownHarnessLiveTest`) are
  written and compile, and skip themselves without credentials. They have never been run.
- **No filter is pushed to ServiceNow, because no pushdown entry is verified** (see below).

## What works (against the local stub)

- Table and column discovery from the instance's metadata tables `sys_db_object`, `sys_dictionary`
  and `sys_glide_object`, with inherited columns (`incident` extends `task`), cached on disk per
  instance and user. If the user cannot read the metadata tables, startup fails and says which
  table; columns are never guessed from sample rows.
- Keyset paging on `sys_id`; a scan ends only on an empty page; a page that does not advance is an
  error (see "Paging").
- Projection pushdown (`sysparm_fields`; `sys_id` is always added for paging).
- Rate limiting: a 429 is retried only as `Retry-After` allows, within `maxRetries` and
  `maxRetryWaitSeconds`; after that it is an error carrying the server's message.
- Type mapping, `NULL` handling, reference columns, `LIMIT` stopping the scan early.
- The pushdown translator and the harness that verifies it (text output tested offline).

## Configuration

Operands of `ServiceNowSchemaFactory` (also the `jdbc:servicenow:key=value;...` URL parameters, the
pgwire `model.json` operand and the Trino catalog properties):

| operand | meaning |
|---|---|
| `instanceUrl` (required) | `https://<instance>.service-now.com`; plain http only for localhost |
| `authType` (required) | `basic`. `oauth_client_credentials` is recognised and rejected as not implemented; the seam is `ServiceNowAuth` |
| `username`, `password` | for `basic`; use a dedicated least-privilege integration user |
| `tables` | only these tables (list or comma-separated); default: every table in `sys_db_object`. The table set always comes from discovery; keeping one SQL schema per application scope is a change in `ServiceNowSchema` and the catalog, not in the scan |
| `excludeColumnTypes` | field types whose columns are left out; a column of a type the adapter cannot map is otherwise an error on the table it belongs to |
| `pageSize` / `metadataPageSize` | rows per request, default 1000 |
| `maxConcurrentRequests` | default 2: REST traffic shares a small thread pool with the customer's other integrations |
| `maxRetries`, `maxRetryWaitSeconds` | 429 handling, defaults 3 and 60 |
| `requestTimeoutSeconds` | default 75 (ServiceNow's Table API quota is 60) |
| `catalogCacheDirectory`, `catalogCacheTtlMinutes` | default `~/.calcite/servicenow/catalog-cache`, 1440; 0 keeps nothing on disk |
| `pushdownVerification` | path of a pushdown verification record (below) |
| `trustPushdown` | pushdown entries to trust without a record (below) |

The integration user needs read access to the tables you expose and to `sys_db_object`,
`sys_dictionary` and `sys_glide_object` (for example the `personalize_dictionary` role). Tables
with "allow access to this table via web services" off are not reachable whatever the roles.

## Columns and types

One SQL table per ServiceNow table, named as in ServiceNow (lower case), with inherited columns:
`sys_id` first, then the rest by name. Field types follow `GlideTypes` (documented types, then
`sys_glide_object`'s scalar type for others; unknown is an error naming the type):
string-like to `VARCHAR(max_length)`, `sys_id` and reference to `VARCHAR(32)` (the assessment
proposed `CHAR(32)`; `CHAR` pads in Trino), `integer` INTEGER, `longint` BIGINT, `decimal`
DECIMAL(19,2), `currency`/`price` DECIMAL(19,4), `float` DOUBLE, `boolean` BOOLEAN,
`glide_date_time`/`due_date` TIMESTAMP (read as UTC), `glide_date` DATE, `glide_time` TIME,
`glide_duration`, `glide_list`, journal and password types as unparsed `VARCHAR`.

**Reference fields are two columns**: `caller_id` holds the referenced record's sys_id and
`caller_id__display` holds its display value (the user's name). The display column is read, with
`sysparm_display_value=all`, only when a query selects it, because display values are slower and
depend on the caller's locale and time zone. A table that already has a column named
`<field>__display` is an error.

**Empty is NULL, for every type including strings, and every column is nullable, `sys_id`
included** (an owner decision; the single conversion point is `ValueConverter`). The adapter never
returns `''`. The wire format cannot tell an empty string from an absent value, and a typed column
cannot hold `''`. Consequences: `x = ''`, `x <> ''` and `x IN ('', ...)` match nothing under SQL
and are always evaluated by Calcite, never pushed as `ISEMPTY`/`ISNOTEMPTY`; `IS NULL` is pushable
as `ISEMPTY`.

## Paging

Each page is `<pushed filter>^sys_id><last key>^ORDERBYsys_id` with `sysparm_limit`. ServiceNow
applies the limit before ACL checks, so a short page is not the end of data: the scan ends only on
an empty page (costing one extra request per scan). Whether an empty page can still precede
readable rows (a whole window hidden by ACLs) is unknown until tested live. A page that does not
advance past the previous key (what happens if the `sys_id>` term were silently dropped) is an
error instead of a repeat or a loop.

## Filter pushdown: off until verified

ServiceNow drops an invalid `sysparm_query` term and runs the rest ("fail open"), and several of
its comparisons differ from SQL. So a filter is sent to ServiceNow only if every *capability entry*
it needs is **verified**; everything else is evaluated by Calcite. Nothing is verified today, so no
filter is pushed. `ORDER BY` is never pushed (the collation of a ServiceNow sort is undocumented).
`LIMIT` is never sent as `sysparm_limit`; the scan is lazy, so a `LIMIT` stops the page fetches
early, which is correct whether filters were pushed, kept in Calcite or both. A limit is therefore
never applied before a filter that is still pending.

### Entries

An entry is `OP:TYPE:POSITION` (`PushdownCapabilities`): operators `EQ NE NE_NOT_EMPTY LT LE GT GE
IN NOT_IN NOT_IN_NOT_EMPTY IS_NULL IS_NOT_NULL LIKE_EXACT LIKE_PREFIX LIKE_SUFFIX LIKE_CONTAINS`,
types `TEXT NUMERIC BOOLEAN TIMESTAMP DATE REFERENCE GUID`, positions `AND` (top-level conjunct)
and `OR` (member of an `a^ORb` group). `NE_NOT_EMPTY` and `NOT_IN_NOT_EMPTY` are `!=` / `NOT IN`
plus an added `^<field>ISNOTEMPTY`, the form needed if ServiceNow's `!=` includes empty rows while
SQL's `<>` excludes NULL; the translator uses whichever is verified, added-term form first. Shape
entries: `SHAPE:AND` (terms joined with `^`, followed by the paging term), `SHAPE:OR_GROUP`,
`SHAPE:OR_WITH_OTHER_TERMS`, and `VALUE:SPECIAL` (values containing `= < > ! @ % * ' " \ ; & + # ,`
or edge spaces). `PushdownCapabilities.candidates()` lists them all.

What is translated: comparisons of a column with a literal, `IN` / `NOT IN`, one range (`BETWEEN`),
`IS [NOT] NULL`, boolean columns, `NOT` over those (by inverting the operator), `LIKE` patterns that
are exactly prefix, suffix, contains or equality, and an OR of such comparisons. Never pushed:
expressions, functions, casts of columns, display columns, dot-walked fields, `NOT LIKE`, `LIKE`
with `_`, an interior `%` or `ESCAPE`, OR over AND (would need `^NQ`), values containing `^`, a
comma inside an `IN` list, an empty-string literal, more than 100 `IN` values. Column names written
into a query come from the discovered metadata and are checked first; the API is never relied on
to reject a bad field.

### How an entry becomes verified

Either:

1. **A verification record** written by the live harness (`PushdownHarnessLiveTest`), copied to
   `src/main/resources/servicenow/pushdown-verification.json` (bundled, default) or pointed at
   with the `pushdownVerification` operand. Only `"status": "verified"` entries are enabled. A
   record that names an instance is accepted only for that instance. The bundled record is empty.
2. **`trustPushdown`**: entry names listed in the model. Explicit, logged at WARN, for people who
   accept the risk; an unknown name is an error.

An entry that is not verified is never pushed, silently or otherwise: the filter stays in Calcite.

### The differential harness

`PushdownHarness` / `PushdownCases` (test sources). For each case it runs the same SQL twice over
the same table, once with nothing trusted (ServiceNow returns everything; Calcite evaluates the
predicate with SQL three-valued logic, empties being NULL) and once with the case's entries trusted
(ServiceNow evaluates the encoded query, read through the real paging reader), and compares the
sets of `sys_id`s. Equal: MATCH. Different: MISMATCH, with the differing rows recorded, and the
entry stays unverified. An entry is verified only if all its cases match and the entries it
depends on are verified (negated forms, ranges and OR members also need `SHAPE:AND` / the OR
shapes). Entries no case exercises stay unverified.

Cases are built from the data in `incident` (so literals and boundaries exist) after the live
test seeds extra rows (prefix `ZZ_HARNESS_`, deleted in a `finally`; direct REST calls inside the
test, the adapter never writes): per column type equality, `!=` in both encodings (does `!=`
include empty rows?), `< <= > >=`, ranges at existing boundary values (time zone shows up as
missing/extra rows), `IN` with several values and one, `NOT IN`, `IS [NOT] NULL`, `NOT`, boolean
columns; for text also case-swapped values (`=`, `LIKE`, `IN`), `LIKE` shapes, `LIKE` patterns that
must be declined, values with `= , ' % _` and spaces, an empty literal; shapes (AND, one OR group,
AND with an OR group, two OR groups) and shapes that must be declined; every operator inside an OR
group. Information probes (`pushdown-info-probes.md`, verify nothing): fail-open for an invalid
field, operator and value (and so the `glide.invalid_query.returns_no_rows` setting in effect),
caret escaping, dot-walking, display-value literals, `BETWEEN`, `^NQ`, and the precedence of
`a^b^ORc`.

Two modes. **Live** (`PushdownHarnessLiveTest`, tag `integration`, skipped without credentials)
is the only one that can write a record. **Offline** (`PushdownHarnessTest`) runs the same code
against the local stub: it checks that the translator pushes or declines each case as the case
says and that the plumbing (comparison, verdicts, record) works, and it **cannot** mark anything
verified or write a record, because the stub is this project's own reading of the documentation.

Run it: put `SN_INSTANCE_URL`, `SN_USERNAME`, `SN_PASSWORD` in `govdata/.env.prod`, then
`./gradlew :servicenow:test -PincludeTags=integration --tests '*PushdownHarnessLive*'`. Use a
developer instance (it writes). Reports and the record land in `servicenow/build/servicenow-probe/`.
Review `pushdown-harness.md` before copying the record: a MATCH on a table with few rows is weak
evidence.

## Not done

Writes; OAuth (only the seam); aggregate pushdown (the Aggregate API); `ORDER BY` / `LIMIT`
pushdown; joins; Batch API; choice labels; CMDB routing; attachments. Journal fields read as NULL
(their entries live in `sys_journal_field`).

## Terms

Not affiliated with or endorsed by ServiceNow, Inc. Use a dedicated integration user; do not use
the adapter for whole-instance replication.
