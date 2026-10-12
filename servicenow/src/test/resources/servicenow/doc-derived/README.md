# Documentation-derived fixtures

Everything under this directory was written by hand from ServiceNow's published documentation
(as summarised in `scratch/servicenow/assessment.md`). **None of it was captured from a ServiceNow
instance.** No instance was available when the adapter was written. The unit tests that read these
files prove that the adapter handles the shapes described below; they do not prove that a real
instance sends those shapes.

`tables/<table>.json` holds the rows of one table. `FixtureServer` (test sources) plays the Table
API over them. Rows are stored once and rendered per request:

- a plain field is a JSON string;
- a reference field is stored as `{"value": "<sys_id>", "display_value": "<text>"}`;
- with `sysparm_display_value=false` and `sysparm_exclude_reference_link=true` the server renders
  every field as a string (a reference as its sys_id);
- with `sysparm_display_value=all` it renders every field as `{"value": ..., "display_value": ...}`.

The server accepts only `sysparm_query` terms of the form `sys_id><key>` and `ORDERBYsys_id`, and
answers anything else with HTTP 400, so a filter that begins to be pushed down fails the tests.

## Response-shape assumptions that need a live instance

Each of these is encoded in the fixtures or in `FixtureServer`, and each is a guess until a real
instance confirms it. The adapter fails with a message naming the offending table, field and value
when reality differs; it does not adapt.

Metadata tables
1. `sys_db_object` returns `sys_id`, `name`, `label`, `super_class`; `super_class` is the sys_id of
   the parent table's `sys_db_object` row (not its name), and empty for a root table.
2. `sys_dictionary` returns `name` (table), `element` (column), `internal_type`, `max_length`,
   `active`. With display values off, `internal_type` is the type's name (`GUID`, `string`,
   `glide_date_time`), not a sys_id. `active` is `"true"` or `"false"`; `max_length` is a number
   as text or empty.
3. A table's own dictionary rows list only the columns it declares; inherited columns are on
   ancestor tables' rows. The row with an empty `element` describes the table and declares no column.
4. Every table has a `sys_id` column in the dictionary of itself or an ancestor.
5. `sys_glide_object` returns `name` and `scalar_type`; the scalar vocabulary the adapter maps is
   `string`, `integer`, `longint`, `decimal`, `float`, `boolean`, `GUID`, `reference`, `datetime`,
   `date`, `time` (and the documented field type names).
6. The integration user can read the three metadata tables (a missing role gives 401 or 403,
   which the adapter reports with the table name).

Row data
7. With `sysparm_exclude_reference_link=true` a reference field is a plain string. (The adapter
   also accepts `{"link": ..., "value": ...}` for a reference field.)
8. With `sysparm_display_value=all` every requested field is an object with `value` and
   `display_value`; `value` is the stored (UTC) value; a reference's `display_value` is the
   referenced record's display text; an empty reference is empty strings.
9. Empty values are `""`, never JSON null and never omitted. A field hidden by an ACL is expected
   to be omitted or empty; an omitted requested field is an error in the adapter.
10. Booleans are `"true"` / `"false"`; date-times `yyyy-MM-dd HH:mm:ss` in UTC; dates `yyyy-MM-dd`;
    times of day `1970-01-01 HH:mm:ss`; integers, decimals, `price` and `currency` plain numbers
    as text. `glide_duration` and `currency2` are passed through as text because their wire
    format is not known.
11. `sysparm_fields` limits the fields returned and `sys_id` is returned when asked for.

Paging
12. `sys_id><key>^ORDERBYsys_id` with `sysparm_limit` pages correctly: the comparison on a sys_id
    is accepted, and `ORDERBYsys_id` orders the same way `>` compares.
13. The end of data is an empty page. Whether a page whose whole `sysparm_limit` window is hidden
    by ACLs comes back empty while readable rows follow is unknown; if it does, a scan would end
    early with no error.
14. `sysparm_no_count=true` is accepted.

Errors and limits
15. A 429 carries `Retry-After` as a whole number of seconds. (HTTP-date form is rejected by the
    adapter with a message.)
16. Errors are JSON `{"error": {"message": ..., "detail": ...}, "status": "failure"}`.
17. A sleeping developer instance, a login redirect and a transaction quota that cuts a response
    short are assumed to show up as: HTML with HTTP 200, a non-200 status, or a JSON body that
    does not parse. What they really return is unknown.
18. A successful response carries `Content-Type: application/json`.
19. Basic authentication is enabled for the integration user (no MFA or SSO requirement on API
    calls).
