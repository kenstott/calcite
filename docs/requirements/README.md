# Requirements Ledger

Business requirements for the fork's adapters, one file per Calcite adapter. The ledger is the
rebuild spec and the test-coverage matrix. See [../testing/strategy.md](../testing/strategy.md) for
the theory and [../testing/plan.md](../testing/plan.md) for the build-out plan.

## Files

| File | Adapter | ID prefix | Unit of work |
|---|---|---|---|
| [file.yaml](file.yaml) | `file` | `FILE-` | a matrix cell (format × engine × storageType × …) |
| [govdata.yaml](govdata.yaml) | `govdata` | `GOV-` | a schema (econ, sec, geo, …) |
| [splunk.yaml](splunk.yaml) | `splunk` | `SPLUNK-` | (TBD — after file/govdata methods proven) |
| [sharepoint-list.yaml](sharepoint-list.yaml) | `sharepoint-list` | `SPLIST-` | (TBD) |

IDs are `<PREFIX>-NNN`, unique within their file.

## Schema (per requirement)

```
id          <PREFIX>-NNN
status      proposed | accepted | in-progress | complete | rejected
group       coarse area (govdata: schema name; file: source-format or feature)
category    feature area
priority    MUST | SHOULD | MAY
type        behavioral | structural | constraint
description what the adapter guarantees
scenario    Given/When/Then (required for behavioral once accepted)
tests       golden test(s) that verify it (empty until re-covered)
supersedes  legacy test(s) this retires — the archive deletion gate (optional)
source      where it was reverse-engineered from (optional)
since       YYYY-MM (required when complete)
```

### file-track fields (`module: file` only — see strategy §5)

```
mode        read | write
cardinality 1:1 (simple format) | 1:N (complex format / walking source)
topology    single | walking
seam        bytes | catalog | json-set | table | execution   (what the test asserts up to)
applies     { engines: [...], storagetypes: [...] }           (cells the requirement holds for)
exceptions  [ { engine|storagetype|format: <v>, reason: <why> } ]  (accepted-unsupported cells)
```

## Lifecycle

`proposed` → `accepted` (reverse-engineered; feature exists, golden pending) → `complete` (green
golden linked in `tests:`). A `complete` MUST requirement must cite a test. An archived legacy test
may be deleted only once every requirement that `supersedes` it is `complete`.
