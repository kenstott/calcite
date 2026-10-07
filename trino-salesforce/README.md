# Trino Salesforce Connector

A [Trino](https://trino.io) connector that exposes a Salesforce org's sObjects (`Account`,
`Contact`, `Opportunity`, custom `*__c` objects) as a SQL catalog. It is a thin wrapper over the
generic [trino-calcite](../trino-calcite) connector: it reuses `CalciteClient` for type mapping and
only swaps in the Salesforce JDBC driver
(`org.apache.calcite.adapter.salesforce.SalesforceDriver`) plus friendly catalog properties, so
users never write a raw JDBC URL.

## Requirements

Like any Trino plugin, it must be built against the exact SPI version of the target server — the
version pinned by `trino.version` in the repo's `gradle.properties` (currently **481**, **Java 25**).

## Download

Prebuilt plugin archives are attached to each `trino-v*` GitHub release. Grab
`trino-salesforce-plugin-<version>.zip` from the
[Releases page](https://github.com/kenstott/calcite/releases), or with the GitHub CLI:

```bash
gh release download <trino-vX.Y.Z> --repo kenstott/calcite --pattern 'trino-salesforce-plugin-*.zip'
```

The `Trino Connectors` workflow also publishes the archives as a `trino-connector-packages`
build artifact on each run.

## Build & install

Install the downloaded archive (or build from source) into the Trino server's `plugin/` directory,
then restart Trino:

```bash
# build from source (optional)
./gradlew :trino-salesforce:trinoPlugin
# install
unzip trino-salesforce/build/distributions/trino-salesforce-plugin-*.zip -d "$TRINO_HOME/plugin/"
# yields $TRINO_HOME/plugin/trino-salesforce/<jars>; restart Trino
```

## Configure a catalog

Create `etc/catalog/salesforce.properties`. The default is the OAuth **client credentials** flow:

```properties
connector.name=salesforce
# The org's My Domain URL (Setup → My Domain); login.salesforce.com does not work for this flow.
login-url=https://acme.my.salesforce.com
client-id=3MVG9...
client-secret=...
# Required: sObject names are mixed case (Account, OpportunityLineItem) and Trino lower-cases
# identifiers; the connector fails fast at startup without this. Keep it on.
case-insensitive-name-matching=true
```

The connected app needs *Enable Client Credentials Flow* checked and a *Run As* user set under its
OAuth policies; every query runs with that user's permissions.

| Property | Description | Required |
|----------|-------------|----------|
| `login-url` | Salesforce login URL; the org's My Domain URL for the client credentials flow | Yes |
| `case-insensitive-name-matching` | Must be `true` — see note above | Yes |
| `client-id` / `client-secret` | Connected app consumer key + secret | Conditional |
| `username` / `password` | OAuth username-password flow; also needs `client-id` + `client-secret`. Orgs created since Summer '23 block it by default | Conditional |
| `security-token` | User security token, appended to the password (username-password flow) | No |
| `access-token` / `instance-url` | A pre-issued OAuth access token and the org instance URL it belongs to | Conditional |
| `api-version` | REST API version, e.g. `v61.0` (adapter default `v58.0`) | No |
| `schema` | Schema name the sObjects are registered under (default `salesforce`) | No |
| `cache-max-size` | Maximum sObject describe results cached per connection (default 1000) | No |

Exactly one credential set is needed: `client-id` + `client-secret`, or `username` + `password`
with `client-id` + `client-secret`, or `access-token` + `instance-url`. The connector refuses to
start when none is complete.

These map onto a `jdbc:salesforce:loginUrl=…;clientId=…` URL for `SalesforceDriver`; see the
[salesforce adapter](../salesforce) for the underlying schema factory.

## Example queries

```sql
SHOW TABLES FROM salesforce.salesforce;
DESCRIBE salesforce.salesforce.account;
SELECT name, industry FROM salesforce.salesforce.account WHERE name LIKE 'A%' ORDER BY name LIMIT 10;
SELECT a.name, count(*) FROM salesforce.salesforce.contact c
JOIN salesforce.salesforce.account a ON c.accountid = a.id GROUP BY a.name;
```

## Behaviour and limitations

- **Pushdown.** Same as [trino-calcite](../trino-calcite) (it *is* that connector under the hood):
  Trino pushes projection and simple predicate filters down to the Calcite JDBC source. Below
  Calcite, the Salesforce adapter turns filters, projections, sorts and limits into SOQL; joins and aggregates run in Trino or Calcite.
- **Bind parameters are pushed into SOQL.** A comparison against a prepared statement parameter —
  which is how Trino delivers join dynamic filters — is bound into the SOQL `WHERE` clause at
  execution time, so a join against a small build side fetches only the matching rows.
- Columns and types come from each sObject's describe; authentication and describe caching are
  owned by the underlying Salesforce adapter.

## Testing

`TestSalesforceClientModule` covers catalog-property validation and URL assembly and runs with
`./gradlew :trino-salesforce:test`. `TestSalesforceConnector` is tagged `integration`: it starts an
in-process Trino server against a live org, reading `SF_LOGIN_URL`, `SF_CONSUMER_KEY` and
`SF_CONSUMER_SECRET` from `govdata/.env.prod`:

```bash
./gradlew :trino-salesforce:test -PincludeTags=integration
```
