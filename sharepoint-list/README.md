# SharePoint List JDBC Driver

Query and update SharePoint Lists using standard SQL from DBeaver, DataGrip, Power BI, or any JDBC client.

## Download

Build the shadow JAR from this repo:

```bash
./gradlew :sharepoint-list:shadowJar
# Output: sharepoint-list/build/libs/calcite-sharepoint-list-*-all.jar
```

## Connect

**JDBC URL format:**
```
jdbc:sharepoint:siteUrl=<url>;authType=<type>;<auth-params>
```

**Driver class:** `org.apache.calcite.adapter.sharepoint.SharePointListDriver`

### Authentication examples

```
# Client credentials (app registration — recommended for automation)
jdbc:sharepoint:siteUrl=https://contoso.sharepoint.com/sites/mysite;authType=CLIENT_CREDENTIALS;clientId=xxx;clientSecret=xxx;tenantId=xxx

# Username/password (for interactive or testing)
jdbc:sharepoint:siteUrl=https://contoso.sharepoint.com/sites/mysite;authType=USERNAME_PASSWORD;user=me@contoso.com;password=xxx;clientId=xxx;tenantId=xxx

# Device code (prompts browser login)
jdbc:sharepoint:siteUrl=https://contoso.sharepoint.com/sites/mysite;authType=DEVICE_CODE;clientId=xxx;tenantId=xxx
```

## Connection parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `siteUrl` | Full SharePoint site URL | Yes |
| `authType` | `CLIENT_CREDENTIALS`, `USERNAME_PASSWORD`, `DEVICE_CODE`, `MANAGED_IDENTITY`, `CERTIFICATE` | Yes |
| `clientId` | Azure AD app registration client ID | Yes |
| `tenantId` | Azure AD tenant ID | Yes |
| `clientSecret` | Client secret (CLIENT_CREDENTIALS) | Conditional |
| `user` | UPN (USERNAME_PASSWORD) | Conditional |
| `password` | Password (USERNAME_PASSWORD) | Conditional |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:sharepoint:siteUrl=https://contoso.sharepoint.com/sites/mysite;authType=CLIENT_CREDENTIALS;clientId=...;clientSecret=...;tenantId=...`
3. **Driver JAR:** add `calcite-sharepoint-list-*-all.jar`
4. **Driver class:** `org.apache.calcite.adapter.sharepoint.SharePointListDriver`

## Sample queries

```sql
-- List all SharePoint lists as tables
SELECT table_name FROM information_schema.tables WHERE table_schema = 'sharepoint';

-- Query a list
SELECT Title, Status, AssignedTo, DueDate
FROM sharepoint."Project Tasks"
WHERE Status = 'In Progress'
ORDER BY DueDate;

-- Insert a new item
INSERT INTO sharepoint."Project Tasks" (Title, Status, AssignedTo)
VALUES ('New task', 'Not Started', 'jane@contoso.com');

-- Delete completed items
DELETE FROM sharepoint."Project Tasks" WHERE Status = 'Completed';
```

## Supported operations

| Operation | Supported |
|-----------|-----------|
| SELECT | Yes |
| INSERT | Yes |
| DELETE | Yes |
| UPDATE | Planned |
| CREATE TABLE | Yes (creates a SharePoint list) |
| DROP TABLE | Yes (deletes a SharePoint list) |

## Azure AD app registration

To use `CLIENT_CREDENTIALS` auth, register an app in Azure AD with:
- **API permissions:** `Sites.ReadWrite.All` (Microsoft Graph)
- **Grant type:** Application (not delegated)
- Generate a **client secret** and note the client ID and tenant ID
