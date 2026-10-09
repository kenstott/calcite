<#
.SYNOPSIS
  Creates the read-only Azure credentials the cloud-ops adapter needs and writes them to the
  env file.

.DESCRIPTION
  Creates (or reuses) an app registration with a service principal, gives it the built-in
  "Reader" role on each subscription, creates a client secret, and fills in:

    CLOUDOPS_AZURE_TENANT_ID
    CLOUDOPS_AZURE_CLIENT_ID
    CLOUDOPS_AZURE_CLIENT_SECRET
    CLOUDOPS_AZURE_SUBSCRIPTION_IDS

  Needs the Azure CLI (az), signed in (az login) as someone who may register applications and
  is Owner or User Access Administrator on the subscriptions.

  Running it again with the same -Name keeps the app registration and its role assignments and
  adds a new client secret; earlier secrets keep working until they expire or are deleted in
  the portal (App registrations -> the app -> Certificates & secrets).

.PARAMETER SubscriptionIds
  Subscriptions to grant Reader on. Default: the Azure CLI's current subscription.

.PARAMETER Name
  Display name of the app registration.

.PARAMETER EnvFile
  Env file holding the four CLOUDOPS_AZURE_* lines. Default: govdata/.env.prod in this repo.

.PARAMETER YearsValid
  Lifetime of the client secret, in years.

.PARAMETER PrintOnly
  Print the four values instead of writing the env file. The secret is shown on screen.

.EXAMPLE
  pwsh cloud-ops/scripts/New-CloudOpsAzureCredentials.ps1

.EXAMPLE
  # In Azure Cloud Shell (already signed in; there is no env file there): print the values
  ./New-CloudOpsAzureCredentials.ps1 -PrintOnly

.EXAMPLE
  pwsh cloud-ops/scripts/New-CloudOpsAzureCredentials.ps1 -SubscriptionIds 1111-..., 2222-...
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string[]] $SubscriptionIds,
    [string] $Name = 'calcite-cloudops-readonly',
    [string] $EnvFile = (Join-Path $PSScriptRoot '..' '..' 'govdata' '.env.prod'),
    [ValidateRange(1, 2)]
    [int] $YearsValid = 1,
    [switch] $PrintOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$variables = @(
    'CLOUDOPS_AZURE_TENANT_ID',
    'CLOUDOPS_AZURE_CLIENT_ID',
    'CLOUDOPS_AZURE_CLIENT_SECRET',
    'CLOUDOPS_AZURE_SUBSCRIPTION_IDS'
)

# Runs the Azure CLI and returns its JSON output as objects; a failing call stops the script
# with the CLI's own message.
function Invoke-Az {
    param([Parameter(Mandatory)] [string[]] $Arguments)
    # Both streams through the pipeline (a file redirection would be skipped under -WhatIf):
    # stderr lines arrive as ErrorRecords, stdout lines as strings.
    $all = & az @Arguments --only-show-errors --output json 2>&1
    $failed = $LASTEXITCODE -ne 0
    $messages = @($all | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] })
    $output = @($all | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] })
    if ($failed) {
        throw "az $($Arguments[0..2] -join ' ') failed: $(($messages | Out-String).Trim())"
    }
    if ($output.Count -gt 0) { return ($output | Out-String | ConvertFrom-Json) }
}

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'The Azure CLI (az) is not installed: https://learn.microsoft.com/cli/azure/install-azure-cli'
}

$account = Invoke-Az @('account', 'show')
if (-not $SubscriptionIds) {
    $SubscriptionIds = @($account.id)
}
$tenantId = $account.tenantId

# Every subscription must exist, be visible to this login and belong to the same tenant: a
# service principal lives in one tenant and cannot be given a role in another.
$scopes = @(foreach ($subscriptionId in $SubscriptionIds) {
    $subscription = Invoke-Az @('account', 'show', '--subscription', $subscriptionId)
    if ($subscription.tenantId -ne $tenantId) {
        throw "Subscription $subscriptionId is in tenant $($subscription.tenantId), not $tenantId"
    }
    Write-Host "Subscription: $($subscription.name) ($($subscription.id))"
    "/subscriptions/$($subscription.id)"
})

if (-not $PrintOnly) {
    $EnvFile = (Resolve-Path $EnvFile).Path
    $lines = Get-Content $EnvFile
    foreach ($variable in $variables) {
        if (-not ($lines -match "^$variable=")) {
            throw "$EnvFile has no '$variable=' line to fill in"
        }
    }
}

$action = "Create app registration '$Name' with Reader on $($scopes.Count) subscription(s)"
if (-not $PSCmdlet.ShouldProcess($tenantId, $action)) {
    return
}

Write-Host "Signed in as $($account.user.name), tenant $tenantId"
Write-Host "Creating service principal '$Name' with the Reader role ..."
$arguments = @('ad', 'sp', 'create-for-rbac', '--name', $Name, '--role', 'Reader',
    '--years', "$YearsValid", '--scopes') + $scopes
$principal = Invoke-Az $arguments

$values = [ordered]@{
    CLOUDOPS_AZURE_TENANT_ID        = $principal.tenant
    CLOUDOPS_AZURE_CLIENT_ID        = $principal.appId
    CLOUDOPS_AZURE_CLIENT_SECRET    = $principal.password
    CLOUDOPS_AZURE_SUBSCRIPTION_IDS = ($SubscriptionIds -join ',')
}

if ($PrintOnly) {
    foreach ($variable in $values.Keys) {
        Write-Output "$variable=$($values[$variable])"
    }
    return
}

$updated = foreach ($line in $lines) {
    $variable = $variables | Where-Object { $line.StartsWith("$_=") }
    if ($variable) { "$variable=$($values[$variable])" } else { $line }
}
Set-Content -Path $EnvFile -Value $updated -Encoding utf8NoBOM

Write-Host ''
Write-Host "Wrote to ${EnvFile}:"
Write-Host "  CLOUDOPS_AZURE_TENANT_ID        = $($values.CLOUDOPS_AZURE_TENANT_ID)"
Write-Host "  CLOUDOPS_AZURE_CLIENT_ID        = $($values.CLOUDOPS_AZURE_CLIENT_ID)"
Write-Host "  CLOUDOPS_AZURE_CLIENT_SECRET    = (set, $($values.CLOUDOPS_AZURE_CLIENT_SECRET.Length) characters, not shown)"
Write-Host "  CLOUDOPS_AZURE_SUBSCRIPTION_IDS = $($values.CLOUDOPS_AZURE_SUBSCRIPTION_IDS)"
Write-Host ''
Write-Host 'The Reader role can take a few minutes to take effect.'
