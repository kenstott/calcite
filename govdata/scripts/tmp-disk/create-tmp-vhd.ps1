<#
  create-tmp-vhd.ps1 - Windows side of the govdata temp-disk move. Run ONCE from an ELEVATED PowerShell:

      powershell -ExecutionPolicy Bypass -File create-tmp-vhd.ps1

  Creates a capped, dynamically-sized VHDX on the NVMe (C:), attaches it to WSL, and teaches the boot
  task C:\Scripts\start-minio.ps1 to re-attach it after every restart, the same way it already does for
  C:\WSL\pgwal.vhdx. Safe to re-run: an existing VHDX is reused and the boot script is patched once.

  Dynamic, not fixed: C: has ~145 GB free, and a fixed 120 GB file would take almost all of it up front.
  The cap (-SizeGB) is what keeps a runaway worker from filling C: -- the disk fills, not the drive.
#>
param(
  [string]$Path = 'C:\WSL\govdata-tmp.vhdx',
  [int]$SizeGB = 120,
  [string]$BootScript = 'C:\Scripts\start-minio.ps1'
)
$ErrorActionPreference = 'Stop'

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
  [Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { throw 'run this from an elevated (Administrator) PowerShell' }

if (Test-Path $Path) {
  Write-Host "reusing existing $Path"
} else {
  New-VHD -Path $Path -SizeBytes ($SizeGB * 1GB) -Dynamic -BlockSizeBytes 1MB | Out-Null
  Write-Host "created $Path ($SizeGB GB max, dynamic)"
}

# Boot task: attach the VHDX and include the new mountpoint in its post-mount check.
$text = Get-Content -Raw $BootScript
if ($text -match 'Govdata temp disk') {
  Write-Host "$BootScript already re-attaches the temp disk"
} else {
  Copy-Item $BootScript "$BootScript.bak-tmpdisk-$(Get-Date -Format yyyyMMdd-HHmmss)"
  $block = @'
# --- Govdata temp disk (capped dynamic VHDX on NVMe) ---
$tmpVhd = 'C:\WSL\govdata-tmp.vhdx'
if (Test-Path $tmpVhd) {
    Attach "TMP" @('--mount', '--vhd', $tmpVhd, '--bare')
} else {
    # The govdata services refuse to start without it (RequiresMountsFor), which is intended.
    Log "ERROR: $tmpVhd not found; govdata services will not start"
}

'@
  $marker = '# --- Mount and start services ---'
  if ($text.IndexOf($marker) -lt 0) { throw "marker '$marker' not found in $BootScript; patch it by hand" }
  $text = $text.Replace($marker, $block + $marker)
  $old = 'for m in /mnt/minio /mnt/pgwal; do'
  if ($text.IndexOf($old) -lt 0) { throw "mount-check loop not found in $BootScript; patch it by hand" }
  $text = $text.Replace($old, 'for m in /mnt/minio /mnt/pgwal /var/tmp/govdata; do')
  Set-Content -Path $BootScript -Value $text -Encoding UTF8
  Write-Host "patched $BootScript (backup alongside it)"
}

# Attach now so the WSL-side script can format it.
$out = (wsl --mount --vhd $Path --bare 2>&1 | Out-String) -replace "`0", ''
Write-Host $out
Write-Host 'Candidate devices (blank disks of about the right size):'
wsl -u root -e /bin/bash -c "lsblk -dno NAME,SIZE,TYPE,FSTYPE | awk '`$3==\"disk\" && `$4==\"\"'"
Write-Host "Next, in WSL:  sudo bash ~/calcite/govdata/scripts/tmp-disk/cutover-tmp-disk.sh --device /dev/<name>"
