<#
  register-wsl-boot-task.ps1 - Windows side of MinIO auto-start (Layer A).

  WSL is dormant until Windows invokes it, so nothing runs at boot on its own.
  This registers a Scheduled Task that, at logon, runs start-minio-with-disk.ps1,
  which reattaches the govdata drive (a wsl --mount attachment never survives a
  WSL/Windows restart) and only then wakes the Ubuntu distro and starts the
  (systemd-enabled, native) minio.service. Once WSL is up, systemd's `enable`d
  minio.service keeps MinIO running with Restart=always.

  Runs at RunLevel Highest (not Limited) because `wsl --mount` requires
  Administrator rights; Task Scheduler grants this without a UAC prompt for an
  admin user's own logon task.

  Run ONCE in an elevated PowerShell:
    powershell -ExecutionPolicy Bypass -File .\register-wsl-boot-task.ps1

  Undo:
    Unregister-ScheduledTask -TaskName "WSL-MinIO-Autostart" -Confirm:$false
#>

param(
  [string]$Distro   = "Ubuntu",
  [string]$TaskName = "WSL-MinIO-Autostart"
)

$ErrorActionPreference = "Stop"

$diskScript = Join-Path $PSScriptRoot "start-minio-with-disk.ps1"
if (-not (Test-Path $diskScript)) {
  throw "start-minio-with-disk.ps1 not found next to this script at $diskScript"
}

# Runs start-minio-with-disk.ps1, which reattaches the govdata disk (needs admin
# rights) before waking the distro and starting minio.service.
$powershell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$action  = New-ScheduledTaskAction -Execute $powershell `
             -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$diskScript`" -Distro $Distro"

# AtLogOn for the current user: this runs in the interactive session, which is
# the reliable way to boot a per-user WSL2 distro instance (an AtStartup/SYSTEM
# trigger can target a different session and is flaky for WSL).
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

# Let it run on battery, don't kill it, retry a few times if WSL is slow to init.
$settings = New-ScheduledTaskSettingsSet `
              -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
              -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
              -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Write-Host "Replacing existing task '$TaskName'..."
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName `
  -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
  -Description "Boot WSL ($Distro) at logon and start native minio.service (govdata object store)." | Out-Null

Write-Host "Registered '$TaskName' (AtLogOn, user $env:USERNAME, RunLevel Highest)."
Write-Host "Test now:  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Then in WSL:  mountpoint -q /mnt/minio && systemctl status minio && curl -s localhost:9002/minio/health/live"
Write-Host ""
Write-Host "Note: this fires at logon. If the machine sleeps/resumes without a new logon,"
Write-Host "the disk attachment and WSL may have been torn down; a manual"
Write-Host "'powershell -File $diskScript' (or add an extra AtWorkstationUnlock trigger) re-wakes both."
