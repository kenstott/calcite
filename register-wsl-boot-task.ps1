<#
  register-wsl-boot-task.ps1 — Windows side of MinIO auto-start (Layer A).

  WSL is dormant until Windows invokes it, so nothing runs at boot on its own.
  This registers a Scheduled Task that, at logon, wakes the Ubuntu distro and
  starts the (systemd-enabled, native) minio.service. Once WSL is up, systemd's
  `enable`d minio.service keeps MinIO running with Restart=always.

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

# `wsl -d <distro> -u root -- systemctl start minio` boots the distro if it is
# down (systemd then auto-starts every enabled unit) AND explicitly ensures the
# service is up. -w hidden keeps no console window on logon.
$wsl = "$env:SystemRoot\System32\wsl.exe"
$action  = New-ScheduledTaskAction -Execute $wsl `
             -Argument "-d $Distro -u root -- systemctl start minio"

# AtLogOn for the current user: wsl.exe runs in the interactive session, which is
# the reliable way to boot a per-user WSL2 distro instance (an AtStartup/SYSTEM
# trigger can target a different session and is flaky for WSL).
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

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

Write-Host "Registered '$TaskName' (AtLogOn, user $env:USERNAME)."
Write-Host "Test now:  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Then in WSL:  systemctl status minio  &&  curl -s localhost:9002/minio/health/live"
Write-Host ""
Write-Host "Note: this fires at logon. If the machine sleeps/resumes without a new logon,"
Write-Host "WSL may have been torn down; a manual 'wsl -d $Distro -u root -- systemctl start minio'"
Write-Host "(or add an extra AtWorkstationUnlock trigger) re-wakes it."
