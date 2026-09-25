<#
  start-minio-with-disk.ps1 - Windows side of MinIO auto-start (Layer A), disk step.

  A `wsl --mount` attachment never survives a WSL/Windows restart, and nothing else
  in this repo re-attaches the govdata drive. Without this step, the scheduled task
  in register-wsl-boot-task.ps1 would call `systemctl start minio` against a disk
  that was never reattached - and MinIO's ExecStartPre guard only checks that a
  directory exists, not that a real filesystem is mounted there, so it would start
  anyway and silently serve/write an empty store from the WSL root disk instead of
  refusing to start. (install-minio-native-wsl.sh now also adds a mountpoint check
  to the guard itself as a second layer, but reattaching the disk here is what lets
  MinIO start against the *real* data at all.)

  Looks the disk up by FriendlyName rather than a hardcoded PHYSICALDRIVEn index,
  since physical drive numbers can shift across reboots when other disks are
  plugged in or removed.

  Also mounts /mnt/wsltmp (the isolated /var/tmp partition, sde2 on the same
  physical disk as MinIO's sde1) for the same reason: that attachment doesn't
  survive a restart either, and /var/tmp is bind-mounted from it.

  Called by the WSL-MinIO-Autostart scheduled task (RunLevel Highest, since
  `wsl --mount` requires Administrator rights). Safe to re-run: mounting an
  already-attached disk / mountpoint / running service are all no-ops.
#>

param(
  [string]$Distro     = "Ubuntu",
  [string]$DiskMatch  = "*MG08ACA16TE*"
)

$ErrorActionPreference = "Stop"

# Transcript so an unattended run leaves evidence of what it did and why it failed.
$logPath = Join-Path $env:LOCALAPPDATA "start-minio-with-disk.log"
Start-Transcript -Path $logPath -Append | Out-Null

try {
    $disk = Get-Disk | Where-Object { $_.FriendlyName -like $DiskMatch } | Select-Object -First 1
    if (-not $disk) {
        throw "no disk matching '$DiskMatch' found"
    }

    # `wsl --mount` needs Windows to have released the disk, so an Online disk must be
    # taken offline first.
    if (-not $disk.IsOffline) {
        Write-Host "start-minio-with-disk: disk $($disk.Number) is Online in Windows - setting it offline."
        Set-Disk -Number $disk.Number -IsOffline $true
    }

    $target = "\\.\PHYSICALDRIVE$($disk.Number)"
    Write-Host "start-minio-with-disk: attaching $target (disk $($disk.Number), '$($disk.FriendlyName)') to WSL..."
    # wsl.exe writes its diagnostics to stderr; under "Stop" Windows PowerShell 5.1 would turn
    # that into a terminating error before the exit code could be read. An already-attached
    # disk is reported this way, and is handled by the mount step below.
    $ErrorActionPreference = "Continue"
    $mountOutput = wsl --mount $target --bare 2>&1 | Out-String
    $mountExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    Write-Host "start-minio-with-disk: wsl --mount exit=$mountExit output=$mountOutput"

    # Mount through systemd's fstab-generated units, not a bare `mount`: a mount made from the
    # wsl.exe session is not visible to systemd, so minio.service's ExecStartPre mountpoint
    # check would still fail.
    Write-Host "start-minio-with-disk: mounting /mnt/minio, /mnt/wsltmp and /var/tmp via systemd in $Distro..."
    wsl -d $Distro -u root -- systemctl start mnt-minio.mount mnt-wsltmp.mount var-tmp.mount
    if ($LASTEXITCODE -ne 0) {
        throw "systemd could not mount the MinIO disk units in $Distro (wsl --mount exit=$mountExit): $mountOutput"
    }

    Write-Host "start-minio-with-disk: starting minio.service in $Distro..."
    wsl -d $Distro -u root -- systemctl start minio
    if ($LASTEXITCODE -ne 0) {
        throw "minio.service failed to start in $Distro - see: journalctl -u minio.service"
    }
    Write-Host "start-minio-with-disk: /mnt/minio mounted and minio.service started."
} finally {
    Stop-Transcript | Out-Null
}
