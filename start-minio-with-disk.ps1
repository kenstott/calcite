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
  [string]$DiskMatch  = "ST16000NM001G*"
)

$ErrorActionPreference = "SilentlyContinue"

$disk = Get-Disk | Where-Object { $_.FriendlyName -like $DiskMatch } | Select-Object -First 1
if (-not $disk) {
    Write-Host "start-minio-with-disk: no disk matching '$DiskMatch' found - skipping wsl --mount."
    Write-Host "start-minio-with-disk: MinIO's own mountpoint guard should refuse to start rather than serve a stale local dir."
} else {
    $target = "\\.\PHYSICALDRIVE$($disk.Number)"
    Write-Host "start-minio-with-disk: attaching $target (disk $($disk.Number), '$($disk.FriendlyName)') to WSL..."
    wsl --mount $target --bare | Out-Null
}

Write-Host "start-minio-with-disk: mounting /mnt/minio and /mnt/wsltmp (if needed) and starting minio.service in $Distro..."
wsl -d $Distro -u root -- bash -lc "mountpoint -q /mnt/minio || mount /mnt/minio; mountpoint -q /mnt/wsltmp || mount /mnt/wsltmp; mountpoint -q /var/tmp || mount /var/tmp; systemctl start minio"
