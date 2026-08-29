# The Windows runner M1b line 27 asks for.
#
# Usage: powershell -NoProfile -File scripts\zou-windows.ps1 [-Root C:\zoubench] [-Build]
#
# Everything else in scripts/ is POSIX sh, which is the right language
# for the boxes those scripts run on and the wrong one here. Git for
# Windows does ship a bash, but the readings this needs are Windows
# readings: there is no /proc to walk, no load average to quote, and the
# disk under a path is a volume rather than a device node. So this is a
# separate script rather than a compatibility layer over the others, and
# it says up front which of the M1b scenarios it can carry.
#
# It can carry the store level ones. The backends this box is here to
# test are a directory on NTFS and a sqlite file on NTFS, both of them
# pure Rust with no postgres under them, and `zou probe` measures each
# through the same CasStore client the engine uses, so the signing, the
# retries and the file handling are inside the number.
#
# It cannot carry the pgbench legs. Those need the patched postgres,
# which is built with meson against a vendored source tree and has not
# been built for Windows, and vanilla Postgres 18 native needs an
# installer and a data directory that nothing here should be creating on
# somebody's desktop. Both are still owed and the M1b line stays open
# until they land.
#
# Nothing outside -Root is written to or read from, because this box is
# in daily use by its owner.

param(
    [string]$Root = "C:\zoubench",
    [switch]$Build
)

$ErrorActionPreference = "Stop"

$repo = Join-Path $Root "zou"
$exe = Join-Path $repo "target\release\zou.exe"

if ($Build) {
    Push-Location $repo
    try { cargo build --release -p zou } finally { Pop-Location }
}
if (-not (Test-Path $exe)) {
    Write-Error "no zou.exe at $exe, run with -Build first"
}

# The machine, stamped with the reading rather than remembered, for the
# same reason the sh scripts stamp theirs: a box is not the same box six
# months later.
$cpu = Get-CimInstance Win32_Processor
$os = Get-CimInstance Win32_OperatingSystem
$vol = Get-Volume -DriveLetter $Root.Substring(0, 1)
$disk = Get-PhysicalDisk | Select-Object -First 1
# Windows has no load average. The nearest honest reading is how busy
# the cpu is right now, which is not the same quantity and is labeled as
# what it is rather than dressed up as a load.
$busy = (Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor |
    Where-Object { $_.Name -eq "_Total" }).PercentProcessorTime

Write-Output ("host: " + $env:COMPUTERNAME)
Write-Output ("cpu: " + $cpu.Name + ", " + $cpu.NumberOfCores + " cores, " +
    $cpu.NumberOfLogicalProcessors + " threads")
Write-Output ("memory: " + [math]::Round($os.TotalVisibleMemorySize / 1MB, 1) + " GB")
Write-Output ("os: " + $os.Caption + " " + $os.Version)
Write-Output ("store disk: " + $disk.FriendlyName + " " + $disk.MediaType + ", " +
    $vol.FileSystemType + ", " + [math]::Round($vol.Size / 1GB) + " GB with " +
    [math]::Round($vol.SizeRemaining / 1GB) + " GB free")
Write-Output ("cpu busy at probe: " + $busy + "%")
Write-Output ""

# A directory store and a sqlite store, the two backends that run on
# this box without a postgres. The probe writes and deletes what it
# wrote, so both targets are left as they were found.
$dir = Join-Path $Root "store"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
& $exe probe $dir

$db = (Join-Path $Root "store.db") -replace "\\", "/"
& $exe probe ("sqlite://" + $db)
Remove-Item -Force -ErrorAction SilentlyContinue $db
