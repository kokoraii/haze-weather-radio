Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$DevRoot = Join-Path $Root "dist\Haze-RadioMET-Development"
$Executable = Join-Path $DevRoot "haze.exe"
$PidPath = Join-Path $DevRoot "runtime\state\dev-server.pid"
$RootPrefix = [IO.Path]::GetFullPath($DevRoot).TrimEnd('\') + '\'

$DevPid = 0
$HostProcess = $null
if (Test-Path -LiteralPath $PidPath -PathType Leaf) {
    if (-not [int]::TryParse((Get-Content -LiteralPath $PidPath -Raw).Trim(), [ref]$DevPid)) {
        throw "Development PID file is invalid: $PidPath"
    }
    $HostProcess = Get-Process -Id $DevPid -ErrorAction SilentlyContinue
    if ($HostProcess -and (-not $HostProcess.Path -or
        [IO.Path]::GetFullPath($HostProcess.Path) -ne [IO.Path]::GetFullPath($Executable))) {
        throw "Refusing to stop PID $DevPid because it is outside the development runtime."
    }
}

$ManagedPids = @(Get-CimInstance Win32_Process | Where-Object {
    $Path = [string]$_.ExecutablePath
    $Path -and $_.Name -like "haze*.exe" -and
        [IO.Path]::GetFullPath($Path).StartsWith($RootPrefix, [StringComparison]::OrdinalIgnoreCase)
} | Select-Object -ExpandProperty ProcessId)

if ($ManagedPids.Count -eq 0) {
    Remove-Item -LiteralPath $PidPath -Force -ErrorAction SilentlyContinue
    Write-Output "Haze development server is not running."
    exit 0
}

if ($HostProcess) {
    Stop-Process -Id $DevPid
} else {
    foreach ($ProcessId in $ManagedPids) {
        Stop-Process -Id $ProcessId -ErrorAction SilentlyContinue
    }
}
$Deadline = [DateTime]::UtcNow.AddSeconds(10)
do {
    $Remaining = @($ManagedPids | Where-Object { Get-Process -Id $_ -ErrorAction SilentlyContinue })
    if ($Remaining.Count -eq 0) {
        break
    }
    Start-Sleep -Milliseconds 250
} while ([DateTime]::UtcNow -lt $Deadline)

foreach ($ProcessId in $Remaining) {
    $Candidate = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
    $Path = [string]$Candidate.ExecutablePath
    if ($Path -and $Candidate.Name -like "haze*.exe" -and
        [IO.Path]::GetFullPath($Path).StartsWith($RootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    }
}

Remove-Item -LiteralPath $PidPath -Force -ErrorAction SilentlyContinue
Write-Output "Haze development server stopped."
