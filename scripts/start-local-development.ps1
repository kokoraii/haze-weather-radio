Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$DevRoot = Join-Path $Root "dist\Haze-RadioMET-Development"
$Executable = Join-Path $DevRoot "haze.exe"
$Config = Join-Path $DevRoot "config.yaml"
$StateDir = Join-Path $DevRoot "runtime\state"
$LogDir = Join-Path $DevRoot "logs"
$PidPath = Join-Path $StateDir "dev-server.pid"
$HealthURL = "http://127.0.0.1:18080/api/public/v1/health"
$MsysRoot = if ([string]::IsNullOrWhiteSpace($env:MSYS2_ROOT)) { "C:\msys64" } else { $env:MSYS2_ROOT }
$GStreamerBin = Join-Path $MsysRoot "clang64\bin"
$GStreamerPlugins = Join-Path $MsysRoot "clang64\lib\gstreamer-1.0"
$GStreamerScanner = Join-Path $MsysRoot "clang64\libexec\gstreamer-1.0\gst-plugin-scanner.exe"

if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
    throw "Local development host is not built: $Executable"
}
if (-not (Test-Path -LiteralPath $Config -PathType Leaf)) {
    throw "Local development config is missing: $Config"
}
foreach ($RequiredPath in @(
    (Join-Path $GStreamerBin "libgstreamer-1.0-0.dll"),
    $GStreamerPlugins,
    $GStreamerScanner
)) {
    if (-not (Test-Path -LiteralPath $RequiredPath)) {
        throw "Local WebRTC requires the MSYS2 CLANG64 GStreamer runtime: $RequiredPath"
    }
}

if (Test-Path -LiteralPath $PidPath -PathType Leaf) {
    $ExistingPid = 0
    if ([int]::TryParse((Get-Content -LiteralPath $PidPath -Raw).Trim(), [ref]$ExistingPid)) {
        $Existing = Get-Process -Id $ExistingPid -ErrorAction SilentlyContinue
        if ($Existing -and $Existing.Path -and
            [IO.Path]::GetFullPath($Existing.Path) -eq [IO.Path]::GetFullPath($Executable)) {
            Write-Output "Haze development server is already running (PID $ExistingPid)."
            Write-Output "Panel: http://127.0.0.1:18080"
            exit 0
        }
    }
}

# Recover from an interrupted supervisor that left managed child services behind.
& (Join-Path $ScriptDir "stop-local-development.ps1")

$Listener = Get-NetTCPConnection -LocalPort 18080 -State Listen -ErrorAction SilentlyContinue
if ($Listener) {
    throw "Port 18080 is already in use by PID $($Listener.OwningProcess)."
}

New-Item -ItemType Directory -Force -Path $StateDir, $LogDir | Out-Null
$env:Path = "$GStreamerBin;$env:Path"
$env:GST_PLUGIN_PATH = if ([string]::IsNullOrWhiteSpace($env:GST_PLUGIN_PATH)) {
    $GStreamerPlugins
} else {
    "$GStreamerPlugins;$env:GST_PLUGIN_PATH"
}
$env:GST_PLUGIN_SCANNER = $GStreamerScanner
$env:GST_REGISTRY = Join-Path $StateDir "gstreamer-registry.bin"
$Process = Start-Process `
    -FilePath $Executable `
    -ArgumentList @("--config", "config.yaml", "--workdir", $DevRoot) `
    -WorkingDirectory $DevRoot `
    -RedirectStandardOutput (Join-Path $LogDir "launcher.stdout.log") `
    -RedirectStandardError (Join-Path $LogDir "launcher.stderr.log") `
    -WindowStyle Hidden `
    -PassThru
$Process.Id | Set-Content -LiteralPath $PidPath -Encoding ascii

$Deadline = [DateTime]::UtcNow.AddSeconds(30)
do {
    if ($Process.HasExited) {
        throw "Haze development server exited during startup. Check $LogDir."
    }
    try {
        $Health = Invoke-RestMethod -Uri $HealthURL -TimeoutSec 2
        if ($Health.ok -eq $true) {
            Write-Output "Haze development server is ready (PID $($Process.Id))."
            Write-Output "Panel: http://127.0.0.1:18080"
            exit 0
        }
    } catch {
        Start-Sleep -Milliseconds 500
    }
} while ([DateTime]::UtcNow -lt $Deadline)

throw "Haze development server did not become healthy within 30 seconds. Check $LogDir."
