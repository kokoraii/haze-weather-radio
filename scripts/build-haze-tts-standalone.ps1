# Builds a standalone Windows haze-tts bundle for running TTS on another
# machine, such as a host with native SAPI5 voices. The Haze daemon connects to
# it when services.go.tts.host is that machine (see docs/remote-services.md).
#
# Needs MSYS2 CLANG64 clang for the native Piper and Kokoro runtimes. Voice
# models are not bundled: install SAPI5 voices on the machine, and copy Piper or
# Kokoro voices into managed/voices next to haze-tts.exe when readers use them.
param(
    [string] $OutputDir = "dist/haze-tts-Windows-x86_64"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-NativeSuccess {
    param([string] $Description)
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Resolve-Path (Join-Path $ScriptDir "..")
. (Join-Path $ScriptDir "lib/sherpa-onnx-runtime.ps1")

$OutFull = [System.IO.Path]::GetFullPath((Join-Path $Root $OutputDir))
$DistRoot = [System.IO.Path]::GetFullPath((Join-Path $Root "dist"))
if (-not $OutFull.StartsWith($DistRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to write outside the dist directory: $OutFull"
}
if (-not [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
    throw "The standalone haze-tts bundle is built on Windows, where SAPI5 runs."
}

$MsysRoot = if ([string]::IsNullOrWhiteSpace($env:MSYS2_ROOT)) { "C:\msys64" } else { $env:MSYS2_ROOT }
$Clang64Bin = Join-Path $MsysRoot "clang64\bin"
$Clang = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang.exe"
if (-not (Test-Path -LiteralPath $Clang)) {
    throw "MSYS2 CLANG64 clang was not found at $Clang. Install mingw-w64-clang-x86_64-clang, or set MSYS2_ROOT."
}

New-Item -ItemType Directory -Force -Path $OutFull | Out-Null
foreach ($Dir in @("managed\voices\piper", "runtime\cache\tts")) {
    New-Item -ItemType Directory -Force -Path (Join-Path $OutFull $Dir) | Out-Null
}
New-Item -ItemType Directory -Force -Path (Join-Path $Root "target/go-build-cache") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Root "target/go-tmp") | Out-Null

$SavedEnv = @{}
foreach ($Name in @("Path", "CGO_ENABLED", "CC", "CXX", "GOOS", "GOARCH", "GOCACHE", "GOTMPDIR")) {
    $SavedEnv[$Name] = [Environment]::GetEnvironmentVariable($Name)
}
Push-Location (Join-Path $Root "services/go")
try {
    $env:Path = "$Clang64Bin;$(Join-Path $MsysRoot "usr\bin");$env:Path"
    $env:GOCACHE = Join-Path $Root "target/go-build-cache"
    $env:GOTMPDIR = Join-Path $Root "target/go-tmp"
    $env:CGO_ENABLED = "1"
    $env:CC = $Clang
    $env:CXX = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang++.exe"
    go build -trimpath -o (Join-Path $OutFull "haze-tts.exe") ./cmd/haze-tts
    Assert-NativeSuccess "haze-tts build"
    Copy-SherpaOnnxRuntimeLibraries -DestinationDir $OutFull

    # haze-tts runs every SAPI5 job in one of these shims, so an engine that
    # crashes or hangs takes down only its job: x64 first, then x86, since
    # 32-bit-only voices load only in a 32-bit process.
    $env:GOOS = "windows"
    $env:CGO_ENABLED = "0"
    $env:GOARCH = "amd64"
    go build -trimpath -o (Join-Path $OutFull "haze-sapi5-shim-x64.exe") ./cmd/haze-sapi5-shim
    Assert-NativeSuccess "haze-sapi5-shim-x64 build"
    $env:GOARCH = "386"
    go build -trimpath -o (Join-Path $OutFull "haze-sapi5-shim.exe") ./cmd/haze-sapi5-shim
    Assert-NativeSuccess "haze-sapi5-shim build"
} finally {
    Pop-Location
    foreach ($Name in $SavedEnv.Keys) {
        [Environment]::SetEnvironmentVariable($Name, $SavedEnv[$Name])
    }
}

foreach ($DllName in @("libc++.dll", "libunwind.dll")) {
    $DllPath = Join-Path $Clang64Bin $DllName
    if (Test-Path -LiteralPath $DllPath -PathType Leaf) {
        Copy-Item -LiteralPath $DllPath -Destination $OutFull -Force
    }
}

# Fail here, not on the remote machine, when a bundled library is missing: run
# with only System32 on PATH so nothing outside the bundle can stand in.
$SavedPath = $env:Path
try {
    $env:Path = "$env:SystemRoot\System32;$env:SystemRoot"
    $Voices = & (Join-Path $OutFull "haze-tts.exe") --list-voices --provider sapi5
    Assert-NativeSuccess "haze-tts smoke check (missing bundled DLLs exit with 0xC0000135)"
    # haze-tts drops a broken shim silently from the voice list, so check each on its own.
    & (Join-Path $OutFull "haze-sapi5-shim-x64.exe") list | Out-Null
    Assert-NativeSuccess "haze-sapi5-shim-x64 smoke check"
    $ShimVoices = & (Join-Path $OutFull "haze-sapi5-shim.exe") list
    Assert-NativeSuccess "haze-sapi5-shim smoke check"
} finally {
    $env:Path = $SavedPath
}
$VoiceLabels = @($Voices | ConvertFrom-Json) | ForEach-Object { "$($_.name) ($($_.arch))" }
Write-Host "SAPI5 voices on this machine: $($VoiceLabels -join ', ')"
Write-Host "32-bit SAPI5 voices through the shim: $(@($ShimVoices | ConvertFrom-Json).Count)"

Set-Content -LiteralPath (Join-Path $OutFull ".env.example") -Encoding ascii -Value @(
    "# Shared secret from the Haze daemon's .env (HAZE_REMOTE_TOKEN). Both sides must match."
    "HAZE_REMOTE_TOKEN="
)
Set-Content -LiteralPath (Join-Path $OutFull "run-haze-tts.cmd") -Encoding ascii -Value @(
    "@echo off"
    "rem Waits for the Haze daemon on port 41810. Set HAZE_REMOTE_TOKEN in .env first,"
    "rem and allow inbound TCP 41810 from the Haze host in Windows Firewall."
    "rem Readers, the dictionary, and the default language come from Haze on connect."
    "cd /d ""%~dp0"""
    "haze-tts.exe --service --listen 0.0.0.0:41810 --cache-dir runtime\cache\tts --cache-max-bytes 536870912 --piper-voices-dir managed\voices\piper --kokoro-model-dir managed\voices\kokoro-multi-lang-v1_0 --workers 4 %*"
)
Write-Host "Built standalone haze-tts in $OutFull"
