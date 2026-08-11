param(
    [string] $OutputDir = ""
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

function Get-PortableOSName {
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
        return "Windows"
    }
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)) {
        return "Linux"
    }
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::OSX)) {
        return "macOS"
    }
    return "UnknownOS"
}

function Get-PortableArchName {
    switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
        "X64" { return "x86_64" }
        "Arm64" { return "aarch64" }
        "X86" { return "x86" }
        "Arm" { return "armv7" }
        default { return [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant() }
    }
}

function Get-DefaultPortableOutputDir {
    return "dist/Haze_UAP-$(Get-PortableOSName)-$(Get-PortableArchName)-Portable"
}

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Get-DefaultPortableOutputDir
}

$OutFull = [System.IO.Path]::GetFullPath((Join-Path $Root $OutputDir))
$BinFull = Join-Path $OutFull "bin"
$DistRoot = [System.IO.Path]::GetFullPath((Join-Path $Root "dist"))
$DistRootWithSeparator = $DistRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar

if ($OutFull -ne $DistRoot -and -not $OutFull.StartsWith($DistRootWithSeparator, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to write outside the dist directory: $OutFull"
}

function Copy-BundleDirectory {
    param(
        [Parameter(Mandatory = $true)][string] $Name
    )
    $Source = Join-Path $Root (Join-Path "bundle" $Name)
    $Target = Join-Path $OutFull $Name
    if (-not (Test-Path -LiteralPath $Source -PathType Container)) {
        return
    }
    $PreserveDir = $null
    if ($Name -eq "managed" -and (Test-Path -LiteralPath $Target -PathType Container)) {
        $PreserveFiles = @(Get-ChildItem -LiteralPath $Target -Filter "*.onnx" -Recurse -File -ErrorAction SilentlyContinue)
        $LocationsTarget = Join-Path $Target "locations"
        if (Test-Path -LiteralPath $LocationsTarget -PathType Container) {
            $PreserveFiles += @(Get-ChildItem -LiteralPath $LocationsTarget -Recurse -File -ErrorAction SilentlyContinue)
        }
        $VoicesTarget = Join-Path $Target "voices"
        if (Test-Path -LiteralPath $VoicesTarget -PathType Container) {
            $PreserveFiles += @(Get-ChildItem -LiteralPath $VoicesTarget -Directory -Filter "kokoro*" -ErrorAction SilentlyContinue | ForEach-Object {
                Get-ChildItem -LiteralPath $_.FullName -Recurse -File -ErrorAction SilentlyContinue
            })
        }
        if ($PreserveFiles.Count -gt 0) {
            $PreserveDir = Join-Path ([System.IO.Path]::GetTempPath()) ("haze-preserve-managed-" + [System.Guid]::NewGuid().ToString("N"))
            $TargetFull = [System.IO.Path]::GetFullPath($Target).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
            foreach ($File in $PreserveFiles) {
                $Relative = $File.FullName.Substring($TargetFull.Length).TrimStart([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
                $Backup = Join-Path $PreserveDir $Relative
                New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Backup) | Out-Null
                Copy-Item -LiteralPath $File.FullName -Destination $Backup -Force
            }
        }
    }
    if (Test-Path -LiteralPath $Target) {
        Remove-Item -LiteralPath $Target -Recurse -Force
    }
    Copy-Item -LiteralPath $Source -Destination $Target -Recurse -Force
    if ($PreserveDir -and (Test-Path -LiteralPath $PreserveDir -PathType Container)) {
        $PreserveFull = [System.IO.Path]::GetFullPath($PreserveDir).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
        Get-ChildItem -LiteralPath $PreserveDir -Recurse -File | ForEach-Object {
            $Relative = $_.FullName.Substring($PreserveFull.Length).TrimStart([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
            $Restored = Join-Path $Target $Relative
            if (-not (Test-Path -LiteralPath $Restored)) {
                New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Restored) | Out-Null
                Copy-Item -LiteralPath $_.FullName -Destination $Restored -Force
            }
        }
        Remove-Item -LiteralPath $PreserveDir -Recurse -Force
    }
}

New-Item -ItemType Directory -Force -Path $OutFull | Out-Null
New-Item -ItemType Directory -Force -Path $BinFull | Out-Null
foreach ($File in @("haze-web.exe", "haze-data-ingest.exe", "haze-tts.exe", "haze-sapi5-shim.exe", "haze-product-render.exe", "haze-playlist.exe", "haze-webhook.exe", "haze-asr.exe", "haze-ivr.exe", "whisper-server.exe", "libopus-0.dll", "libopusfile-0.dll", "libogg-0.dll", "libwinpthread-1.dll")) {
    $LegacyTarget = Join-Path $OutFull $File
    if (Test-Path -LiteralPath $LegacyTarget) {
        Remove-Item -LiteralPath $LegacyTarget -Force
    }
}
New-Item -ItemType Directory -Force -Path (Join-Path $Root "target/go-build-cache") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Root "target/go-tmp") | Out-Null

$RunningOnWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
$MsysRoot = if ([string]::IsNullOrWhiteSpace($env:MSYS2_ROOT)) { "C:\msys64" } else { $env:MSYS2_ROOT }
$MsysUsrBin = Join-Path $MsysRoot "usr\bin"
$Clang64Root = Join-Path $MsysRoot "clang64"
$Clang64Bin = Join-Path $Clang64Root "bin"
$Clang64Lib = Join-Path $Clang64Root "lib"
$OpusBin = $Clang64Bin
$BuildWebArgs = @()
$AllowNoOpus = $env:HAZE_ALLOW_NO_OPUS -eq "1" -or $env:HAZE_ALLOW_NO_OPUS -ieq "true"

function Test-OpusPkgConfig {
    if (-not (Get-Command pkg-config -ErrorAction SilentlyContinue)) {
        return $false
    }
    pkg-config --exists opus
    return $LASTEXITCODE -eq 0
}

function Copy-SherpaOnnxRuntimeLibraries {
    param(
        [Parameter(Mandatory = $true)][string] $DestinationDir
    )

    $GoOS = (go env GOOS 2>$null).Trim()
    $GoArch = (go env GOARCH 2>$null).Trim()
    if ([string]::IsNullOrWhiteSpace($GoOS) -or [string]::IsNullOrWhiteSpace($GoArch)) {
        return
    }

    $Module = switch ($GoOS) {
        "windows" { "github.com/k2-fsa/sherpa-onnx-go-windows" }
        "linux" { "github.com/k2-fsa/sherpa-onnx-go-linux" }
        "darwin" { "github.com/k2-fsa/sherpa-onnx-go-macos" }
        default { "" }
    }
    if ([string]::IsNullOrWhiteSpace($Module)) {
        return
    }

    $Triple = switch ("$GoOS/$GoArch") {
        "windows/amd64" { "x86_64-pc-windows-gnu" }
        "windows/386" { "i686-pc-windows-gnu" }
        "linux/amd64" { "x86_64-unknown-linux-gnu" }
        "linux/arm64" { "aarch64-unknown-linux-gnu" }
        "linux/arm" { "arm-unknown-linux-gnueabihf" }
        "darwin/amd64" { "x86_64-apple-darwin" }
        "darwin/arm64" { "aarch64-apple-darwin" }
        default { "" }
    }
    if ([string]::IsNullOrWhiteSpace($Triple)) {
        return
    }

    $ModuleDir = (go list -m -f "{{.Dir}}" $Module 2>$null).Trim()
    if ([string]::IsNullOrWhiteSpace($ModuleDir)) {
        return
    }
    $LibDir = Join-Path $ModuleDir (Join-Path "lib" $Triple)
    if (-not (Test-Path -LiteralPath $LibDir -PathType Container)) {
        return
    }

    Get-ChildItem -LiteralPath $LibDir -File | Where-Object {
        $_.Extension -in @(".dll", ".so", ".dylib") -or $_.Name -like "*.so.*"
    } | ForEach-Object {
        Copy-Item -LiteralPath $_.FullName -Destination $DestinationDir -Force
    }
}

if ($RunningOnWindows -and (Test-Path (Join-Path $Clang64Bin "x86_64-w64-mingw32-clang.exe")) -and (Test-Path (Join-Path $Clang64Bin "pkg-config.exe"))) {
    $env:Path = "$Clang64Bin;$MsysUsrBin;$env:Path"
    $env:CGO_ENABLED = "1"
    $env:CC = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang.exe"
    $env:CXX = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang++.exe"
    $env:PKG_CONFIG = Join-Path $Clang64Bin "pkg-config.exe"
    $env:PKG_CONFIG_PATH = "$(Join-Path $Clang64Lib "pkgconfig");$(Join-Path $Clang64Root "share\pkgconfig")"
    $BuildWebArgs = @("-tags", "opus_cgo")
} elseif (-not $RunningOnWindows -and (Test-OpusPkgConfig)) {
    $env:CGO_ENABLED = "1"
    $BuildWebArgs = @("-tags", "opus_cgo")
} elseif (-not $AllowNoOpus) {
    throw "Native Opus build inputs are required for receiver/WebRTC audio. Install MSYS2 CLANG64 clang, pkgconf, opus, and opusfile, or set HAZE_ALLOW_NO_OPUS=1 for a degraded dev-only build."
} else {
    Write-Warning "Building haze-web without native Opus support; install MSYS2 CLANG64 clang, pkgconf, opus, and opusfile for receiver Opus."
}

& (Join-Path $Root "scripts/build-whisper-runtime.ps1") `
    -OutputDir $BinFull `
    -LicenseDir (Join-Path $OutFull "licenses")

Push-Location (Join-Path $Root "services/go")
try {
    $env:GOCACHE = Join-Path $Root "target/go-build-cache"
    $env:GOTMPDIR = Join-Path $Root "target/go-tmp"
    $GitCommit = "unknown"
    try {
        $GitCommitRaw = git -C $Root rev-parse --short=12 HEAD
        if (-not [string]::IsNullOrWhiteSpace($GitCommitRaw)) {
            $GitCommit = $GitCommitRaw.Trim()
        }
    } catch {
        $GitCommit = "unknown"
    }
    $WebLdflags = "-X github.com/meowraii/haze-weather-radio/services/go/internal/webgateway.BuildGitCommit=$GitCommit"
    go build @BuildWebArgs -ldflags $WebLdflags -o (Join-Path $BinFull "haze-web.exe") ./cmd/haze-web
    Assert-NativeSuccess "haze-web build"
    go build -o (Join-Path $BinFull "haze-data-ingest.exe") ./cmd/haze-data-ingest
    Assert-NativeSuccess "haze-data-ingest build"
    go build -o (Join-Path $BinFull "haze-tts.exe") ./cmd/haze-tts
    Assert-NativeSuccess "haze-tts build"
    if ($RunningOnWindows) {
        $SavedGoOS = $env:GOOS
        $SavedGoArch = $env:GOARCH
        $SavedCGOEnabled = $env:CGO_ENABLED
        try {
            $env:GOOS = "windows"
            $env:GOARCH = "386"
            $env:CGO_ENABLED = "0"
            go build -o (Join-Path $BinFull "haze-sapi5-shim.exe") ./cmd/haze-sapi5-shim
            Assert-NativeSuccess "haze-sapi5-shim build"
        } finally {
            $env:GOOS = $SavedGoOS
            $env:GOARCH = $SavedGoArch
            $env:CGO_ENABLED = $SavedCGOEnabled
        }
    }
    go build -o (Join-Path $BinFull "haze-product-render.exe") ./cmd/haze-product-render
    Assert-NativeSuccess "haze-product-render build"
    go build -o (Join-Path $BinFull "haze-playlist.exe") ./cmd/haze-playlist
    Assert-NativeSuccess "haze-playlist build"
    go build -o (Join-Path $BinFull "haze-webhook.exe") ./cmd/haze-webhook
    Assert-NativeSuccess "haze-webhook build"
    go build -o (Join-Path $BinFull "haze-asr.exe") ./cmd/haze-asr
    Assert-NativeSuccess "haze-asr build"
    go build -o (Join-Path $BinFull "haze-ivr.exe") ./cmd/haze-ivr
    Assert-NativeSuccess "haze-ivr build"
    Copy-SherpaOnnxRuntimeLibraries -DestinationDir $BinFull
    if ($RunningOnWindows) {
        foreach ($DllName in @("libc++.dll", "libunwind.dll")) {
            $DllPath = Join-Path $Clang64Bin $DllName
            if (Test-Path -LiteralPath $DllPath -PathType Leaf) {
                Copy-Item -LiteralPath $DllPath -Destination $BinFull -Force
            }
        }
    }
    if ($BuildWebArgs.Count -gt 0) {
        foreach ($DllName in @("libopus-0.dll", "libopusfile-0.dll", "libogg-0.dll", "libwinpthread-1.dll")) {
            $DllPath = Join-Path $OpusBin $DllName
            if (Test-Path $DllPath) {
                Copy-Item -Force -Path $DllPath -Destination $BinFull
            }
        }
        & (Join-Path $BinFull "haze-web.exe") --check-codecs --require-opus | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "haze-web native Opus smoke check failed"
        }
    }
    Write-Host "Built Go services in $BinFull"
} finally {
    Pop-Location
}

foreach ($BundledDir in @("webroot", "managed", "audio")) {
    Copy-BundleDirectory -Name $BundledDir
}

$ManagedScripts = Join-Path (Join-Path $OutFull "managed") "scripts"
New-Item -ItemType Directory -Force -Path $ManagedScripts | Out-Null
foreach ($Script in @("scripts/tts/chatterbox_infer.py", "scripts/tts/f5_infer.py", "scripts/install-whisper-model.sh", "scripts/install-whisper-model.ps1")) {
    $Source = Join-Path $Root $Script
    if (Test-Path -LiteralPath $Source) {
        Copy-Item -LiteralPath $Source -Destination $ManagedScripts -Force
    }
}
