param(
    [Parameter(Mandatory = $true)][string] $OutputDir,
    [string] $LicenseDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$WhisperCommit = "23ee03506a91ac3d3f0071b40e66a430eebdfa1d"
$WhisperVersion = "v1.8.6"
$WhisperRepository = "https://github.com/ggml-org/whisper.cpp.git"

function Assert-WhisperCommand {
    param([string] $Description)
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir ".."))
$CacheRoot = Join-Path $Root (Join-Path "target/whisper-cpp" $WhisperCommit)
$SourceDir = if ([string]::IsNullOrWhiteSpace($env:HAZE_WHISPER_CPP_SOURCE)) {
    Join-Path $CacheRoot "source"
} else {
    [System.IO.Path]::GetFullPath($env:HAZE_WHISPER_CPP_SOURCE)
}
$BuildDir = Join-Path $CacheRoot "build-Windows-x86_64"
$OutputFull = [System.IO.Path]::GetFullPath($OutputDir)

$Git = Get-Command git -ErrorAction Stop
$MsysRoot = if ([string]::IsNullOrWhiteSpace($env:MSYS2_ROOT)) { "C:\msys64" } else { $env:MSYS2_ROOT }
$ClangBin = Join-Path $MsysRoot "clang64\bin"
$CMakePath = Join-Path $ClangBin "cmake.exe"
$NinjaPath = Join-Path $ClangBin "ninja.exe"
$CCompiler = Join-Path $ClangBin "x86_64-w64-mingw32-clang.exe"
$CXXCompiler = Join-Path $ClangBin "x86_64-w64-mingw32-clang++.exe"
foreach ($RequiredPath in @($CMakePath, $NinjaPath, $CCompiler, $CXXCompiler)) {
    if (-not (Test-Path -LiteralPath $RequiredPath -PathType Leaf)) {
        throw "required local Whisper build tool is missing: $RequiredPath"
    }
}

if (-not (Test-Path -LiteralPath (Join-Path $SourceDir ".git") -PathType Container)) {
    if (-not [string]::IsNullOrWhiteSpace($env:HAZE_WHISPER_CPP_SOURCE)) {
        throw "HAZE_WHISPER_CPP_SOURCE is not a Git checkout: $SourceDir"
    }
    New-Item -ItemType Directory -Force -Path $SourceDir | Out-Null
    & $Git.Source -C $SourceDir init --quiet
    Assert-WhisperCommand "whisper.cpp repository initialization"
    & $Git.Source -C $SourceDir remote add origin $WhisperRepository
    Assert-WhisperCommand "whisper.cpp remote configuration"
    & $Git.Source -C $SourceDir fetch --quiet --depth 1 origin $WhisperCommit
    Assert-WhisperCommand "whisper.cpp pinned source fetch"
    & $Git.Source -C $SourceDir checkout --quiet --detach FETCH_HEAD
    Assert-WhisperCommand "whisper.cpp pinned source checkout"
}

$SourceCommit = (& $Git.Source -C $SourceDir rev-parse HEAD).Trim()
Assert-WhisperCommand "whisper.cpp source verification"
if ($SourceCommit -ne $WhisperCommit) {
    throw "Refusing unpinned whisper.cpp source at ${SourceDir}: $SourceCommit"
}

$NativeBuild = if ([string]::IsNullOrWhiteSpace($env:HAZE_WHISPER_NATIVE)) { "OFF" } else { $env:HAZE_WHISPER_NATIVE.ToUpperInvariant() }
if ($NativeBuild -notin @("ON", "OFF")) {
    throw "HAZE_WHISPER_NATIVE must be ON or OFF"
}

New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
& $CMakePath -S $SourceDir -B $BuildDir -G Ninja `
    "-DCMAKE_BUILD_TYPE=Release" `
    "-DCMAKE_MAKE_PROGRAM=$NinjaPath" `
    "-DCMAKE_C_COMPILER=$CCompiler" `
    "-DCMAKE_CXX_COMPILER=$CXXCompiler" `
    "-DBUILD_SHARED_LIBS=OFF" `
    "-DWHISPER_BUILD_TESTS=OFF" `
    "-DWHISPER_BUILD_EXAMPLES=ON" `
    "-DWHISPER_BUILD_SERVER=ON" `
    "-DWHISPER_CURL=OFF" `
    "-DWHISPER_SDL2=OFF" `
    "-DGGML_BACKEND_DL=OFF" `
    "-DGGML_OPENMP=OFF" `
    "-DGGML_NATIVE=$NativeBuild" `
    "-DGGML_CCACHE=OFF"
Assert-WhisperCommand "whisper.cpp CMake configuration"
& $CMakePath --build $BuildDir --config Release --target whisper-server --parallel
Assert-WhisperCommand "whisper.cpp runtime build"

$Candidates = @(
    (Join-Path $BuildDir "bin\whisper-server.exe"),
    (Join-Path $BuildDir "bin\Release\whisper-server.exe")
)
$RuntimeBinary = $Candidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if ([string]::IsNullOrWhiteSpace($RuntimeBinary)) {
    throw "whisper-server.exe was not produced under $BuildDir\bin"
}

New-Item -ItemType Directory -Force -Path $OutputFull | Out-Null
$RuntimeOut = Join-Path $OutputFull "whisper-server.exe"
Copy-Item -LiteralPath $RuntimeBinary -Destination $RuntimeOut -Force
foreach ($RuntimeDll in @("libc++.dll", "libunwind.dll")) {
    $RuntimeDllPath = Join-Path $ClangBin $RuntimeDll
    if (-not (Test-Path -LiteralPath $RuntimeDllPath -PathType Leaf)) {
        throw "required local Whisper runtime DLL is missing: $RuntimeDllPath"
    }
    Copy-Item -LiteralPath $RuntimeDllPath -Destination $OutputFull -Force
}
& $RuntimeOut --help *> $null
Assert-WhisperCommand "whisper.cpp runtime smoke check"

if (-not [string]::IsNullOrWhiteSpace($LicenseDir)) {
    $LicenseFull = [System.IO.Path]::GetFullPath($LicenseDir)
    New-Item -ItemType Directory -Force -Path $LicenseFull | Out-Null
    Copy-Item -LiteralPath (Join-Path $SourceDir "LICENSE") -Destination (Join-Path $LicenseFull "whisper.cpp-$WhisperVersion-LICENSE.txt") -Force
}

Write-Host "Built local Whisper runtime $WhisperVersion in $OutputFull"
