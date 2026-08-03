param(
    [ValidateSet("debug", "release")]
    [string] $Profile = "release",
    [string] $OutputDir = "",
    [ValidateSet("auto", "builtin", "ffmpeg", "rsmpeg")]
    [string] $MediaBackend = "auto",
    [switch] $IncludeEnv,
    [switch] $SkipGoServices,
    [switch] $SkipCargoBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Resolve-Path (Join-Path $ScriptDir "..")

if ($MediaBackend -eq "rsmpeg") {
    Write-Warning "The rsmpeg backend name is deprecated. Using the version-independent FFmpeg runtime loader."
}

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

$DistRoot = [System.IO.Path]::GetFullPath((Join-Path $Root "dist"))
$OutFull = [System.IO.Path]::GetFullPath((Join-Path $Root $OutputDir))
$BinFull = Join-Path $OutFull "bin"
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
        $VoicesTarget = Join-Path $Target "voices"
        if (Test-Path -LiteralPath $VoicesTarget -PathType Container) {
            $PreserveFiles += @(Get-ChildItem -LiteralPath $VoicesTarget -Directory -Filter "kokoro*" -ErrorAction SilentlyContinue | ForEach-Object {
                Get-ChildItem -LiteralPath $_.FullName -Recurse -File -ErrorAction SilentlyContinue
            })
        }
        if ($PreserveFiles.Count -gt 0) {
            $PreserveDir = Join-Path ([System.IO.Path]::GetTempPath()) ("haze-preserve-onnx-" + [System.Guid]::NewGuid().ToString("N"))
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

function Test-RunningOnWindows {
    return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
}

function Get-MsysRoot {
    if ([string]::IsNullOrWhiteSpace($env:MSYS2_ROOT)) {
        return "C:\msys64"
    }
    return $env:MSYS2_ROOT
}

function Initialize-Clang64BuildEnvironment {
    $MsysRoot = Get-MsysRoot
    $MsysUsrBin = Join-Path $MsysRoot "usr\bin"
    $Clang64Root = Join-Path $MsysRoot "clang64"
    $Clang64Bin = Join-Path $Clang64Root "bin"
    $Clang64Lib = Join-Path $Clang64Root "lib"
    foreach ($RequiredTool in @("x86_64-w64-mingw32-clang.exe", "x86_64-w64-mingw32-clang++.exe", "llvm-ar.exe", "pkg-config.exe", "llvm-objdump.exe")) {
        $RequiredPath = Join-Path $Clang64Bin $RequiredTool
        if (-not (Test-Path -LiteralPath $RequiredPath -PathType Leaf)) {
            throw "required MSYS2 CLANG64 tool not found: $RequiredPath"
        }
    }
    foreach ($RequiredRuntime in @("libc++.dll", "libunwind.dll")) {
        $RequiredPath = Join-Path $Clang64Bin $RequiredRuntime
        if (-not (Test-Path -LiteralPath $RequiredPath -PathType Leaf)) {
            $RuntimePackage = if ($RequiredRuntime -eq "libc++.dll") { "mingw-w64-clang-x86_64-libc++" } else { "mingw-w64-clang-x86_64-libunwind" }
            throw "required MSYS2 CLANG64 runtime DLL not found: $RequiredPath. Install $RuntimePackage."
        }
    }
    $Libclang = Join-Path $Clang64Bin "libclang.dll"
    if (-not (Test-Path -LiteralPath $Libclang -PathType Leaf)) {
        throw "required MSDF build dependency not found: $Libclang. Install mingw-w64-clang-x86_64-clang."
    }
    $LibcxxImport = Join-Path $Clang64Lib "libc++.dll.a"
    if (-not (Test-Path -LiteralPath $LibcxxImport -PathType Leaf)) {
        throw "required MSDF C++ import library not found: $LibcxxImport. Install mingw-w64-clang-x86_64-libc++."
    }

    $Clang = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang.exe"
    $Clangxx = Join-Path $Clang64Bin "x86_64-w64-mingw32-clang++.exe"
    $env:Path = "$Clang64Bin;$MsysUsrBin;$env:Path"
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNULLVM_LINKER = $Clang
    $env:CC_x86_64_pc_windows_gnullvm = $Clang
    $env:CXX_x86_64_pc_windows_gnullvm = $Clangxx
    $env:AR_x86_64_pc_windows_gnullvm = Join-Path $Clang64Bin "llvm-ar.exe"
    $env:LIBCLANG_PATH = $Clang64Bin
    # msdfgen-sys consumes this exact variable while generating its bindings.
    $env:CXX_STDLIB = "c++"
    $env:CXXSTDLIB_x86_64_pc_windows_gnullvm = "c++"
    $env:BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_gnullvm = "--target=x86_64-w64-windows-gnu"
    $env:PKG_CONFIG = Join-Path $Clang64Bin "pkg-config.exe"
    $env:PKG_CONFIG_PATH = "$(Join-Path $Clang64Lib "pkgconfig");$(Join-Path $Clang64Root "share\pkgconfig")"
    $env:PKG_CONFIG_ALLOW_CROSS = "1"
}

function Assert-Clang64GStreamerBuildEnvironment {
    if (-not (Test-RunningOnWindows)) {
        return
    }

    $MsysRoot = Get-MsysRoot
    $Clang64Root = Join-Path $MsysRoot "clang64"
    $Clang64Bin = Join-Path $Clang64Root "bin"
    $Clang64Lib = Join-Path $Clang64Root "lib"
    $PkgConfig = Join-Path $Clang64Bin "pkg-config.exe"
    $GstInspect = Join-Path $Clang64Bin "gst-inspect-1.0.exe"
    foreach ($Package in @("gstreamer-1.0", "gstreamer-app-1.0", "gstreamer-audio-1.0", "gstreamer-video-1.0")) {
        & $PkgConfig --exists $Package
        if ($LASTEXITCODE -ne 0) {
            throw "required MSYS2 CLANG64 GStreamer pkg-config package not found: $Package. Install mingw-w64-clang-x86_64-gstreamer and mingw-w64-clang-x86_64-gst-plugins-base."
        }
    }
    if (-not (Test-Path -LiteralPath $GstInspect -PathType Leaf)) {
        throw "required MSYS2 CLANG64 GStreamer inspector not found: $GstInspect"
    }
    $MissingElements = New-Object System.Collections.Generic.List[string]
    foreach ($Element in @("x264enc", "avenc_aac", "flvmux")) {
        & $GstInspect $Element *> $null
        if ($LASTEXITCODE -ne 0) {
            [void] $MissingElements.Add($Element)
        }
    }
    $HasRtmpSink = $false
    foreach ($Element in @("rtmp2sink", "rtmpsink")) {
        & $GstInspect $Element *> $null
        if ($LASTEXITCODE -eq 0) {
            $HasRtmpSink = $true
            break
        }
    }
    if (-not $HasRtmpSink) {
        [void] $MissingElements.Add("rtmp2sink-or-rtmpsink")
    }
    if ($MissingElements.Count -gt 0) {
        $Joined = [string]::Join(", ", $MissingElements)
        throw "missing GStreamer element(s) required by the bundled CGEN profile: $Joined. Install mingw-w64-clang-x86_64-gst-plugins-ugly, mingw-w64-clang-x86_64-gst-libav, mingw-w64-clang-x86_64-gst-plugins-bad, and mingw-w64-clang-x86_64-gst-plugins-good."
    }

    foreach ($RequiredPath in @(
        (Join-Path $Clang64Bin "libgstreamer-1.0-0.dll"),
        (Join-Path $Clang64Bin "libgstapp-1.0-0.dll"),
        (Join-Path $Clang64Bin "libgstaudio-1.0-0.dll"),
        (Join-Path $Clang64Bin "libgstvideo-1.0-0.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstcoreelements.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstplayback.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstmpegtsdemux.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstmpegtsmux.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstlibav.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstopus.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstopusparse.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstpango.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstrtp.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstrtpmanager.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstudp.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstdtls.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstsrtp.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstnice.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstwebrtc.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstshout2.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstaudiotestsrc.dll"),
        (Join-Path $Clang64Lib "gstreamer-1.0\libgstvideotestsrc.dll"),
        (Join-Path $Clang64Root "libexec\gstreamer-1.0\gst-plugin-scanner.exe")
    )) {
        if (-not (Test-Path -LiteralPath $RequiredPath)) {
            throw "required MSYS2 CLANG64 GStreamer runtime file not found: $RequiredPath"
        }
    }
}

function Get-PeImportedDllNames {
    param(
        [Parameter(Mandatory = $true)][string] $Path
    )

    $Objdump = Get-Command "llvm-objdump.exe" -ErrorAction SilentlyContinue
    if ($null -eq $Objdump) {
        return @()
    }

    $Output = & $Objdump.Source -p $Path 2>$null
    $Names = @()
    foreach ($Line in $Output) {
        if ($Line -cmatch "^\s*DLL Name:\s*(.+)$") {
            $Names += $Matches[1].Trim()
        }
    }
    return $Names
}

function Copy-Clang64RuntimeDependencies {
    param(
        [Parameter(Mandatory = $true)][string[]] $EntryPoints,
        [Parameter(Mandatory = $true)][string] $DestinationDir
    )

    if (-not (Test-RunningOnWindows)) {
        return
    }

    $MsysRoot = Get-MsysRoot
    $Clang64Bin = Join-Path (Join-Path $MsysRoot "clang64") "bin"
    if (-not (Test-Path -LiteralPath $Clang64Bin -PathType Container)) {
        return
    }

    New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null
    $Queue = [System.Collections.Generic.Queue[string]]::new()
    $Seen = @{}
    foreach ($EntryPoint in $EntryPoints) {
        if (Test-Path -LiteralPath $EntryPoint -PathType Leaf) {
            $FullEntryPoint = [System.IO.Path]::GetFullPath($EntryPoint)
            if (-not $Seen.ContainsKey($FullEntryPoint)) {
                $Seen[$FullEntryPoint] = $true
                $Queue.Enqueue($FullEntryPoint)
            }
        }
    }

    while ($Queue.Count -gt 0) {
        $Current = $Queue.Dequeue()
        foreach ($DllName in (Get-PeImportedDllNames -Path $Current)) {
            $Source = Join-Path $Clang64Bin $DllName
            if (Test-Path -LiteralPath $Source -PathType Leaf) {
                Copy-Item -LiteralPath $Source -Destination $DestinationDir -Force
                $FullSource = [System.IO.Path]::GetFullPath($Source)
                if (-not $Seen.ContainsKey($FullSource)) {
                    $Seen[$FullSource] = $true
                    $Queue.Enqueue($FullSource)
                }
                continue
            }
            $Bundled = Join-Path $DestinationDir $DllName
            if (Test-Path -LiteralPath $Bundled -PathType Leaf) {
                $FullBundled = [System.IO.Path]::GetFullPath($Bundled)
                if (-not $Seen.ContainsKey($FullBundled)) {
                    $Seen[$FullBundled] = $true
                    $Queue.Enqueue($FullBundled)
                }
            }
        }
    }
}

function Copy-MsvcRuntimeDependencies {
    param(
        [Parameter(Mandatory = $true)][string] $DestinationDir
    )

    if (-not (Test-RunningOnWindows)) {
        return
    }

    $System32 = Join-Path $env:SystemRoot "System32"
    foreach ($DllName in @(
        "vcruntime140.dll",
        "vcruntime140_1.dll",
        "msvcp140.dll",
        "msvcp140_1.dll",
        "msvcp140_2.dll",
        "msvcp140_atomic_wait.dll",
        "concrt140.dll"
    )) {
        $Source = Join-Path $System32 $DllName
        if (Test-Path -LiteralPath $Source -PathType Leaf) {
            Copy-Item -LiteralPath $Source -Destination $DestinationDir -Force
        }
    }
}

function Copy-Clang64GStreamerPlugins {
    param(
        [Parameter(Mandatory = $true)][string] $DestinationDir
    )

    if (-not (Test-RunningOnWindows)) {
        return
    }

    $MsysRoot = Get-MsysRoot
    $Clang64Root = Join-Path $MsysRoot "clang64"
    $PluginSource = Join-Path $Clang64Root "lib\gstreamer-1.0"
    if (-not (Test-Path -LiteralPath $PluginSource -PathType Container)) {
        throw "GStreamer plugin directory not found: $PluginSource"
    }

    $PluginTarget = Join-Path $DestinationDir "gstreamer-1.0"
    if (Test-Path -LiteralPath $PluginTarget) {
        Remove-Item -LiteralPath $PluginTarget -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $PluginTarget | Out-Null
    Copy-Item -Path (Join-Path $PluginSource "*") -Destination $PluginTarget -Recurse -Force
    $PluginEntryPoints = @(Get-ChildItem -LiteralPath $PluginTarget -Filter "*.dll" -File -Recurse -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName })
    if ($PluginEntryPoints.Count -gt 0) {
        Copy-Clang64RuntimeDependencies -EntryPoints $PluginEntryPoints -DestinationDir $DestinationDir
    }

    $ScannerSource = Join-Path $Clang64Root "libexec\gstreamer-1.0\gst-plugin-scanner.exe"
    if (-not (Test-Path -LiteralPath $ScannerSource -PathType Leaf)) {
        throw "GStreamer plugin scanner not found: $ScannerSource"
    }
    $ScannerTarget = Join-Path $PluginTarget "gst-plugin-scanner.exe"
    Copy-Item -LiteralPath $ScannerSource -Destination $ScannerTarget -Force
    Copy-Clang64RuntimeDependencies -EntryPoints @($ScannerTarget) -DestinationDir $DestinationDir
}

function Test-WindowsSystemDll {
    param(
        [Parameter(Mandatory = $true)][string] $Name
    )

    if ($Name -match "^(api-ms-win-|ext-ms-win-)") {
        return $true
    }
    if ($Name -match "^(vcruntime|msvcp|concrt)140") {
        return $false
    }
    foreach ($Directory in @(
        (Join-Path $env:SystemRoot "System32"),
        (Join-Path $env:SystemRoot "SysWOW64")
    )) {
        if (Test-Path -LiteralPath (Join-Path $Directory $Name) -PathType Leaf) {
            return $true
        }
    }
    return $false
}

function Assert-PortableRuntimeDependencies {
    param(
        [Parameter(Mandatory = $true)][string[]] $Directories
    )

    if (-not (Test-RunningOnWindows)) {
        return
    }

    $Missing = New-Object System.Collections.Generic.List[string]
    foreach ($Directory in $Directories) {
        if (-not (Test-Path -LiteralPath $Directory -PathType Container)) {
            continue
        }
        $Available = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        Get-ChildItem -LiteralPath $Directory -Filter "*.dll" -File -ErrorAction SilentlyContinue | ForEach-Object {
            [void] $Available.Add($_.Name)
        }
        $Binaries = @()
        $Binaries += @(Get-ChildItem -LiteralPath $Directory -Filter "*.exe" -File -ErrorAction SilentlyContinue)
        $Binaries += @(Get-ChildItem -LiteralPath $Directory -Filter "*.dll" -File -ErrorAction SilentlyContinue)
        foreach ($Binary in $Binaries) {
            foreach ($DllName in (Get-PeImportedDllNames -Path $Binary.FullName)) {
                if ($Available.Contains($DllName) -or (Test-WindowsSystemDll -Name $DllName)) {
                    continue
                }
                $Missing.Add("$($Binary.Name) imports $DllName, but $DllName is not bundled in $Directory")
            }
        }
    }
    if ($Missing.Count -gt 0) {
        $Joined = [string]::Join([Environment]::NewLine, ($Missing | Sort-Object -Unique))
        throw "Missing portable runtime DLL dependencies:$([Environment]::NewLine)$Joined"
    }
}

Push-Location $Root
try {
    $RunningOnWindows = Test-RunningOnWindows
    $CargoTargetArgs = @()
    if ($RunningOnWindows) {
        $CargoTargetArgs += "--target"
        $CargoTargetArgs += "x86_64-pc-windows-gnullvm"
        Initialize-Clang64BuildEnvironment
        Assert-Clang64GStreamerBuildEnvironment
    }

    if (-not $SkipCargoBuild) {
        $CargoProfileArgs = @()
        if ($Profile -eq "release") {
            $CargoProfileArgs += "--release"
        }
        if ($MediaBackend -ne "builtin") {
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-cap
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-easnet
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-location
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-playout --features ffmpeg-runtime
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-media --features gstreamer-backend
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-cgen --features "gpu-wgpu"
        } else {
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze -p haze-cap -p haze-easnet -p haze-location -p haze-playout
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-media --features gstreamer-backend
            cargo build @CargoProfileArgs @CargoTargetArgs -p haze-cgen --features "gpu-wgpu"
        }
    }

    $ProfileDir = if ($Profile -eq "release") { "release" } else { "debug" }
    $TargetProfileDir = if ($RunningOnWindows) { "target/x86_64-pc-windows-gnullvm/$ProfileDir" } else { "target/$ProfileDir" }
    $ExePath = Join-Path $Root "$TargetProfileDir/haze.exe"
    $CapRustExePath = Join-Path $Root "$TargetProfileDir/haze-cap-ingest.exe"
    $EasNetExePath = Join-Path $Root "$TargetProfileDir/haze-easnet.exe"
    $LocationExePath = Join-Path $Root "$TargetProfileDir/haze-location.exe"
    $PlayoutExePath = Join-Path $Root "$TargetProfileDir/haze-playout-rs.exe"
    $MediaExePath = Join-Path $Root "$TargetProfileDir/haze-media.exe"
    $CgenExePath = Join-Path $Root "$TargetProfileDir/haze-cgen.exe"
    if (-not (Test-Path -LiteralPath $ExePath)) {
        throw "Missing Haze executable: $ExePath"
    }
    if (-not (Test-Path -LiteralPath $CapRustExePath)) {
        throw "Missing Rust CAP ingest executable: $CapRustExePath"
    }
    if (-not (Test-Path -LiteralPath $EasNetExePath)) {
        throw "Missing Rust EAS NET executable: $EasNetExePath"
    }
    if (-not (Test-Path -LiteralPath $LocationExePath)) {
        throw "Missing Rust location executable: $LocationExePath"
    }
    if (-not (Test-Path -LiteralPath $PlayoutExePath)) {
        throw "Missing Rust playout executable: $PlayoutExePath"
    }
    if (-not (Test-Path -LiteralPath $MediaExePath)) {
        throw "Missing Rust media executable: $MediaExePath"
    }
    if (-not (Test-Path -LiteralPath $CgenExePath)) {
        throw "Missing Rust cgen executable: $CgenExePath"
    }

    New-Item -ItemType Directory -Force -Path $OutFull | Out-Null
    New-Item -ItemType Directory -Force -Path $BinFull | Out-Null

    $BundleOwnedFiles = @(
        "haze.exe",
        "haze.cmd",
        "README-runtime.txt",
        "config.yaml",
        ".env.example",
        ".haze-runtime"
    )
    $BundleOwnedBinFiles = @(
        "haze.exe",
        "haze-web.exe",
        "haze-data-ingest.exe",
        "haze-cap-ingest.exe",
        "haze-easnet.exe",
        "haze-location.exe",
        "haze-tts.exe",
        "haze-sapi5-shim.exe",
        "haze-product-render.exe",
        "haze-playlist.exe",
        "haze-webhook.exe",
        "haze-asr.exe",
        "whisper-server.exe",
        "haze-ivr.exe",
        "haze-playout.exe",
        "haze-playout-rs.exe",
        "haze-media.exe",
        "haze-cgen.exe",
        "libopus-0.dll",
        "libopusfile-0.dll",
        "libogg-0.dll",
        "libunwind.dll",
        "opus.dll",
        "sherpa-onnx-c-api.dll",
        "sherpa-onnx-cxx-api.dll"
    )
    $LegacyBundleBinFiles = @(
        "haze-cap-ingest-rs.exe"
    )
    foreach ($File in $BundleOwnedFiles) {
        $Target = Join-Path $OutFull $File
        if (Test-Path -LiteralPath $Target) {
            Remove-Item -LiteralPath $Target -Force
        }
    }
    foreach ($File in $BundleOwnedBinFiles) {
        $Target = Join-Path $BinFull $File
        if (Test-Path -LiteralPath $Target) {
            Remove-Item -LiteralPath $Target -Force
        }
        $LegacyTarget = Join-Path $OutFull $File
        if (Test-Path -LiteralPath $LegacyTarget) {
            Remove-Item -LiteralPath $LegacyTarget -Force
        }
    }
    foreach ($File in $LegacyBundleBinFiles) {
        foreach ($Dir in @($OutFull, $BinFull)) {
            $Target = Join-Path $Dir $File
            if (Test-Path -LiteralPath $Target) {
                Remove-Item -LiteralPath $Target -Force
            }
        }
    }
    foreach ($DllDir in @($OutFull, $BinFull)) {
        if (Test-Path -LiteralPath $DllDir -PathType Container) {
            Get-ChildItem -LiteralPath $DllDir -Filter "*.dll" -File -ErrorAction SilentlyContinue |
                Remove-Item -Force
        }
    }
    foreach ($Dir in @("webroot", "managed", "audio")) {
        $Target = Join-Path $OutFull $Dir
        if (Test-Path -LiteralPath $Target) {
            Remove-Item -LiteralPath $Target -Recurse -Force
        }
    }

    Copy-Item -LiteralPath $ExePath -Destination (Join-Path $BinFull "haze.exe") -Force
    Copy-Item -LiteralPath $CapRustExePath -Destination (Join-Path $BinFull "haze-cap-ingest.exe") -Force
    Copy-Item -LiteralPath $EasNetExePath -Destination (Join-Path $BinFull "haze-easnet.exe") -Force
    Copy-Item -LiteralPath $LocationExePath -Destination (Join-Path $BinFull "haze-location.exe") -Force
    Copy-Item -LiteralPath $PlayoutExePath -Destination (Join-Path $BinFull "haze-playout-rs.exe") -Force
    Copy-Item -LiteralPath $MediaExePath -Destination (Join-Path $BinFull "haze-media.exe") -Force
    Copy-Item -LiteralPath $CgenExePath -Destination (Join-Path $BinFull "haze-cgen.exe") -Force

    if (-not $SkipGoServices) {
        & (Join-Path $ScriptDir "build-go-services.ps1") -OutputDir $OutputDir
    }
    if ($RunningOnWindows) {
        $ServiceEntryPoints = @()
        if (Test-Path -LiteralPath $BinFull -PathType Container) {
            $ServiceEntryPoints = @(Get-ChildItem -LiteralPath $BinFull -Filter "*.exe" -File -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName })
        }
        if ($ServiceEntryPoints.Count -gt 0) {
            Copy-Clang64RuntimeDependencies -EntryPoints $ServiceEntryPoints -DestinationDir $BinFull
            Copy-Clang64GStreamerPlugins -DestinationDir $BinFull
            Copy-MsvcRuntimeDependencies -DestinationDir $BinFull
        }
        Assert-PortableRuntimeDependencies -Directories @($BinFull)
    }

    foreach ($Item in @("config.yaml", ".env.example")) {
        if (Test-Path -LiteralPath $Item) {
            Copy-Item -LiteralPath $Item -Destination $OutFull -Force
        }
    }

    foreach ($BundledDir in @("webroot", "managed", "audio")) {
        Copy-BundleDirectory -Name $BundledDir
    }
    $ManagedOut = Join-Path $OutFull "managed"
    New-Item -ItemType Directory -Force -Path $ManagedOut | Out-Null
    if ((Test-Path -LiteralPath "scripts/tts/chatterbox_infer.py") -or (Test-Path -LiteralPath "scripts/tts/f5_infer.py") -or (Test-Path -LiteralPath "scripts/install-whisper-model.ps1")) {
        $ManagedScripts = Join-Path $ManagedOut "scripts"
        New-Item -ItemType Directory -Force -Path $ManagedScripts | Out-Null
        foreach ($Script in @("scripts/tts/chatterbox_infer.py", "scripts/tts/f5_infer.py", "scripts/install-whisper-model.sh", "scripts/install-whisper-model.ps1")) {
            if (Test-Path -LiteralPath $Script) {
                Copy-Item -LiteralPath $Script -Destination $ManagedScripts -Force
            }
        }
    }

    New-Item -ItemType Directory -Force -Path (Join-Path $OutFull "audio") | Out-Null

    foreach ($Dir in @("audio/_uploads", "audio/_previews", "bin", "logs", "runtime", "runtime/audio/alerts", "runtime/audio/playlist", "runtime/audio/playout", "runtime/audio/tts", "runtime/feeds", "runtime/models/whisper", "runtime/playlists", "runtime/queues/alerts", "runtime/state")) {
        New-Item -ItemType Directory -Force -Path (Join-Path $OutFull $Dir) | Out-Null
    }

    "Haze Weather Radio runtime directory" |
        Set-Content -LiteralPath (Join-Path $OutFull ".haze-runtime") -Encoding ASCII

    if ($IncludeEnv -and (Test-Path -LiteralPath ".env")) {
        Copy-Item -LiteralPath ".env" -Destination $OutFull -Force
    }

@"
@echo off
set "HAZE_HOME=%~dp0."
set "PATH=%~dp0bin;%PATH%"
set "GST_PLUGIN_PATH=%~dp0bin\gstreamer-1.0"
set "GST_PLUGIN_SYSTEM_PATH_1_0=%~dp0bin\gstreamer-1.0"
set "GST_PLUGIN_SCANNER=%~dp0bin\gstreamer-1.0\gst-plugin-scanner.exe"
"%~dp0bin\haze.exe" %*
set "HAZE_EXIT=%ERRORLEVEL%"
if not "%HAZE_EXIT%"=="0" (
    echo.
    echo Haze exited with code %HAZE_EXIT%.
    echo The console is being kept open so the error above can be read.
    pause
)
exit /b %HAZE_EXIT%
"@ | Set-Content -LiteralPath (Join-Path $OutFull "haze.cmd") -Encoding ASCII

    @"
Haze Weather Radio host bundle

Run:
  haze.cmd --config config.yaml

Bundled service executables are kept in:
  bin/

The top-level launcher sets PATH for the native runtime DLLs in bin/ and then
starts bin/haze.exe. Managed services are launched by Haze and can also be
extracted from its embedded service payload.

This bundle was built from:
  $Root
"@ | Set-Content -LiteralPath (Join-Path $OutFull "README-runtime.txt") -Encoding UTF8

    Write-Host "Built $OutFull"
    Write-Host "Run: $OutFull\haze.cmd --config config.yaml"
} finally {
    Pop-Location
}
