# Copies the sherpa-onnx runtime libraries the Go TTS providers (Piper, Kokoro)
# load at run time into a bundle directory. Dot-source this file, then run from
# services/go so `go env` and `go list` see the module.
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
