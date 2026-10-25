# Lint the protobuf contracts and regenerate the committed Go code.
#
#   scripts/gen-proto.ps1                           lint and regenerate
#   scripts/gen-proto.ps1 -Check                    also fail if generated code is stale
#   scripts/gen-proto.ps1 -BreakingAgainst main     also check wire compatibility
#
# buf and the protoc plugins run through `go run` at pinned versions, so only
# Go is required. Rust compiles the same .proto files in its own build.rs.
[CmdletBinding()]
param(
    [switch] $Check,
    [string] $BreakingAgainst = ""
)

$ErrorActionPreference = "Stop"
$BufVersion = "v1.50.0"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $Root
try {
    function Invoke-Buf {
        go run "github.com/bufbuild/buf/cmd/buf@$BufVersion" @args
        if ($LASTEXITCODE -ne 0) {
            throw "buf $($args -join ' ') failed with exit code $LASTEXITCODE"
        }
    }

    Invoke-Buf lint
    if ($BreakingAgainst) {
        Invoke-Buf breaking --against ".git#ref=$BreakingAgainst"
    }
    Invoke-Buf generate

    if ($Check) {
        $changes = git status --porcelain -- services/go/internal/gen
        if ($changes) {
            Write-Error "Generated protobuf code is out of date. Run scripts/gen-proto.ps1 and commit the result.`n$changes"
        }
    }
}
finally {
    Pop-Location
}
