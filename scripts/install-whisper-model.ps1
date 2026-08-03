param(
    [string] $Destination = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ModelName = "base-q5_1"
$ModelSHA256 = "422f1ae452ade6f30a004d7e5c6a43195e4433bc370bf23fac9cc591f01a8898"
$ModelURL = "https://huggingface.co/ggerganov/whisper.cpp/resolve/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-base-q5_1.bin"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ScriptParentName = Split-Path -Leaf (Split-Path -Parent $ScriptDir)
$RuntimeRoot = if ($ScriptParentName -eq "managed") {
    [System.IO.Path]::GetFullPath((Join-Path $ScriptDir "..\.."))
} else {
    [System.IO.Path]::GetFullPath((Join-Path $ScriptDir ".."))
}
if ([string]::IsNullOrWhiteSpace($Destination)) {
    $Destination = Join-Path $RuntimeRoot "runtime\models\whisper\ggml-$ModelName.bin"
}
$Destination = [System.IO.Path]::GetFullPath($Destination)
$DestinationDir = Split-Path -Parent $Destination
New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null

if (Test-Path -LiteralPath $Destination -PathType Leaf) {
    $ExistingSHA = (Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($ExistingSHA -eq $ModelSHA256) {
        Write-Host "Local Whisper model is already installed: $Destination"
        exit 0
    }
    throw "Refusing to overwrite a model with an unexpected checksum: $Destination"
}

$Temporary = "$Destination.part.$PID"
try {
    Invoke-WebRequest -Uri $ModelURL -OutFile $Temporary -MaximumRedirection 5
    $DownloadedSHA = (Get-FileHash -LiteralPath $Temporary -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($DownloadedSHA -ne $ModelSHA256) {
        throw "Downloaded local Whisper model failed SHA-256 verification"
    }
    Move-Item -LiteralPath $Temporary -Destination $Destination
} finally {
    if (Test-Path -LiteralPath $Temporary) {
        Remove-Item -LiteralPath $Temporary -Force
    }
}

Write-Host "Installed local Whisper model: $Destination"
