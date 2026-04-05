# download_js_files.ps1
# PowerShell script to download Google Nest web app JS resources for reverse-engineering.

$TargetDir = $PSScriptRoot
if (-not $TargetDir) {
    $TargetDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if (-not $TargetDir) {
    $TargetDir = "."
}

if (-not (Test-Path $TargetDir)) {
    New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null
}

$FilesToDownload = [ordered]@{
    "common-e754c34dcc5abc22ba35.js"   = "https://home.nest.com/js/_vendors_/common-e754c34dcc5abc22ba35.js"
    "main-36606966c8417ddf91d0.js"     = "https://home.nest.com/js/_shared_/main-36606966c8417ddf91d0.js"
    "main-84209c0f2f2486fc3627.js"     = "https://home.nest.com/js/_vendors_/main-84209c0f2f2486fc3627.js"
    "sdk-421f7ba4a82b319d562a.js"      = "https://home.nest.com/js/_vendors_/lib/phoenix-sdk/sdk-421f7ba4a82b319d562a.js"
    "thermostat-c75045b13e00ac156fa1.js" = "https://home.nest.com/js/bundles/thermostat-c75045b13e00ac156fa1.js"
}

Write-Host "Downloading Google Nest JS files to $TargetDir..." -ForegroundColor Cyan

foreach ($FileName in $FilesToDownload.Keys) {
    $Url = $FilesToDownload[$FileName]
    $OutPath = Join-Path $TargetDir $FileName
    
    Write-Host "  Downloading $FileName..." -NoNewline
    try {
        Invoke-WebRequest -Uri $Url -OutFile $OutPath -UseBasicParsing -ErrorAction Stop
        Write-Host " Done." -ForegroundColor Green
    } catch {
        Write-Host " FAILED. ($($_.Exception.Message))" -ForegroundColor Red
    }
}
