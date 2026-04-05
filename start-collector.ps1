param(
    [string]$SourceConfigDir = "./config",
    [switch]$Build,
    [switch]$ForceConfigSync,
    [switch]$ForceRecreate,
    [Alias("h")]
    [switch]$Help
)

$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Host "Usage: .\start-collector.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -SourceConfigDir <path>  Source config folder to copy from (default: ./config)"
    Write-Host "  -Build                   Build images before starting collector"
    Write-Host "  -ForceRecreate           Force container recreation on startup"
    Write-Host "  -ForceConfigSync         Always copy source config into /app/config"
    Write-Host "  -h, -Help                Show this help text"
    return
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = $scriptDir

Set-Location $repoRoot

$sourcePath = if ([System.IO.Path]::IsPathRooted($SourceConfigDir)) {
    $SourceConfigDir
} else {
    Join-Path $scriptDir $SourceConfigDir
}

if (-not (Test-Path $sourcePath)) {
    throw "Config source directory not found: $sourcePath"
}

$composeArgs = @("compose", "up", "-d")
if ($Build) {
    $composeArgs += "--build"
}
if ($ForceRecreate) {
    $composeArgs += "--force-recreate"
}
$composeArgs += "collector"

Write-Host "Starting collector container..."
& docker @composeArgs

$configState = (& docker exec nest_collector sh -lc "if [ -f /app/config/config.json ]; then echo present; else echo missing; fi").Trim()
if ($LASTEXITCODE -ne 0) {
    throw "Could not check /app/config/config.json inside container nest_collector."
}

if ($ForceConfigSync -or $configState -eq "missing") {
    Write-Host "Copying config files from $sourcePath to container /app/config..."
    & docker cp "$sourcePath/." "nest_collector:/app/config"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy config files to nest_collector:/app/config"
    }

    Write-Host "Restarting collector container to apply updated config..."
    & docker restart nest_collector | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to restart container nest_collector after config sync"
    }
} else {
    Write-Host "Existing /app/config/config.json found in persistent volume; skipping config copy."
    Write-Host "Use -ForceConfigSync to overwrite with local source config files."
}

Write-Host "Done."
Write-Host "Collector is running and config files are synced to /app/config."
