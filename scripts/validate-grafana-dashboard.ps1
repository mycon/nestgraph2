param(
    [string]$DashboardPath = "grafana/metrics.json",
    [string]$SchemaPath = "grafana/schema/dashboard_spec_v2.cue"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$dashboardFullPath = Join-Path $repoRoot $DashboardPath
$schemaFullPath = Join-Path $repoRoot $SchemaPath

if (-not (Test-Path -Path $dashboardFullPath)) {
    Write-Error "Dashboard file not found: $dashboardFullPath"
    exit 1
}

if (-not (Test-Path -Path $schemaFullPath)) {
    Write-Error "Schema file not found: $schemaFullPath"
    exit 1
}

try {
    $null = Get-Content -Raw -Path $dashboardFullPath | ConvertFrom-Json -ErrorAction Stop
    Write-Output "PASS: JSON syntax is valid ($DashboardPath)."
} catch {
    Write-Error "FAIL: JSON syntax invalid ($DashboardPath). $($_.Exception.Message)"
    exit 1
}

$cueExe = $null
$cueCmd = Get-Command cue -ErrorAction SilentlyContinue
if ($cueCmd) {
    $cueExe = $cueCmd.Source
}

if (-not $cueExe) {
    $wingetCue = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\CueLang.Cue_*" -Filter cue.exe -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($wingetCue) {
        $cueExe = $wingetCue.FullName
    }
}

if (-not $cueExe) {
    Write-Error "FAIL: CUE CLI is not installed. Install from https://cuelang.org/docs/install/ and rerun this task."
    exit 1
}

Push-Location $repoRoot
try {
    & $cueExe vet $dashboardFullPath $schemaFullPath -d "DashboardSpec"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "FAIL: CUE schema validation failed for $DashboardPath against $SchemaPath."
        exit $LASTEXITCODE
    }
    Write-Output "PASS: CUE schema validation passed."
} finally {
    Pop-Location
}
