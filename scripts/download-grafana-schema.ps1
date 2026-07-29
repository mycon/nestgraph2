param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$schemaDir = Join-Path $repoRoot "grafana/schema"

New-Item -ItemType Directory -Force -Path $schemaDir | Out-Null

$schemaFiles = @(
    @{
        Name = "dashboard_spec_v2.cue"
        Url  = "https://raw.githubusercontent.com/grafana/grafana/main/apps/dashboard/pkg/apis/dashboard/v2/dashboard_spec.cue"
    },
    @{
        Name = "dashboard_kind.cue"
        Url  = "https://raw.githubusercontent.com/grafana/grafana/main/apps/dashboard/pkg/apis/dashboard/v1/dashboard_kind.cue"
    }
)

foreach ($schemaFile in $schemaFiles) {
    $destinationPath = Join-Path $schemaDir $schemaFile.Name

    if ((-not $Force) -and (Test-Path -Path $destinationPath)) {
        continue
    }

    Invoke-WebRequest -Uri $schemaFile.Url -OutFile $destinationPath
    Write-Output "Downloaded $($schemaFile.Name)"
}

Write-Output "PASS: Grafana schema files are available in $schemaDir."