# Grafana Dashboard Validation

This folder contains Grafana dashboard JSON files and schema validation assets.

## Files

- `metrics.json`: Main dashboard JSON.
- `realtime.json`: Realtime dashboard JSON.
- `schema/dashboard_spec_v2.cue`: Official Grafana v2 CUE schema source used for validation. This file is downloaded locally and ignored by git.

## Validation Options

Four validation tasks are configured in VS Code:

1. `Validate Grafana dashboard (JSON only)`
- Checks JSON syntax for `grafana/metrics.json`.
- Fast check for malformed JSON.

2. `Validate Grafana dashboard (CUE)`
- Runs JSON syntax validation first.
- Then validates `grafana/metrics.json` against Grafana's v2 CUE schema.

3. `Validate Grafana realtime (JSON only)`
- Checks JSON syntax for `grafana/realtime.json`.
- Fast check for malformed JSON.

4. `Validate Grafana realtime (CUE)`
- Runs JSON syntax validation first.
- Then validates `grafana/realtime.json` against Grafana's v2 CUE schema.

Task definitions are in `.vscode/tasks.json`.

## Script

Validation script:

- `scripts/validate-grafana-dashboard.ps1`

Default behavior:

- Dashboard path: `grafana/metrics.json`
- Schema path: `grafana/schema/dashboard_spec_v2.cue`

Run manually from repo root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate-grafana-dashboard.ps1
```

Validate realtime manually:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate-grafana-dashboard.ps1 -DashboardPath grafana/realtime.json
```

## CUE CLI Setup

CUE is required for schema validation.

Install with winget:

```powershell
winget install --id CueLang.Cue --exact --accept-source-agreements --accept-package-agreements
```

Verify installation:

```powershell
cue version
```

Note:

- The script includes a fallback that locates `cue.exe` in the winget package directory if PATH has not refreshed yet.

## Updating the Official Schema

Refresh the local schema copies:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/download-grafana-schema.ps1
```

This downloads the Grafana dashboard schema files into `grafana/schema/` for local validation. They are not committed to the repository.

## Common Issues

- `CUE CLI is not installed`:
  - Install CUE using the command above.
- JSON passes but CUE fails:
  - The file is valid JSON but does not match Grafana v2 schema constraints.
  - Review the reported path and type mismatch in the validator output.
