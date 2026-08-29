param(
    [string]$PythonCommand = "python",
    [string[]]$PythonArgs = @()
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

docker compose up -d postgres

if (-not (Test-Path ".venv")) {
    & $PythonCommand @PythonArgs -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

.\.venv\Scripts\python.exe -m ingestion.static.download_mbta_gtfs
.\.venv\Scripts\python.exe -m ingestion.static.load_static_gtfs

Push-Location dbt
..\.venv\Scripts\dbt.exe build --profiles-dir .
Pop-Location

Write-Host ""
Write-Host "Smoke test complete. Start the dashboard with:"
Write-Host ".\.venv\Scripts\streamlit.exe run dashboard/app.py"
