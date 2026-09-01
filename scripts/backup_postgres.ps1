param(
    [string]$OutputDir = "backups"
)

$ErrorActionPreference = "Stop"

python -m scripts.postgres_backup backup --output $OutputDir
if ($LASTEXITCODE -ne 0) { throw "Backup failed" }
