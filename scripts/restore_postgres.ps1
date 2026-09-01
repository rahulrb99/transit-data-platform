param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath,
    [Parameter(Mandatory = $true)]
    [string]$TargetDatabase
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $BackupPath)) {
    throw "Backup file does not exist: $BackupPath"
}

python -m scripts.postgres_backup restore $BackupPath --database $TargetDatabase
if ($LASTEXITCODE -ne 0) { throw "Restore failed; working database was not changed" }
