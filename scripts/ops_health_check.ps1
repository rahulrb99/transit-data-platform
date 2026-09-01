$ErrorActionPreference = "Stop"

python -m scripts.ops_health_check
if ($LASTEXITCODE -ne 0) { throw "One or more operational probes failed" }
