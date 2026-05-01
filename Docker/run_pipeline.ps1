# =============================================================================
# Docker/run_pipeline.ps1 - Daily pipeline runner.
#
# Invoked by Windows Task Scheduler. Ensures Postgres is up, then runs ONE
# pass of the AI News Aggregator pipeline (scrape -> summarize -> email).
# Logs to <project>/logs/pipeline_YYYY-MM-DD.log.
#
# Usage (manual smoke test):
#   pwsh -File Docker/run_pipeline.ps1
#
# Pre-reqs:
#   - Docker Desktop running (set it to start at login)
#   - .env at the project root with DATABASE_URL (currently Neon),
#     OPENAI_API_KEY, SMTP_*, DIGEST_TO
#   - Image built: see README under Deployment for `docker compose build`
# =============================================================================

$ErrorActionPreference = "Continue"

# Resolve the project root from this script's location, regardless of where
# Task Scheduler invokes it from.
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$LogDir  = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$LogFile = Join-Path $LogDir ("pipeline_" + (Get-Date -Format "yyyy-MM-dd") + ".log")

function Log-Line {
    param([string]$msg)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts $msg" | Tee-Object -FilePath $LogFile -Append
}

Log-Line "=============================================================="
Log-Line "pipeline run start"
Log-Line "project root: $ProjectRoot"

# Run the pipeline once. The app talks to whatever DATABASE_URL points at
# (currently Neon). --rm removes the container after exit so a daily cron
# doesn't accumulate dead containers.
Log-Line "running pipeline once..."
docker compose -f Docker/docker-compose.yml --env-file .env --profile pipeline `
    run --rm app 2>&1 |
    Tee-Object -FilePath $LogFile -Append

$exit = $LASTEXITCODE
Log-Line "pipeline run end (exit=$exit)"
exit $exit
