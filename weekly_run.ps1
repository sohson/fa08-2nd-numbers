param(
    [switch]$SkipCollection,
    [switch]$SkipPrediction,
    [string]$LiveProjectRoot = "C:\Users\Admin\numbers"
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = "C:\Users\Admin\AppData\Local\Programs\Python\Python312\python.exe"
$LogsDir = Join-Path $ProjectRoot "logs"
$AutoDir = Join-Path $ProjectRoot "data\incoming\auto"
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogPath = Join-Path $LogsDir "weekly_run_$RunStamp.log"

if (-not (Test-Path $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir | Out-Null
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable was not found: $PythonExe"
}

Set-Location $ProjectRoot

function Write-RunLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $line
    Add-Content -Path $LogPath -Value $line -Encoding UTF8
}

function Invoke-Step {
    param(
        [string]$Name,
        [string]$ScriptPath
    )

    Write-RunLog "$Name 시작"
    & $PythonExe $ScriptPath 2>&1 | Tee-Object -FilePath $LogPath -Append
    if ($LASTEXITCODE -ne 0) {
        throw "$Name 실패 (exit code: $LASTEXITCODE)"
    }
    Write-RunLog "$Name 완료"
}

function Sync-ToLiveApp {
    param([string]$SourceDir, [string]$TargetRoot)

    $TargetDir = Join-Path $TargetRoot "data\incoming\auto"
    if (-not (Test-Path $SourceDir)) {
        Write-RunLog "Auto output directory not found, skipping live sync: $SourceDir"
        return
    }
    if (-not (Test-Path $TargetDir)) {
        New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null
    }

    $patterns = @(
        "weekly_predictions_*.csv",
        "weekly_strong_in_*.csv",
        "weekly_strong_out_*.csv",
        "feature_krx_*.csv",
        "major_holder_*.csv",
        "model_input_*.csv",
        "weekly_collection_summary.json",
        "weekly_prediction_summary.json",
        "naver_foreign_holding_weekly.csv",
        "naver_stock_meta_weekly.csv",
        "yahoo_price_daily.csv",
        "dart_major_holder_weekly.csv"
    )

    foreach ($pattern in $patterns) {
        Get-ChildItem -Path $SourceDir -Filter $pattern -ErrorAction SilentlyContinue | ForEach-Object {
            Copy-Item -Path $_.FullName -Destination (Join-Path $TargetDir $_.Name) -Force
        }
    }

    Write-RunLog "Synced weekly outputs to live app: $TargetDir"
}

Write-RunLog "Weekly automation run started"
Write-RunLog "Project root: $ProjectRoot"

if (-not $SkipCollection) {
    Invoke-Step -Name "Weekly data collection" -ScriptPath (Join-Path $ProjectRoot "run_weekly_collection.py")
} else {
    Write-RunLog "Weekly data collection skipped"
}

if (-not $SkipPrediction) {
    Invoke-Step -Name "Weekly prediction" -ScriptPath (Join-Path $ProjectRoot "run_weekly_prediction.py")
} else {
    Write-RunLog "Weekly prediction skipped"
}

Sync-ToLiveApp -SourceDir $AutoDir -TargetRoot $LiveProjectRoot

$Summary = @{
    run_at = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    log_path = $LogPath
    collection_summary = Join-Path $ProjectRoot "data\incoming\auto\weekly_collection_summary.json"
    prediction_summary = Join-Path $ProjectRoot "data\incoming\auto\weekly_prediction_summary.json"
}

$SummaryPath = Join-Path $LogsDir "weekly_run_latest.json"
$Summary | ConvertTo-Json -Depth 3 | Set-Content -Path $SummaryPath -Encoding UTF8

Write-RunLog "Weekly automation run completed"
Write-RunLog "Log file: $LogPath"
Write-RunLog "Summary file: $SummaryPath"
