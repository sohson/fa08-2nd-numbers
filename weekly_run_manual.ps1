param(
    [string]$Period = "",
    [switch]$SkipPipeline,
    [switch]$OpenApp
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $PythonExe)) {
    throw "가상환경 Python을 찾을 수 없습니다: $PythonExe"
}

Set-Location $ProjectRoot

Write-Host "[1/3] 데이터 상태 점검"
& $PythonExe inspect_data.py

if (-not $SkipPipeline -and $Period) {
    Write-Host "[2/3] 파이프라인 실행 시도: $Period"
    & $PythonExe pipeline.py --period $Period
} else {
    Write-Host "[2/3] 파이프라인 단계 건너뜀"
}

if ($OpenApp) {
    Write-Host "[3/3] Streamlit 실행"
    & $PythonExe -m streamlit run app.py
} else {
    Write-Host "[3/3] 앱 실행 생략"
}
