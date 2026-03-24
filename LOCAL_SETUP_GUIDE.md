# NEXT200 로컬 실행 가이드

이 문서는 다른 사용자가 GitHub 저장소를 내려받은 뒤, 로컬 환경에서 NEXT200 서비스를 실행하는 방법을 처음부터 차근차근 설명합니다.

## 1. 먼저 준비할 것

GitHub 저장소에는 보안 또는 용량 문제 때문에 일부 파일이 포함되지 않습니다.
따라서 저장소를 clone한 뒤 아래 파일을 별도로 준비해야 합니다.

- `.env`
- `data/raw/kospi_db_full_20260320.sql`
- `data/incoming/manual/actual_kospi200_2026.csv`

또한 미래 예측 결과를 새로 만들고 싶다면 앱 실행 전에 데이터 수집과 예측 실행을 한 번 먼저 진행해야 합니다.

## 2. 준비 환경

필수 환경은 아래와 같습니다.

- Windows
- Python 3.12 권장
- PowerShell
- Git

API 키가 있으면 아래도 준비합니다.

- 네이버 뉴스 API Client ID
- 네이버 뉴스 API Client Secret
- OpenDART API Key

## 3. 저장소 받기

PowerShell에서 원하는 경로로 이동한 뒤 저장소를 clone 합니다.

```powershell
git clone 저장소URL
cd 저장소폴더
```

예시:

```powershell
git clone https://github.com/sohson/fa08-2nd-numbers.git
cd fa08-2nd-numbers
```

## 4. 가상환경 생성

프로젝트 폴더에서 아래 명령을 실행합니다.

```powershell
py -3.12 -m venv .venv
```

가상환경을 활성화합니다.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## 5. 패키지 설치

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 6. .env 파일 만들기

프로젝트 루트에 `.env` 파일을 만들고 아래 값을 채웁니다.

```env
NAVER_CLIENT_ID=여기에_클라이언트_ID
NAVER_CLIENT_SECRET=여기에_클라이언트_SECRET
OPEN_DART_API_KEY=여기에_API_KEY
```

뉴스 기능을 쓰지 않을 경우 네이버 키가 없으면 관련 뉴스 일부가 제한될 수 있습니다.

## 7. GitHub에 없는 필수 파일 배치

아래 파일들을 각각 같은 경로에 넣어야 합니다.

- `data/raw/kospi_db_full_20260320.sql`
- `data/incoming/manual/actual_kospi200_2026.csv`

폴더가 없으면 먼저 생성합니다.

```powershell
New-Item -ItemType Directory -Force data\raw
New-Item -ItemType Directory -Force data\incoming\manual
New-Item -ItemType Directory -Force data\incoming\auto
New-Item -ItemType Directory -Force logs
```

## 8. 앱만 실행하는 방법

가상환경 기준으로 아래 명령을 실행합니다.

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

브라우저에서 아래 주소로 접속합니다.

- http://127.0.0.1:8501

## 9. 미래 예측 데이터를 새로 만들고 싶을 때

`2026_H1` 같은 미래 반기는 먼저 자동 수집과 예측을 실행해야 합니다.

### 9-1. 데이터 수집

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
```

### 9-2. 예측 실행

```powershell
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

### 9-3. 한 번에 실행

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\weekly_run.ps1
```

## 10. 팀원들과 비슷한 2026_H1 결과를 보고 싶을 때

미래 예측은 외부 데이터 소스를 다시 읽기 때문에 실행 시점이 다르면 결과가 조금 달라질 수 있습니다.
가능하면 팀원들과 같은 날, 같은 시간대에 아래 순서로 함께 실행하는 것이 좋습니다.

```powershell
cd C:\Users\Admin\numbers
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
powershell.exe -ExecutionPolicy Bypass -File .\weekly_run.ps1
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

## 11. 자주 발생하는 문제

### PowerShell 실행 정책 오류

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### 실시간 우측 사이드바 가격이 갱신되지 않을 때

시스템 Python이 아니라 반드시 프로젝트 가상환경으로 앱을 실행해야 합니다.

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

### `2026_H1` 결과가 다른 사람과 다를 때

미래 반기는 최신 데이터 기반으로 다시 계산되기 때문에 실행 시점 차이로 결과가 달라질 수 있습니다.
같은 시간대에 수집과 예측을 실행하면 결과를 더 비슷하게 맞출 수 있습니다.

## 12. 핵심 파일 설명

- `app.py`: Streamlit 메인 앱
- `src/predictor.py`: 모델 예측 로직
- `src/pipeline/feature_builder.py`: 피처 생성 로직
- `run_weekly_collection.py`: 최신 데이터 수집 실행
- `run_weekly_prediction.py`: 예측 실행
- `weekly_run.ps1`: 수집과 예측 일괄 실행
- `data/raw/model_package.pkl`: 학습된 모델 패키지
- `data/raw/kospi_db_full_20260320.sql`: 과거 실제/예측 데이터 원본

## 13. 실행 순서 요약

1. 저장소 clone
2. `.venv` 생성
3. 패키지 설치
4. `.env` 작성
5. 누락 파일 배치
6. 필요하면 `weekly_run.ps1` 실행
7. `streamlit run app.py` 실행

이 순서대로 진행하면 로컬에서 NEXT200 서비스를 정상 실행할 수 있습니다.
