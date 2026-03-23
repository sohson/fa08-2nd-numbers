# NEXT200 로컬 실행 가이드

이 문서는 다른 사용자가 GitHub 저장소를 내려받은 뒤, 로컬에서 `NEXT200` 서비스를 실행하는 방법을 처음부터 끝까지 설명합니다.

이 프로젝트는 다음 두 가지를 모두 포함합니다.

- Streamlit 기반 대시보드 실행
- 최신 데이터 수집 후 미래 반기 예측 실행

## 1. 먼저 알아둘 점

이 저장소는 보안/용량 문제 때문에 일부 파일을 Git에 올리지 않습니다.

즉, 저장소를 clone한 뒤에도 아래 파일은 별도로 준비해야 할 수 있습니다.

- `.env`
- `data/raw/kospi_db_full_20260320.sql`
- `data/incoming/manual/actual_kospi200_2026.csv`

또한 자동 수집 결과물은 Git에 포함되지 않습니다.

- `data/incoming/auto/*.csv`
- `data/incoming/auto/*.json`

즉 미래 예측을 보려면, 로컬에서 직접 수집/예측을 한 번 실행해야 합니다.

## 2. 준비물

필수:

- Windows
- Python 3.12 권장
- PowerShell
- Git

선택:

- 네이버 뉴스 API 키
- OpenDART API 키

## 3. 저장소 받기

원하는 폴더에서 아래 명령을 실행합니다.

```powershell
git clone 저장소URL
cd fa08-2nd-numbers
```

예시:

```powershell
git clone https://github.com/sohson/fa08-2nd-numbers.git
cd fa08-2nd-numbers
```

이 문서에서는 프로젝트 루트를 아래처럼 가정합니다.

```text
C:\Users\사용자명\fa08-2nd-numbers
```

## 4. Git에 없는 필수 파일 준비

`.gitignore` 기준으로, 아래 파일은 저장소에 포함되지 않습니다.

### 4-1. SQL dump 파일

아래 경로에 파일을 넣습니다.

```text
data/raw/kospi_db_full_20260320.sql
```

이 파일은 과거 반기 데이터, labels, predictions, kospi_friday_daily 등을 읽는 핵심 원본입니다.

### 4-2. 실제 KOSPI200 구성종목 CSV

아래 경로에 파일을 넣습니다.

```text
data/incoming/manual/actual_kospi200_2026.csv
```

이 파일은 중앙 하단의 `실제 KOSPI200 섹터 비율` 차트에 사용됩니다.

### 4-3. 자동 수집 결과물

이 파일들은 Git에 없고, 처음엔 없어도 됩니다.

```text
data/incoming/auto/
```

이 폴더의 결과물은 아래 명령으로 직접 생성합니다.

- `run_weekly_collection.py`
- `run_weekly_prediction.py`

## 5. 가상환경 만들기

프로젝트 루트에서 아래 명령을 실행합니다.

```powershell
python -m venv .venv
```

PowerShell 정책 때문에 활성화가 안 되면, 아래처럼 먼저 한 번 허용합니다.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

그 다음 활성화:

```powershell
.\.venv\Scripts\Activate.ps1
```

활성화가 불편하면, 이후 명령은 모두 `.\.venv\Scripts\python.exe`로 직접 실행해도 됩니다.

## 6. 패키지 설치

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

중요:

- 이 프로젝트는 `pandas.read_html()`를 사용하기 때문에 HTML parser가 필요합니다.
- `requirements.txt`를 정상 설치하면 `lxml` 같은 패키지도 함께 설치됩니다.
- 앱은 가능하면 **반드시 이 프로젝트의 `.venv`** 로 실행해야 합니다.

## 7. 환경변수 파일 만들기

`.env.example`을 복사해서 `.env`를 만듭니다.

```powershell
Copy-Item .env.example .env
```

그 다음 `.env` 파일 안에 실제 값을 넣습니다.

예시:

```env
KRXDATA_API_KEY=여기에값
NAVER_CLIENT_ID=여기에값
NAVER_CLIENT_SECRET=여기에값
OPEN_DART_API_KEY=여기에값
ECOS_API_KEY=여기에값
APP_SQL_DUMP=data/raw/kospi_db_full_20260320.sql
APP_MODEL_PKL=data/raw/model_package.pkl
APP_AUTO_OUTPUT_DIR=data/incoming/auto
```

설명:

- `KRXDATA_API_KEY`
  - fallback 시장 데이터용
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
  - 뉴스 API용
- `OPEN_DART_API_KEY`
  - 대주주 데이터 보강용
- `ECOS_API_KEY`
  - 매크로 데이터용

## 8. 폴더 구조 점검

최소한 아래 경로가 존재하는지 확인합니다.

```text
assets/
data/raw/
data/incoming/auto/
data/incoming/manual/
data/subscriptions/
logs/
src/
app.py
```

필요하면 빈 폴더를 생성합니다.

```powershell
New-Item -ItemType Directory -Force data\incoming\auto
New-Item -ItemType Directory -Force data\incoming\manual
New-Item -ItemType Directory -Force data\subscriptions
New-Item -ItemType Directory -Force logs
```

## 9. 앱만 먼저 실행하기

가장 안전한 실행 방식은 `.venv`의 파이썬을 직접 쓰는 것입니다.

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

브라우저에서 접속:

```text
http://127.0.0.1:8501
```

중요:

- 시스템 Python으로 실행하면 일부 parser/패키지가 빠져 있을 수 있습니다.
- 특히 우측 실시간 패널의 네이버 크롤링은 `.venv`로 실행해야 정상 동작합니다.

## 10. 미래 반기 예측 결과 만들기

`2026_H1` 같은 미래 반기는 GitHub에 자동 결과가 포함되지 않을 수 있습니다.

따라서 로컬에서 아래 순서로 직접 실행해야 합니다.

### 10-1. 최신 데이터 수집

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
```

### 10-2. 예측 실행

```powershell
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

### 10-3. 한 번에 실행

아래 스크립트로 한 번에 실행할 수도 있습니다.

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\weekly_run.ps1
```

생성 위치:

```text
data/incoming/auto/
```

예시 파일:

- `weekly_predictions_2026_H1.csv`
- `weekly_strong_in_2026_H1.csv`
- `weekly_strong_out_2026_H1.csv`
- `weekly_collection_summary.json`
- `weekly_prediction_summary.json`

## 11. 앱에서 보이는 각 데이터의 기준

### 과거 반기

예:

- `2025_H1`
- `2025_H2`

이 기간은 SQL dump 안의 historical 데이터와 labels를 바탕으로 표시합니다.

### 미래 반기

예:

- `2026_H1`

이 기간은 자동 수집 결과를 바탕으로 계산합니다.

즉 아래 명령을 다시 실행하면 결과가 바뀔 수 있습니다.

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

## 12. 우측 패널 실시간 시세

우측 패널은 다음과 같이 동작합니다.

- 실시간 코스피 지수: Yahoo Finance 기반
- 시총 순위 200 개별 종목 가격/등락률: 네이버 시가총액 페이지 기반

주의:

- 완전 체결 단위 실시간은 아닙니다.
- `준실시간`에 가깝습니다.
- 앱은 `.venv`에서 실행해야 네이버 HTML 파싱이 제대로 동작합니다.

## 13. CSV 다운로드

중앙의 `CSV 다운로드` 버튼은 현재 선택한 반기의 예측 결과를 내려받습니다.

과거 반기에서는 아래 actual 라벨도 함께 포함됩니다.

- `label_in`
- `label_out`

## 14. 자주 발생하는 문제

### 14-1. `.ps1` 실행이 막힐 때

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### 14-2. 앱은 켜지는데 우측 실시간 시세가 안 바뀔 때

원인:

- 시스템 Python으로 실행했을 가능성이 큼

해결:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

### 14-3. 미래 예측이 안 보일 때

원인:

- `data/incoming/auto`에 자동 생성 결과가 없음

해결:

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

### 14-4. `actual KOSPI200` 차트가 안 보일 때

원인:

- `data/incoming/manual/actual_kospi200_2026.csv` 파일이 없음

## 15. 가장 추천하는 실행 순서

처음 세팅하는 사람 기준으로는 아래 순서가 가장 안전합니다.

```powershell
cd C:\Users\Admin\fa08-2nd-numbers
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

그 다음:

1. `.env` 값 입력
2. SQL dump와 actual CSV 배치
3. 미래 예측이 필요하면 수집/예측 실행
4. 앱 실행

앱 실행:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501
```

미래 예측 생성:

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

## 16. 팀원들과 결과를 최대한 비슷하게 맞추는 방법

미래 반기 예측은 실행 시점에 따라 조금 달라질 수 있습니다.

가장 비슷하게 맞추려면:

1. 같은 브랜치 사용
2. 같은 `.env` 사용
3. 같은 날, 비슷한 시각에 아래 실행

```powershell
.\.venv\Scripts\python.exe run_weekly_collection.py
.\.venv\Scripts\python.exe run_weekly_prediction.py
```

또는:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\weekly_run.ps1
```

## 17. 관련 핵심 파일

- `app.py`
  - Streamlit 메인 앱
- `run_weekly_collection.py`
  - 최신 데이터 수집
- `run_weekly_prediction.py`
  - 미래 반기 예측 실행
- `weekly_run.ps1`
  - 수집 + 예측 일괄 실행
- `src/predictor.py`
  - 예측 핵심 로직
- `src/pipeline/feature_builder.py`
  - 피처 생성 로직
- `data/raw/model_package.pkl`
  - 학습된 모델 패키지

