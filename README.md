# KOSPI200 Streamlit App

이 프로젝트는 사용자가 직접 데이터를 입력하지 않고, 제공받은 실제 파일만으로 `KOSPI 200 편입·편출 예측` 대시보드를 띄우도록 구성했습니다.

## 포함된 실제 데이터

- `data/raw/kospi_db_full_20260320.sql`
- `data/raw/kospi_db_schema.sql`
- `data/raw/macro.csv`
- `data/raw/model_package.pkl`

## 현재 구현 범위

- MySQL 없이 `SQL dump`를 직접 읽어 필요한 테이블을 메모리에서 복원합니다.
- `final.pkl` 기반 예측을 반기별로 실행합니다.
- 가장 최근 실제 제공 반기부터 선택 가능합니다.
- `feature_krx`가 없는 최신 반기라도 `kospi_friday_daily` 원천 데이터가 있으면 앱에서 동적으로 반기 피처를 구성해 선택 가능하게 시도합니다.
- 제공 데이터 범위 안에서는 샘플 데이터 없이 동작합니다.

## 실행 순서

1. 표준 Python 3.11 또는 3.12 설치
2. 프로젝트 루트에서 가상환경 생성
3. 의존성 설치
4. `.env.example`을 `.env`로 복사
5. API 키 입력
6. Streamlit 실행

## Windows PowerShell 명령어

```powershell
cd C:\Users\Admin\numbers
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
python inspect_data.py
streamlit run app.py
```

주의:

- 현재 PC에서는 일반 Python이 PATH에 잡혀 있지 않았습니다.
- MySQL Workbench 내장 Python은 프로젝트 가상환경 용도로 정상 동작하지 않았습니다.
- 따라서 먼저 표준 Python 설치를 확인한 뒤 위 순서로 진행하는 것이 안전합니다.

## API 키 입력 위치

루트의 `.env` 파일에 입력합니다.

```env
KRXDATA_API_KEY=여기에_KRXDATA_API_키
OPEN_DART_API_KEY=여기에_다트_API_키
ECOS_API_KEY=여기에_ECOS_API_키
```

용도는 다음과 같습니다.

- `KRXDATA_API_KEY`: 시가총액, 거래대금, 시장 순위, 상장 정보 등 미래 반기 예측용 시장 데이터 갱신
- `OPEN_DART_API_KEY`: 대주주지분, 자사주, 유동비율 계산 보강
- `ECOS_API_KEY`: 매크로 지표 갱신

현재 상태 기준:

- 대시보드 실행만 할 때: API 키 없이 가능
- `2026_H2`, `2027_H1`처럼 미래 반기를 계속 갱신하려 할 때: 세 API 키를 준비하는 것을 권장

## 미래 반기 운영 준비

미래 반기 수집 파이프라인 진입 파일은 `pipeline.py`입니다.

```powershell
cd C:\Users\Admin\numbers
python pipeline.py --period 2026_H2
```

현재 `pipeline.py`와 `src/collector.py`는 운영 골격까지 넣어 두었습니다.
다만 실제 자동 수집 엔드포인트 구현을 끝내려면 아래 중 하나가 추가로 필요합니다.

- KRXDATA API 문서
- 실제 응답 예시 JSON
- 이미 사용 중인 사내 호출 코드

## 없는 실데이터

현재 제공 파일 기준으로는 `2026_H1`까지 예측 가능한 구조로 보입니다.
아래 반기를 실제로 예측하려면 같은 스키마의 실데이터를 추가로 받아야 합니다.

- `2026_H2`
- `2027_H1`
- `2027_H2`

필요 데이터는 다음과 같습니다.

- `feature_krx`
- `major_holder`
- `foreign_holding`
- `filter_flag`
- `stock_meta` 업데이트분
- `macro.csv` 업데이트분

## 수동 SQL 운영

수동 SQL 반영 기준과 최신 반기 점검 절차는 [MANUAL_UPDATE_GUIDE.md](/C:/Users/Admin/Documents/Playground/numbers_staging/MANUAL_UPDATE_GUIDE.md)에 정리했습니다.

주간 실행용 진입 스크립트는 [weekly_run.ps1](/C:/Users/Admin/Documents/Playground/numbers_staging/weekly_run.ps1)입니다.
