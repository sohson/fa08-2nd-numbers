# Manual SQL Update Guide

이 프로젝트는 예측 실행 시 `feature_krx.period`의 최신 반기를 자동으로 기본 선택합니다.

## 최신 반기 판별 기준

- 기준 테이블: `feature_krx`
- 최신값 예시:
  - `2025_H2`
  - `2026_H1`
  - `2026_H2`
- 정렬 규칙:
  - 연도 우선
  - 같은 연도에서는 `H2`가 `H1`보다 최신

예외:

- `feature_krx`가 비어 있어도, 해당 반기의 `period` 정의와 `kospi_friday_daily` 실제 일별 데이터가 존재하면 앱이 임시 반기 피처를 동적으로 구성해서 선택 가능하게 시도합니다.
- 따라서 진행 중 반기인 `2026_H1`도 일별 시장 데이터가 충분히 들어 있으면 드롭다운에 나타날 수 있습니다.

## 새 반기 추가 시 반드시 확인할 테이블

### 1. `period`

반기 기간을 먼저 추가합니다.

예:

```sql
INSERT INTO period (period, period_start, period_end)
VALUES ('2026_H2', '2026-05-01', '2026-10-31');
```

### 2. `feature_krx`

예측 대상의 핵심 테이블입니다. 이 테이블의 최신 `period`가 앱의 기본 예측 반기가 됩니다.

필수 컬럼:

- `period`
- `ticker`
- `avg_mktcap`
- `float_ratio`
- `gics_sector`
- `krx_group`
- `period_rank`
- `turnover_ratio`

### 3. `major_holder`

같은 반기의 유동비율 관련 데이터가 필요합니다.

필수 컬럼:

- `period`
- `ticker`
- `major_holder_ratio`
- `treasury_ratio`
- `non_float_ratio`
- `float_rate`

### 4. `filter_flag`

같은 반기의 관리종목/경고 플래그를 반영합니다.

필수 컬럼:

- `ticker`
- `is_managed`
- `is_warning`
- `flag_date`

### 5. `foreign_holding`

해당 반기 기간에 속하는 월별 데이터를 넣습니다.

예:

- `2026_H2`를 예측할 경우 `202605`~`202610`

필수 컬럼:

- `ym`
- `ticker`
- `foreign_holding_qty`
- `foreign_holding_ratio`
- `foreign_limit_qty`
- `foreign_limit_exhaustion_rate`

### 6. `stock_meta`

신규 상장, 리츠, 보통주 여부, 상장일을 최신 상태로 유지합니다.

### 7. `sector_map`

업종/섹터 체계가 바뀌면 최신 상태로 유지합니다.

## 수동 갱신 후 점검 순서

```powershell
cd C:\Users\Admin\numbers
python inspect_data.py
```

이 스크립트는 아래를 확인합니다.

- 최신 `feature_krx.period`
- 최신 반기의 `feature_krx` 행 수
- 최신 반기의 `major_holder` 행 수
- 최신 반기의 `filter_flag` 행 수
- 최신 반기의 `period` 등록 여부
- 최신 `foreign_holding.ym`

## 앱 실행 순서

```powershell
cd C:\Users\Admin\numbers
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python inspect_data.py
streamlit run app.py
```

## 권장 운영 순서

1. 자동 수집 가능한 시장 데이터 갱신
2. 수동 데이터 SQL 반영
3. `python inspect_data.py`로 최신 반기 점검
4. `streamlit run app.py` 실행
5. 최신 반기를 기본 선택한 상태에서 예측 확인
