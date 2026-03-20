"""
KOSPI200 데이터 파이프라인
============================
주간 배치 실행: 수집 → 가공 → 추론

Usage:
    # 전체 파이프라인 실행
    python datapipeline.py --period 2025_H2

    # 단계별 실행
    python datapipeline.py --step collect --period 2025_H2
    python datapipeline.py --step process --period 2025_H2
    python datapipeline.py --step predict --period 2025_H2

구조:
    Step 1. 수집 (collect)  → KRX/ECOS에서 데이터 수집 → DB 저장
    Step 2. 가공 (process)  → DB 원천 데이터 → feature_krx 형태 변환
    Step 3. 추론 (predict)  → final.pkl 로드 → 스코어링 → DB/CSV 저장
"""

import pandas as pd
import numpy as np
import mysql.connector
import datetime
import os


# ===================================================================
#  설정
# ===================================================================
DB_CFG = {'host': 'localhost', 'user': 'root',
          'password': '1234', 'database': 'kospi_db'}

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
PKL_PATH = os.path.join(PROJECT_DIR, 'final.pkl')
MACRO_CSV_PATH = os.path.join(PROJECT_DIR, 'macro.csv')


# ===================================================================
#  Step 1. 수집 (Collect)
# ===================================================================
def collect_data(period, db_cfg=None):
    """
    KRX / ECOS 등에서 데이터 수집 → kospi_db에 저장

    현재는 수집 API가 확정되지 않아서 스켈레톤만 제공.
    실제 구현 시 pykrx, ECOS API 등을 사용.

    Parameters
    ----------
    period : str
        수집 대상 period (예: '2025_H2')
    db_cfg : dict
        DB 접속 정보
    """
    if db_cfg is None:
        db_cfg = DB_CFG

    print(f'\n{"="*70}')
    print(f'  Step 1. 데이터 수집 — {period}')
    print(f'{"="*70}')

    # -------------------------------------------------------
    # 1-1. KRX 시총 TOP300 + 거래량 + 주가
    # -------------------------------------------------------
    # TODO: pykrx 또는 KRX API로 수집
    #
    # from pykrx import stock
    #
    # period_dates = get_period_dates(period)  # 시작~종료 사이 금요일 목록
    # for friday in period_dates:
    #     # 시가총액 TOP300
    #     mktcap = stock.get_market_cap(friday)
    #     # 거래량/거래대금
    #     trading = stock.get_market_trading_value(friday)
    #     # DB INSERT → kospi_friday_daily 테이블
    #     insert_to_db(mktcap, trading, friday, db_cfg)
    #
    print('  [1-1] KRX 시총/거래량 수집 — TODO (pykrx 연동 필요)')

    # -------------------------------------------------------
    # 1-2. 외국인보유 현황
    # -------------------------------------------------------
    # TODO: KRX 외국인보유 데이터 수집
    #
    # for month_ym in period_months:
    #     foreign_data = fetch_foreign_holding(month_ym)
    #     insert_foreign_holding(foreign_data, db_cfg)
    #
    print('  [1-2] 외국인보유 수집 — TODO')

    # -------------------------------------------------------
    # 1-3. 대주주/유동비율
    # -------------------------------------------------------
    # TODO: DART API 또는 KRX 공시에서 수집
    #
    # major_data = fetch_major_holder(period)
    # insert_major_holder(major_data, db_cfg)
    #
    print('  [1-3] 대주주/유동비율 수집 — TODO')

    # -------------------------------------------------------
    # 1-4. 매크로 지표
    # -------------------------------------------------------
    # TODO: ECOS API (한국은행 경제통계시스템)
    #
    # macro_data = fetch_macro_indicators(period)
    # append_to_macro_csv(macro_data, MACRO_CSV_PATH)
    #
    print('  [1-4] 매크로 지표 수집 — TODO (ECOS API)')

    # -------------------------------------------------------
    # 1-5. 관리종목/투자경고 플래그
    # -------------------------------------------------------
    # TODO: KRX 공시에서 수집
    #
    # flag_data = fetch_filter_flags(period)
    # insert_filter_flags(flag_data, db_cfg)
    #
    print('  [1-5] 관리종목/투자경고 수집 — TODO')

    print('\n  ⚠ 수집 단계는 API 연동 후 구현 필요')
    print('  현재는 DB에 이미 있는 데이터를 사용합니다')


# ===================================================================
#  Step 2. 가공 (Process)
# ===================================================================
def process_data(period, db_cfg=None):
    """
    DB 원천 데이터 → 모델 입력용 DataFrame 변환
    (노트북 셀 1~7의 로직을 함수화)

    Parameters
    ----------
    period : str
        가공 대상 period
    db_cfg : dict
        DB 접속 정보

    Returns
    -------
    DataFrame : 모델 입력 가능한 피쳐 DataFrame
    """
    if db_cfg is None:
        db_cfg = DB_CFG

    print(f'\n{"="*70}')
    print(f'  Step 2. 데이터 가공 — {period}')
    print(f'{"="*70}')

    conn = mysql.connector.connect(**db_cfg)

    # --- 테이블 로드 ---
    feature_krx = pd.read_sql(
        f"SELECT * FROM feature_krx WHERE period = '{period}'", conn)
    labels = pd.read_sql('SELECT * FROM labels', conn)
    major = pd.read_sql(
        f"SELECT * FROM major_holder WHERE period = '{period}'", conn)
    foreign = pd.read_sql('SELECT * FROM foreign_holding', conn)
    flag = pd.read_sql('SELECT * FROM filter_flag', conn)
    meta = pd.read_sql('SELECT * FROM stock_meta', conn)
    period_tbl = pd.read_sql('SELECT * FROM period', conn)
    sector_map = pd.read_sql('SELECT * FROM sector_map', conn)

    company_map = pd.read_sql(
        "SELECT ticker, company FROM kospi_friday_daily "
        "WHERE date = (SELECT MAX(date) FROM kospi_friday_daily) "
        "GROUP BY ticker, company", conn)
    conn.close()

    print(f'  feature_krx: {len(feature_krx)}종목')

    # --- 외국인보유 period별 집계 ---
    def date_to_ym(d):
        return d.year * 100 + d.month

    period_tbl['ym_start'] = period_tbl['period_start'].apply(date_to_ym)
    period_tbl['ym_end'] = period_tbl['period_end'].apply(date_to_ym)

    def ym_in_range(ym, start, end):
        sy, sm = start // 100, start % 100
        ey, em = end // 100, end % 100
        y, m = ym // 100, ym % 100
        return (sy * 12 + sm) <= (y * 12 + m) <= (ey * 12 + em)

    pr_row = period_tbl[period_tbl['period'] == period].iloc[0]
    mask = foreign['ym'].apply(
        lambda x: ym_in_range(x, pr_row['ym_start'], pr_row['ym_end']))
    fh_sub = foreign[mask]
    fh_agg = fh_sub.groupby('ticker').agg(
        avg_foreign_ratio=('foreign_holding_ratio', 'mean'),
        last_foreign_ratio=('foreign_holding_ratio', 'last'),
        avg_exhaustion_rate=('foreign_limit_exhaustion_rate', 'mean'),
    ).reset_index()
    fh_agg['period'] = period

    # --- 매크로 집계 ---
    macro = pd.read_csv(MACRO_CSV_PATH)
    macro_cols = ['base_rate', 'usd_krw', 'cpi', 'industrial', 'export',
                  'import_', 'bond_3y', 'cli', 'bsi', 'current_acct', 'capex', 'm2']

    macro_mask = macro['ym'].apply(
        lambda x: ym_in_range(x, pr_row['ym_start'], pr_row['ym_end']))
    macro_sub = macro[macro_mask]
    macro_row = {'period': period}
    if len(macro_sub) > 0:
        for col in macro_cols:
            macro_row[f'macro_{col}_mean'] = macro_sub[col].mean()
            macro_row[f'macro_{col}_last'] = macro_sub[col].iloc[-1]
    macro_agg = pd.DataFrame([macro_row])

    # --- 피쳐 조인 ---
    df = feature_krx.copy()
    df = df.merge(labels[['period', 'ticker', 'was_member', 'label_in',
                           'label_out', 'actual_rank']],
                  on=['period', 'ticker'], how='left')
    df = df.merge(major[['period', 'ticker', 'major_holder_ratio',
                          'treasury_ratio', 'non_float_ratio', 'float_rate']],
                  on=['period', 'ticker'], how='left')
    df = df.merge(fh_agg, on=['period', 'ticker'], how='left')
    df = df.merge(macro_agg, on='period', how='left')
    df = df.merge(meta[['ticker', 'is_not_common', 'is_reits',
                         'list_date', 'ksic_sector']],
                  on='ticker', how='left')

    if not flag.empty:
        flag_sub = flag[['ticker', 'flag_date', 'is_managed', 'is_warning']].copy()
        flag_sub.rename(columns={'flag_date': 'period'}, inplace=True)
        flag_sub = flag_sub[flag_sub['period'] == period]
        df = df.merge(flag_sub, on=['period', 'ticker'], how='left')
        df['is_managed'] = df['is_managed'].fillna(0).astype(int)
        df['is_warning'] = df['is_warning'].fillna(0).astype(int)

    # NaN 처리
    df['was_member'] = df['was_member'].fillna(0).astype(int)
    df['label_in'] = df['label_in'].fillna(0).astype(int)
    df['label_out'] = df['label_out'].fillna(0).astype(int)

    # --- 섹터 인코딩 ---
    sector_dict_gics = dict(zip(sector_map['ksic_sector'],
                                 sector_map['gics_sector_2023']))
    sector_dict_krx = dict(zip(sector_map['ksic_sector'],
                                sector_map['krx_group']))
    df['gics_sector'] = df.get('gics_sector',
                                df['ksic_sector'].map(sector_dict_gics))
    df['krx_group'] = df.get('krx_group',
                              df['ksic_sector'].map(sector_dict_krx))

    # ticker_to_name
    ticker_to_name = dict(zip(company_map['ticker'], company_map['company']))

    print(f'  가공 완료: {df.shape}')
    print(f'  매크로 피쳐: {len([c for c in df.columns if c.startswith("macro_")])}개')

    return df, ticker_to_name


# ===================================================================
#  Step 3. 추론 (Predict)
# ===================================================================
def run_prediction(period, db_cfg=None, save_db=True, save_csv=True):
    """
    가공된 데이터 → 모델 로드 → 예측 → DB/CSV 저장

    Parameters
    ----------
    period : str
        예측 대상 period
    db_cfg : dict
        DB 접속 정보
    save_db : bool
        predictions 테이블에 저장할지 여부
    save_csv : bool
        CSV 파일로 저장할지 여부

    Returns
    -------
    dict : predict() 결과
    """
    if db_cfg is None:
        db_cfg = DB_CFG

    print(f'\n{"="*70}')
    print(f'  Step 3. 추론 — {period}')
    print(f'{"="*70}')

    from inference import load_model, predict, save_to_db, export_csv

    # 모델 로드
    pkg = load_model(PKL_PATH)

    # 데이터 가공
    df, ticker_to_name_new = process_data(period, db_cfg)

    # ticker_to_name 업데이트 (신규 종목 반영)
    pkg['ticker_to_name'].update(ticker_to_name_new)

    # 예측
    result = predict(df, pkg, period=period)

    # 저장
    if save_csv:
        csv_path = os.path.join(PROJECT_DIR,
                                f'kospi200_prediction_{period}.csv')
        export_csv(result, csv_path)

    if save_db:
        save_to_db(result, db_cfg)

    return result


# ===================================================================
#  전체 파이프라인
# ===================================================================
def run_pipeline(period, db_cfg=None):
    """
    전체 파이프라인 실행: 수집 → 가공 → 추론

    Parameters
    ----------
    period : str
        대상 period (예: '2025_H2')
    """
    if db_cfg is None:
        db_cfg = DB_CFG

    print(f'\n{"#"*70}')
    print(f'  KOSPI200 데이터 파이프라인 — {period}')
    print(f'  실행 시각: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'{"#"*70}')

    # Step 1. 수집
    collect_data(period, db_cfg)

    # Step 2. 가공 + Step 3. 추론
    result = run_prediction(period, db_cfg)

    print(f'\n{"#"*70}')
    print(f'  파이프라인 완료')
    print(f'  강력편입: {result["summary"]["strong_in_count"]}종목')
    print(f'  강력편출: {result["summary"]["strong_out_count"]}종목')
    print(f'{"#"*70}')

    return result


# ===================================================================
#  유틸: Period 정보
# ===================================================================
def get_current_period():
    """현재 날짜 기준으로 해당 period 반환"""
    today = datetime.date.today()
    year = today.year
    # H1: 1~6월(5월 결정), H2: 7~12월(11월 결정)
    half = 'H1' if today.month <= 6 else 'H2'
    return f'{year}_{half}'


def get_next_period():
    """다음 예측 대상 period 반환"""
    today = datetime.date.today()
    year = today.year
    if today.month <= 6:
        return f'{year}_H2'
    else:
        return f'{year + 1}_H1'


# ===================================================================
#  메인 실행
# ===================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='KOSPI200 데이터 파이프라인')
    parser.add_argument('--period', default=None,
                        help='대상 period (예: 2025_H2). 미지정 시 자동 결정')
    parser.add_argument('--step', default='all',
                        choices=['all', 'collect', 'process', 'predict'],
                        help='실행할 단계')
    parser.add_argument('--no-db', action='store_true',
                        help='DB 저장 안 함')
    parser.add_argument('--no-csv', action='store_true',
                        help='CSV 저장 안 함')
    args = parser.parse_args()

    period = args.period or get_current_period()
    print(f'대상 period: {period}')

    if args.step == 'all':
        run_pipeline(period)
    elif args.step == 'collect':
        collect_data(period)
    elif args.step == 'process':
        df, _ = process_data(period)
        print(f'가공 결과: {df.shape}')
    elif args.step == 'predict':
        run_prediction(period,
                       save_db=not args.no_db,
                       save_csv=not args.no_csv)
