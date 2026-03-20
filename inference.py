"""
KOSPI200 편입/편출 예측 — 추론 모듈
====================================
팀원은 app.py에서 이렇게 사용:

    from inference import load_model, predict, export_csv, save_to_db

    model = load_model('final.pkl')
    results = predict(df, model, period='2025_H2', prev_members={'005930','000660',...})
    export_csv(results, 'output.csv')
    save_to_db(results, model['db_cfg'])
"""

import pickle
import datetime
import numpy as np
import pandas as pd


# ===================================================================
#  1. 모델 로드
# ===================================================================
def load_model(pkl_path='final.pkl'):
    """pkl 파일에서 모델 패키지 로드"""
    with open(pkl_path, 'rb') as f:
        pkg = pickle.load(f)
    print(f"[inference] 모델 로드 완료: {pkg['method']} - {pkg['model_name']}")
    print(f"  피쳐: {len(pkg['features'])}개 | 버전: {pkg['model_version']} | 생성: {pkg['created_at']}")
    return pkg


# ===================================================================
#  2. 피쳐 엔지니어링 (노트북 셀 5,7,28 로직 재현)
# ===================================================================
def prepare_features(df_raw, pkg, period, prev_members=None):
    """
    원천 데이터 → 모델 입력용 피쳐 DataFrame 변환

    Parameters
    ----------
    df_raw : DataFrame
        feature_krx + major_holder + foreign_holding + filter_flag + stock_meta + macro
        가 조인된 원천 데이터 (datapipeline.py가 생성)
    pkg : dict
        load_model()로 불러온 모델 패키지
    period : str
        예측 대상 period (예: '2025_H2')
    prev_members : set or None
        전기 KOSPI200 멤버 ticker set. None이면 pkg['actual_members']에서 자동 탐색

    Returns
    -------
    DataFrame : 모델 입력 가능한 피쳐 + ticker/period 포함
    """
    df = df_raw.copy()

    # --- 전기 멤버 정보 ---
    if prev_members is None:
        period_order = pkg['period_order']
        idx = period_order.index(period)
        prev_period = period_order[idx - 1] if idx > 0 else None
        if prev_period and prev_period in pkg['actual_members']:
            prev_members = set(pkg['actual_members'][prev_period])
        else:
            prev_members = set()

    df['prev_was_member'] = df['ticker'].apply(lambda t: 1 if t in prev_members else 0)

    # --- 섹터 인코딩 ---
    le_gics = pkg['le_gics']
    le_krx = pkg['le_krx']
    sector_dict_gics = pkg['sector_dict_gics']
    sector_dict_krx = pkg['sector_dict_krx']

    if 'gics_sector' not in df.columns and 'ksic_sector' in df.columns:
        df['gics_sector'] = df['ksic_sector'].map(sector_dict_gics)
        df['krx_group'] = df['ksic_sector'].map(sector_dict_krx)

    # 학습 때 본 클래스만 인코딩, 새 클래스는 '기타'로
    gics_classes = set(le_gics.classes_)
    krx_classes = set(le_krx.classes_)
    df['gics_sector_clean'] = df['gics_sector'].fillna('기타').astype(str).apply(
        lambda x: x if x in gics_classes else '기타')
    df['krx_group_clean'] = df['krx_group'].fillna('기타').astype(str).apply(
        lambda x: x if x in krx_classes else '기타')
    df['gics_sector_enc'] = le_gics.transform(df['gics_sector_clean'])
    df['krx_group_enc'] = le_krx.transform(df['krx_group_clean'])

    # --- 전기 대비 변화량 피쳐 ---
    # 단일 period 추론 시에는 전기 데이터가 없으므로 0으로 처리
    for col in ['prev_rank', 'rank_change', 'mktcap_change', 'foreign_change', 'turnover_change']:
        if col not in df.columns:
            df[col] = 0

    # --- 확장 피쳐 (FEATURES_ENHANCED에 포함된 경우) ---
    if 'float_mktcap' not in df.columns and 'avg_mktcap' in df.columns:
        df['float_mktcap'] = df['avg_mktcap'] * df['float_rate'].fillna(0)

    if 'float_mktcap_rank' not in df.columns and 'float_mktcap' in df.columns:
        df['float_mktcap_rank'] = df['float_mktcap'].rank(ascending=False, method='first').astype(int)

    if 'dist_from_200' not in df.columns and 'period_rank' in df.columns:
        df['dist_from_200'] = df['period_rank'] - 200

    if 'float_dist_from_200' not in df.columns and 'float_mktcap_rank' in df.columns:
        df['float_dist_from_200'] = df['float_mktcap_rank'] - 200

    if 'consecutive_member' not in df.columns:
        df['consecutive_member'] = 0  # 단일 period 추론 시 이력 없음

    if 'sector_rank' not in df.columns and 'gics_sector_enc' in df.columns:
        df['sector_rank'] = df.groupby('gics_sector_enc')['period_rank'].rank(
            method='first').astype(int)
        sector_count = df.groupby('gics_sector_enc')['ticker'].transform('count')
        df['sector_relative_rank'] = df['sector_rank'] / sector_count

    if 'rank_acceleration' not in df.columns:
        df['rank_acceleration'] = 0

    if 'sector_member_score' not in df.columns:
        sector_in_map = pkg.get('sector_in_map', {})
        df['sector_member_score'] = df['gics_sector_enc'].map(sector_in_map).fillna(0.5)

    if 'foreign_acceleration' not in df.columns:
        df['foreign_acceleration'] = 0

    return df


# ===================================================================
#  3. 필터링 (부적격 종목 제거)
# ===================================================================
def apply_filters(df, period_end_date=None):
    """
    TOP300 → 필터링: 비보통주, 유동비율<10%, 리츠, 신규상장<6개월 제외

    Parameters
    ----------
    df : DataFrame
    period_end_date : str or datetime, optional
        period 종료일 (신규상장 판별용). None이면 오늘 날짜

    Returns
    -------
    DataFrame : 필터링된 데이터
    """
    if period_end_date is None:
        period_end_date = pd.Timestamp.now()
    else:
        period_end_date = pd.to_datetime(period_end_date)

    before = len(df)
    mask1 = df.get('is_not_common', pd.Series(0, index=df.index)) == 1
    mask2 = (df['float_rate'] < 0.10) & df['float_rate'].notna()
    mask3 = df.get('is_reits', pd.Series(0, index=df.index)) == 1

    mask4 = pd.Series(False, index=df.index)
    if 'list_date' in df.columns:
        list_dt = pd.to_datetime(df['list_date'])
        months = (period_end_date.year - list_dt.dt.year) * 12 + \
                 (period_end_date.month - list_dt.dt.month)
        mask4 = (months < 6) & months.notna()

    exclude = mask1 | mask2 | mask3 | mask4
    filtered = df[~exclude].copy()
    print(f"[inference] 필터링: {before} → {len(filtered)} "
          f"(비보통주={mask1.sum()}, 유동<10%={mask2.sum()}, "
          f"리츠={mask3.sum()}, 신규<6개월={mask4.sum()})")
    return filtered


# ===================================================================
#  4. 예측 (스코어링 → TOP200 → 강력 편입/편출)
# ===================================================================
def predict(df, pkg, period=None, prev_members=None, period_end_date=None):
    """
    메인 예측 함수: DataFrame → 스코어링 → TOP200 → 강력 편입/편출

    Parameters
    ----------
    df : DataFrame
        원천 데이터 (feature_krx 형태) 또는 prepare_features 결과
    pkg : dict
        load_model()로 불러온 모델 패키지
    period : str, optional
        예측 대상 period. None이면 데이터에서 자동 탐색
    prev_members : set, optional
        전기 KOSPI200 멤버 ticker set
    period_end_date : str, optional
        period 종료일 (필터링용)

    Returns
    -------
    dict : {
        'period': str,
        'scored': DataFrame (전체 종목 스코어링),
        'top200': set (예측 TOP200 tickers),
        'strong_in': DataFrame (강력 편입 종목),
        'strong_out': DataFrame (강력 편출 종목),
        'summary': dict (요약 통계),
    }
    """
    model = pkg['model']
    features = pkg['features']
    ticker_to_name = pkg['ticker_to_name']

    # period 결정
    if period is None:
        if 'period' in df.columns:
            period = df['period'].iloc[0]
        else:
            raise ValueError("period를 지정하거나 df에 period 컬럼이 있어야 합니다")

    # 피쳐 엔지니어링
    df_feat = prepare_features(df, pkg, period, prev_members)

    # 필터링
    df_feat = apply_filters(df_feat, period_end_date)

    # 누락 피쳐 체크 + 0으로 채움
    missing = [f for f in features if f not in df_feat.columns]
    if missing:
        print(f"[inference] ⚠ 누락 피쳐 {len(missing)}개 → 0으로 채움: {missing}")
        for f in missing:
            df_feat[f] = 0

    # 스코어링
    X = df_feat[features].fillna(0)
    scores = model.predict_proba(X)[:, 1]
    df_feat['score'] = scores
    df_feat['pred_rank'] = df_feat['score'].rank(ascending=False, method='first').astype(int)
    df_feat['company'] = df_feat['ticker'].map(ticker_to_name).fillna('')

    # TOP200 선정
    top200_tickers = set(df_feat.nlargest(200, 'score')['ticker'].values)
    df_feat['pred_top200'] = df_feat['ticker'].apply(lambda t: 1 if t in top200_tickers else 0)

    # 전기 멤버 정보
    if prev_members is None:
        period_order = pkg['period_order']
        idx = period_order.index(period) if period in period_order else -1
        prev_period = period_order[idx - 1] if idx > 0 else None
        if prev_period and prev_period in pkg['actual_members']:
            prev_members = set(pkg['actual_members'][prev_period])
        else:
            prev_members = set()

    prev_mem = df_feat['prev_was_member'].fillna(0)

    # 강력 편입: 전기 비멤버인데 TOP200 진입
    strong_in_mask = (prev_mem == 0) & (df_feat['ticker'].isin(top200_tickers))
    strong_in = df_feat[strong_in_mask].sort_values('score', ascending=False).copy()

    # 강력 편출: 전기 멤버인데 TOP200 탈락
    strong_out_mask = (prev_mem == 1) & (~df_feat['ticker'].isin(top200_tickers))
    strong_out = df_feat[strong_out_mask].sort_values('score', ascending=True).copy()

    # 플래그 추가
    df_feat['strong_in'] = strong_in_mask.astype(int).values
    df_feat['strong_out'] = strong_out_mask.astype(int).values

    # 시총순위로 정렬 (대시보드 표시용)
    if 'period_rank' in df_feat.columns:
        df_feat = df_feat.sort_values('period_rank').reset_index(drop=True)

    # 결과 dict
    result = {
        'period': period,
        'scored': df_feat,
        'top200': top200_tickers,
        'strong_in': strong_in,
        'strong_out': strong_out,
        'prev_members': prev_members,
        'summary': {
            'total_stocks': len(df_feat),
            'top200_count': len(top200_tickers),
            'strong_in_count': len(strong_in),
            'strong_out_count': len(strong_out),
            'model': f"{pkg['method']} - {pkg['model_name']}",
            'model_version': pkg['model_version'],
            'run_date': datetime.datetime.now().strftime('%Y-%m-%d'),
        },
    }

    print(f"\n[inference] 예측 완료: {period}")
    print(f"  종목수: {len(df_feat)} → TOP200: {len(top200_tickers)}")
    print(f"  강력편입: {len(strong_in)}종목 | 강력편출: {len(strong_out)}종목")

    return result


# ===================================================================
#  5. 실제 결과와 비교 (검증용)
# ===================================================================
def compare_actual(result, actual_members_current):
    """
    예측 결과를 실제 KOSPI200 멤버와 비교

    Parameters
    ----------
    result : dict
        predict() 반환값
    actual_members_current : set
        당기 실제 KOSPI200 멤버 ticker set

    Returns
    -------
    dict : 비교 결과 (precision, recall 등)
    """
    pred_top200 = result['top200']
    prev_members = result['prev_members']

    actual_in = actual_members_current - prev_members
    actual_out = prev_members - actual_members_current

    pred_in = pred_top200 - prev_members
    pred_out = prev_members - pred_top200

    in_hit = pred_in & actual_in
    out_hit = pred_out & actual_out

    in_prec = len(in_hit) / len(pred_in) if len(pred_in) > 0 else 0
    in_rec = len(in_hit) / len(actual_in) if len(actual_in) > 0 else 0
    out_prec = len(out_hit) / len(pred_out) if len(pred_out) > 0 else 0
    out_rec = len(out_hit) / len(actual_out) if len(actual_out) > 0 else 0
    total = (in_prec + in_rec + out_prec + out_rec) / 4
    overlap = pred_top200 & actual_members_current

    comparison = {
        'top200_accuracy': len(overlap) / 200,
        'in_precision': in_prec,
        'in_recall': in_rec,
        'in_hit': len(in_hit),
        'in_pred': len(pred_in),
        'in_actual': len(actual_in),
        'out_precision': out_prec,
        'out_recall': out_rec,
        'out_hit': len(out_hit),
        'out_pred': len(pred_out),
        'out_actual': len(actual_out),
        'total_score': total,
        'missed_in': actual_in - pred_in,
        'missed_out': actual_out - pred_out,
    }

    print(f"\n[검증] {result['period']} 예측 정확도:")
    print(f"  TOP200 일치율: {len(overlap)}/200 = {len(overlap)/200:.1%}")
    print(f"  편입 Precision: {in_prec:.0%} ({len(in_hit)}/{len(pred_in)})")
    print(f"  편입 Recall:    {in_rec:.0%} ({len(in_hit)}/{len(actual_in)})")
    print(f"  편출 Precision: {out_prec:.0%} ({len(out_hit)}/{len(pred_out)})")
    print(f"  편출 Recall:    {out_rec:.0%} ({len(out_hit)}/{len(actual_out)})")
    print(f"  종합점수:       {total:.1%}")

    return comparison


# ===================================================================
#  6. CSV 저장
# ===================================================================
def export_csv(result, path=None):
    """
    예측 결과를 CSV로 저장 (대시보드 다운로드용)

    Parameters
    ----------
    result : dict
        predict() 반환값
    path : str, optional
        저장 경로. None이면 kospi200_prediction_{period}.csv
    """
    if path is None:
        path = f"kospi200_prediction_{result['period']}.csv"

    scored = result['scored']

    # 대시보드용 컬럼만 추출
    export_cols = ['ticker', 'company', 'score', 'pred_rank', 'pred_top200',
                   'strong_in', 'strong_out', 'prev_was_member']
    if 'period_rank' in scored.columns:
        export_cols.insert(3, 'period_rank')

    csv_df = scored[[c for c in export_cols if c in scored.columns]].copy()
    csv_df = csv_df.sort_values('pred_rank').reset_index(drop=True)
    csv_df['score'] = csv_df['score'].round(6)

    csv_df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"[inference] CSV 저장: {path} ({len(csv_df)}종목)")
    return path


# ===================================================================
#  7. DB 저장 (predictions 테이블)
# ===================================================================
def save_to_db(result, db_cfg=None):
    """
    예측 결과를 predictions 테이블에 저장

    Parameters
    ----------
    result : dict
        predict() 반환값
    db_cfg : dict, optional
        DB 접속 정보. None이면 기본값 사용
    """
    import mysql.connector

    if db_cfg is None:
        db_cfg = {'host': 'localhost', 'user': 'root',
                  'password': '1234', 'database': 'kospi_db'}

    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()

    # predictions 테이블 생성 (없으면)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_date DATE NOT NULL,
            period VARCHAR(20) NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            company VARCHAR(100),
            score DOUBLE,
            pred_rank INT,
            period_rank INT,
            pred_top200 TINYINT DEFAULT 0,
            strong_in TINYINT DEFAULT 0,
            strong_out TINYINT DEFAULT 0,
            prev_member TINYINT DEFAULT 0,
            model_version VARCHAR(20),
            UNIQUE KEY uq_pred (run_date, period, ticker)
        )
    """)

    scored = result['scored']
    summary = result['summary']
    run_date = summary['run_date']
    period = result['period']
    model_version = summary['model_version']

    # 기존 같은 run_date + period 데이터 삭제 (재실행 대비)
    cursor.execute(
        "DELETE FROM predictions WHERE run_date = %s AND period = %s",
        (run_date, period)
    )

    # INSERT
    insert_sql = """
        INSERT INTO predictions
        (run_date, period, ticker, company, score, pred_rank, period_rank,
         pred_top200, strong_in, strong_out, prev_member, model_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for _, row in scored.iterrows():
        rows.append((
            run_date, period,
            row['ticker'],
            row.get('company', ''),
            round(float(row['score']), 6),
            int(row['pred_rank']),
            int(row['period_rank']) if 'period_rank' in row and pd.notna(row.get('period_rank')) else None,
            int(row.get('pred_top200', 0)),
            int(row.get('strong_in', 0)),
            int(row.get('strong_out', 0)),
            int(row.get('prev_was_member', 0)),
            model_version,
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f"[inference] DB 저장 완료: predictions 테이블 ({len(rows)}건, "
          f"run_date={run_date}, period={period})")

    cursor.close()
    conn.close()


# ===================================================================
#  메인 실행 (단독 실행 시 테스트)
# ===================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='KOSPI200 편입/편출 추론')
    parser.add_argument('--pkl', default='final.pkl', help='모델 pkl 경로')
    parser.add_argument('--period', default=None, help='예측 대상 period (예: 2025_H2)')
    parser.add_argument('--csv', default=None, help='CSV 저장 경로')
    parser.add_argument('--db', action='store_true', help='DB에 저장')
    args = parser.parse_args()

    # 모델 로드
    pkg = load_model(args.pkl)

    # DB에서 원천 데이터 로드 (datapipeline이 갱신해둔 데이터 사용)
    import mysql.connector
    db_cfg = pkg['db_cfg']
    conn = mysql.connector.connect(**db_cfg)

    period = args.period
    if period is None:
        period = pkg['period_order'][-1]
        print(f"[inference] period 미지정 → 마지막 period 사용: {period}")

    feature_krx = pd.read_sql(
        f"SELECT * FROM feature_krx WHERE period = '{period}'", conn)
    major = pd.read_sql(
        f"SELECT * FROM major_holder WHERE period = '{period}'", conn)

    # foreign_holding, macro 등도 조인 필요 → datapipeline.py에서 처리
    print(f"[inference] DB에서 {period} 데이터 로드: {len(feature_krx)}종목")
    conn.close()

    # 예측
    result = predict(feature_krx, pkg, period=period)

    # CSV 저장
    if args.csv:
        export_csv(result, args.csv)
    else:
        export_csv(result)

    # DB 저장
    if args.db:
        save_to_db(result, db_cfg)
