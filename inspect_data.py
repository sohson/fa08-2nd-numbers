from __future__ import annotations

from collections import defaultdict

from src.config import load_config
from src.sql_dump import load_table


def period_sort_key(period: str) -> tuple[int, int]:
    year_text, half = period.split("_")
    return int(year_text), 1 if half == "H1" else 2


def main() -> None:
    config = load_config()

    feature_krx = load_table(config.sql_dump_path, "feature_krx")
    major_holder = load_table(config.sql_dump_path, "major_holder")
    filter_flag = load_table(config.sql_dump_path, "filter_flag")
    period_table = load_table(config.sql_dump_path, "period")
    foreign_holding = load_table(config.sql_dump_path, "foreign_holding")
    stock_meta = load_table(config.sql_dump_path, "stock_meta")
    sector_map = load_table(config.sql_dump_path, "sector_map")

    periods = sorted(feature_krx["period"].dropna().unique().tolist(), key=period_sort_key)
    if not periods:
        print("feature_krx에 period 데이터가 없습니다.")
        return

    latest_period = periods[-1]
    print(f"latest_feature_period: {latest_period}")
    print(f"feature_krx_rows[{latest_period}]: {len(feature_krx.loc[feature_krx['period'] == latest_period])}")
    print(f"major_holder_rows[{latest_period}]: {len(major_holder.loc[major_holder['period'] == latest_period])}")
    print(f"filter_flag_rows[{latest_period}]: {len(filter_flag.loc[filter_flag['flag_date'] == latest_period])}")
    print(f"period_rows[{latest_period}]: {len(period_table.loc[period_table['period'] == latest_period])}")

    latest_foreign_ym = foreign_holding["ym"].dropna().max() if not foreign_holding.empty else None
    print(f"latest_foreign_ym: {int(latest_foreign_ym) if latest_foreign_ym else 'N/A'}")
    print(f"stock_meta_rows: {len(stock_meta)}")
    print(f"sector_map_rows: {len(sector_map)}")

    coverage = defaultdict(dict)
    for period in periods[-6:]:
        coverage[period]["feature_krx"] = len(feature_krx.loc[feature_krx["period"] == period])
        coverage[period]["major_holder"] = len(major_holder.loc[major_holder["period"] == period])
        coverage[period]["filter_flag"] = len(filter_flag.loc[filter_flag["flag_date"] == period])
        coverage[period]["period"] = len(period_table.loc[period_table["period"] == period])

    print("\nrecent_period_coverage:")
    for period in sorted(coverage.keys(), key=period_sort_key):
        row = coverage[period]
        print(
            f"- {period}: "
            f"feature_krx={row['feature_krx']}, "
            f"major_holder={row['major_holder']}, "
            f"filter_flag={row['filter_flag']}, "
            f"period={row['period']}"
        )


if __name__ == "__main__":
    main()
