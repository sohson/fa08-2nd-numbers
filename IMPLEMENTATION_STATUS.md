# Implementation Status

## Ready

- Streamlit app scaffold
- SQL dump reader
- Latest-period selection based on `feature_krx.period`
- Dynamic `2026_H1` feature construction from real `kospi_friday_daily` data when possible
- Manual SQL update guide
- Data inspection script
- Weekly run entry script

## Not Ready Yet

- Real KRX API collection and ingestion
- Automatic Friday scheduling
- Production DB write-back
- Manual foreign holding validation loader

## Current Blocker

The machine does not currently expose a normal standalone Python installation on PATH.
The MySQL Workbench bundled Python was found, but it is not suitable for a healthy project venv here.

## Next Practical Step

Install a standard Python 3.11 or 3.12 distribution, then recreate `.venv` and install `requirements.txt`.
