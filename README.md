## Data Work project
# 📊 E-Commerce Data Pipeline & Analysis

A professional data workflow built with **Python**, **Pandas**, and **Plotly**. This project implements a robust ETL pipeline to clean raw transaction data and performs exploratory analysis to uncover revenue trends.

![Revenue by Country](reports/figures/revenue_by_country.png)

## Quick Start

### 1. Setup Environment
```bash
git clone https://github.com/Fayezx0/w2_DataWork
cd w2_DataWork
python -m venv .venv
source .\.venv\Scripts\activate
uv pip install -r requirements.txt
uv pip install -e .
```

### 2. Run the Pipeline
Process raw data into clean analytics tables:
```bash
python scripts/run_etl.py
```

You can also run individual stages:
```powershell
python scripts/run_day1_load.py    # load raw CSVs into interim staging
python scripts/run_day2_clean.py   # cleaning & type conversions
python scripts/run_day3_build_analytics.py  # build analytics tables
```

### 3. Explore Results
- **Interactive Analysis**: Open `notebooks/eda.ipynb` to see dynamic **Plotly** charts.
- **Key Findings**: Read the [Summary Report](reports/summary.md).
- **Processed Data**: Found in `data/processed/` (Parquet format).

---

## 📂 Project Flow

The data flows through the project files as follows:

```text
.
├── data/
│   ├── raw/                      # Input
│   │   ├── orders.csv
│   │   └── users.csv
│   └── processed/                # Output
│       ├── analytics_table.parquet   <-- FINAL CLEAN TABLE
│       ├── orders_clean.parquet
│       └── _run_meta.json            <-- Metadata (row counts, timestamps)
│
├── src/bootcamp_data/            # Core Logic
│   ├── etl.py                        <-- Main Pipeline Logic
│   ├── transforms.py
│   └── joins.py
│
├── scripts/                      # Execution
│   └── run_etl.py                    <-- Entry point: `python scripts/run_etl.py`
│
├── notebooks/                    # Laboratory
│   └── eda.ipynb                     <-- Exploratory Analysis & Charts
│
└── reports/                      # Deliverables
    ├── figures/                      <-- Exported PNG Charts
    └── summary.md                    <-- Executive Summary
```

---

