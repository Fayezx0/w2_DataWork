# 📊 E-Commerce Data Pipeline & Analysis

A professional data workflow built with **Python**, **Pandas**, and **Plotly**. This project implements a robust ETL pipeline to clean raw transaction data and performs exploratory analysis to uncover revenue trends.

![Revenue by Country](reports/figures/revenue_by_country.png)

## Quick Start

### 1. Setup Environment
```bash
python -m venv .venv
source .\.venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### 2. Run the Pipeline
Process raw data into clean analytics tables:
```bash
python scripts/run_etl.py
```

### 3. Explore Results
- **Interactive Analysis**: Open `notebooks/eda.ipynb` to see dynamic **Plotly** charts.
- **Key Findings**: Read the [Summary Report](reports/summary.md).
- **Processed Data**: Found in `data/processed/` (Parquet format).

---

## 📂 Project Flow

The data flows through the project files as follows:

```text
data/raw/                     # 1. Inputs (CSV)
├── orders.csv
└── users.csv
scripts/run_etl.py            # 2. Pipeline Execution
data/processed/               # 3. Outputs (Parquet)
├── analytics_table.parquet   <-- Main analysis table
├── orders_clean.parquet
└── _run_meta.json
notebooks/eda.ipynb           # 4. Analysis & Viz (Plotly)
reports/                      # 5. Deliverables
├── figures/*.png             <-- Exported Charts
└── summary.md                <-- Executive Report

.
├── data/
│   ├── raw/                      # Input
│   │   ├── orders.csv
│   │   └── users.csv
│   └── processed/                # Output
│       ├── analytics_table.parquet   <-- FINAL CLEAN TABLE
│       ├── orders_clean.parquet
│       └── _run_meta.json            <-- Run metadata (row counts, timestamps)
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

## Tech Stack
- **ETL**: Custom Python pipeline with strictly typed configs.
- **Data**: Pandas for transformation, Parquet for efficient storage.
- **Viz**: **Plotly Express** for interactive visualizations.
