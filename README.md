# Loan Default Data Pipeline (Medallion Architecture)

SMU Machine Learning Engineering course, Assignment 1 (2026). A reproducible data pipeline that turns raw loan, customer and clickstream data into a leakage-safe feature store and label store for default prediction. Assignment 2 builds on it: [end-to-end pipeline with Airflow and monitoring](https://github.com/4h4n4-01/loan-default-airflow-mlops).

## Design

| Layer | Tables | What happens |
|---|---|---|
| Bronze | 4 | Raw CSVs ingested verbatim |
| Silver | 4 | Typed, cleaned, standardised; personal data (name, SSN) removed |
| Gold | 2 | ML-ready feature store and label store |

- **No leakage:** features use only information available before the loan start date.
- **Label:** `is_default = 1` if an overdue amount remains at the final instalment.
- **Features:** 70 (demographics, financials, clickstream aggregates).

## Results

- 12,500 loans processed; 28.8% default rate.
- Sanity-check model on the gold tables: 79.4% accuracy (a check that the feature store is usable, not a tuned model).

## Run

```bash
docker-compose build
docker-compose up          # open the JupyterLab link shown in the terminal
python main.py             # in the JupyterLab terminal
```

Output: `datamart/bronze`, `datamart/silver`, `datamart/gold`.

## Repository

```
main.py                    pipeline entry point
utils/bronze.py, silver.py, gold.py
EDA.ipynb                  exploratory analysis
ML Model Training.ipynb    sanity-check model
data/                      raw CSVs
Dockerfile, docker-compose.yaml, requirements.txt
```
