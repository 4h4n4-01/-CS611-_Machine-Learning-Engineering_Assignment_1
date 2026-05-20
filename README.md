# -CS611-_Machine-Learning-Engineering_Assignment_1

## Overview
This repository contains the data pipeline for a loan default prediction system built using the Medallion Architecture (Bronze → Silver → Gold).

## Project Structure
CS611_MLE_Assignment_1/
├── main.py                    # Main pipeline entry point
├── Dockerfile                 # Docker container setup
├── docker-compose.yaml        # Docker compose configuration
├── requirements.txt           # Python dependencies
├── Readme.txt                 # GitHub repo link
├── EDA.ipynb                  # Exploratory Data Analysis notebook
├── ML Model Training.ipynb    # ML sanity check notebook 
├── data/                      # Raw CSV data files
└── utils/
├── bronze.py              # Bronze layer: raw ingestion
├── silver.py              # Silver layer: cleaning & standardisation
└── gold.py                # Gold layer: feature store & label store

## Steps to make this run: 

### Step 1: Build Docker container
```bash
docker-compose build
```

### Step 2: Start JupyterLab
```bash
docker-compose up
```
Open the link shown in terminal (http://127.0.0.1:8888/lab)

### Step 3: Run the pipeline
In JupyterLab terminal:
```bash
python main.py
```

## Output
Running `main.py` creates a `datamart/` folder that shall constitute:
- `datamart/bronze/` - Raw copies of source CSVs
- `datamart/silver/` - Cleaned and standardised tables
- `datamart/gold/` - ML-ready feature store and label store

## Data Pipeline
| Layer | Tables | Description |
|---|---|---|
| Bronze | 4 tables | Raw ingestion of CSV files |
| Silver | 4 tables | Cleaned, typed, PII-removed |
| Gold | 2 tables | ML-ready feature store + label store |

## Key Design Decisions
- **No data leakage**: Features only use data available before loan start date
- **PII removal**: Name and SSN removed in silver layer
- **Default label**: is_default = 1 if overdue_amt > 0 at final installment
- **70 features**: Demographics + financials + clickstream aggregates

## Results
- 12,500 loans processed
- 28.8% default rate
- ML sanity check accuracy: 79.4%