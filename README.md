# Pharmaceutical Vendor Master Data Wrangling Pipeline

## Project Overview

This project focuses on cleaning, standardizing, validating, and consolidating vendor data collected from multiple sources in a pharmaceutical procurement system.

The pipeline processes ERP Vendor Master data and Manual Procurement Log data using Python and stores the cleaned and normalized records in an SQLite database.

## Project Objective

To consolidate vendor information from multiple sources, improve data quality, identify invalid records, remove duplicates, normalize material categories, and store the processed data in a structured SQLite database.

## Technologies Used

- Python
- Pandas
- NumPy
- SQLite
- SQL
- ConfigParser
- Logging

## Project Structure

```text
Data_Wrangling/
│
├── config/
│   └── config.example.ini
│
├── data/
│   ├── raw/
│   │   ├── erp_vendor_master.csv
│   │   └── manual_procurement_log.csv
│   │
│   └── output/
│
├── logs/
│
├── sql/
│   ├── ddl_schema.sql
│   └── verification_queries.sql
│
├── src/
│   └── consolidate_vendor_master.py
│
├── .gitignore
├── requirements.txt
└── README.md
```

## Pipeline Processing Stages

1. **Data Ingestion:** Load ERP and Manual Procurement CSV files and identify their source systems.
2. **Data Standardization:** Clean whitespace, standardize vendor IDs, vendor names, and country values.
3. **Data Type Conversion:** Convert currency and numeric values and parse date fields.
4. **Data Quality Validation:** Identify invalid records and move them into quarantine.
5. **Imputation and Deduplication:** Fill missing lead-time values using the median and remove duplicate vendor records by retaining the latest record.
6. **Category Normalization:** Split material categories into individual vendor-category records.
7. **Database Loading:** Create and populate SQLite tables with cleaned, normalized, and quarantined records.
8. **Report Generation:** Export quarantined records into a CSV report and generate execution logs.

## Database Tables

The pipeline creates three SQLite tables:

| Table | Description |
|---|---|
| gold_vendor_master | Stores cleaned and consolidated vendor records |
| gold_vendor_categories | Stores normalized vendor-category relationships |
| err_vendor_quarantine | Stores invalid records along with rejection reasons |

## Installation and Execution

### 1. Prerequisites

- Python 3.x
- pip

### 2. Clone the Repository

```bash
git clone https://github.com/aarthikkumaresan/pharma-vendor-master-pipeline.git
cd pharma-vendor-master-pipeline
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure the Project

The `config/config.example.ini` file contains the sample configuration settings, including input file paths, output paths, database table names, and logging level.

The Python script directly reads this configuration file during execution. Update the paths and settings in this file if required.
### 5. Run the Pipeline


python src/consolidate_vendor_master.py


## Output Files

After successful execution, the pipeline generates:

- `data/output/pharmaceutical_procurement.db` – SQLite database containing the processed vendor records.
- `data/output/quarantine_report.csv` – Report containing rejected vendor records.
- `logs/` – Timestamped log files containing execution details and processing summaries.

These runtime-generated files are excluded from Git tracking using `.gitignore`.

## Execution Summary

The pipeline was successfully executed with the following results:

| Description | Records |
|---|---:|
| Total source records | 10 |
| Valid records before deduplication | 7 |
| Quarantined records | 3 |
| Duplicate records removed | 2 |
| Unique vendors loaded | 5 |
| Normalized category records | 8 |

## Conclusion

The Pharmaceutical Vendor Master Data Wrangling Pipeline provides a structured approach to integrating vendor data from multiple sources, improving data quality, handling invalid records, eliminating duplicates, and storing consolidated information in a relational database.