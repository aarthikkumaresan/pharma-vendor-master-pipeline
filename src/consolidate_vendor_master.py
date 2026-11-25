#############################################################################################################################
# Script Name: consolidate_vendor_master.py
# Objective:  Clean, standardize, validate, and consolidate
#             vendor data from multiple sources and load the
#             processed data into SQLite database tables.
# Created By: Aarthi
# Created Date: 23-11-2026
# Modified Date: 23-11-2026
# Modified By: Aarthi
# Comments: Implements the six processing stages in the Dirty Vendor Master
#           Consolidation & load into a database 
#############################################################################################################################

"""
PSEUDOCODE:

    Step 1:Read the configuration file and set up the logging configuration.
    Step 2:Validate the required ERP and manual procurement CSV input files.
    Step 3:Load both CSV sources, add source lineage tags, and combine the datasets.
    Step 4:Standardize vendor IDs, vendor names, country values, and other string fields.
    Step 5:Parse numeric values and convert the specified date fields into the required format.
    Step 6:Apply the data quality gate and separate invalid records into the quarantine dataset.
    Step 7:Impute missing lead-time values using the median and remove duplicate vendor records.
    Step 8:Normalize material categories and create separate vendor-category records.
    Step 9:Create the required SQLite tables and load the cleaned, category, and quarantine data.
    Step 10:Export the quarantine report and generate the final ETL execution summary.

"""

# =============================================================================
# IMPORT REQUIRED LIBRARIES
# =============================================================================

import logging
import re
import configparser
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

# =============================================================================
# PIPELINE CONFIGURATION & CONSTANTS
# =============================================================================

# Project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Read configuration file
CONFIG_FILE_PATH = BASE_DIR / "config" / "config.example.ini"

config = configparser.ConfigParser()

if not CONFIG_FILE_PATH.exists():
    raise FileNotFoundError(
        f"Configuration file not found: {CONFIG_FILE_PATH}"
    )

config.read(CONFIG_FILE_PATH, encoding="utf-8")

# File paths
ERP_FILE_PATH = BASE_DIR / config["paths"]["erp_file"]
MANUAL_FILE_PATH = BASE_DIR / config["paths"]["manual_file"]
DB_FILE_PATH = BASE_DIR / config["paths"]["database"]
QUARANTINE_REPORT_PATH = BASE_DIR / config["paths"]["quarantine_report"]
LOG_DIR = BASE_DIR / config["paths"]["log_dir"]

# Database table names
MASTER_TABLE = config["database"]["master_table"]
CATEGORY_TABLE = config["database"]["category_table"]
QUARANTINE_TABLE = config["database"]["quarantine_table"]

# Logging level
LOG_LEVEL = getattr(
    logging,
    config["logging"].get("level", "INFO").upper(),
    logging.INFO
)

COUNTRY_MAPPING = {
    "INDIA": "India",
    "india": "India",
    "USA": "USA",
    "United States": "USA",
    "germany": "Germany",
    "uk": "UK",
    "UK": "UK"
}

EXPECTED_COLUMNS = [
    "vendor_id",
    "vendor_name",
    "country",
    "onboarded_date",
    "lead_time_days",
    "contract_value_usd",
    "material_categories",
    "last_modified"
]


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

def setup_logger():
    """
    Initialize logging.
    Create a logs directory and a timestamped log file.
    Log messages to both file and console.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_file = LOG_DIR / f"consolidate_vendor_master_{timestamp}.log"

    logger = logging.getLogger("VENDOR_MASTER_PIPELINE")
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False

    # Clear old handlers if the function is called more than once.
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    console_handler = logging.StreamHandler(sys.stdout)

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# =============================================================================
# INPUT FILE VALIDATION
# =============================================================================

def validate_input_files(erp_path: Path, manual_path: Path):
    """
    Validate that both required source CSV files exist.
    """

    required_files = [erp_path, manual_path]

    for file_path in required_files:
        if not file_path.exists():
            raise FileNotFoundError(
                f"Required input file not found: {file_path}"
            )

        if not file_path.is_file():
            raise FileNotFoundError(
                f"Input path is not a file: {file_path}"
            )


# =============================================================================
# STEP 1: INGESTION & SOURCE LINEAGE TAGGING
# =============================================================================

def load_raw_sources(
    erp_path: Path,
    manual_path: Path,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Step 1:
    Load both CSV sources, tag their lineage, and concatenate them.
    """

    logger.info("Step 1: Source ingestion and lineage tagging started")

    erp_df = pd.read_csv(
        erp_path,
        dtype={"vendor_id": "string"},
        skipinitialspace=True
    )
    erp_df["source_system"] = "ERP"

    manual_df = pd.read_csv(
        manual_path,
        dtype={"vendor_id": "string"},
        skipinitialspace=True
    )
    manual_df["source_system"] = "MANUAL_LOG"

    # Validate required source columns before combining.
    for source_name, source_df in [
        ("ERP", erp_df),
        ("MANUAL_LOG", manual_df)
    ]:
        missing_columns = [
            column for column in EXPECTED_COLUMNS
            if column not in source_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"{source_name} CSV is missing required columns: "
                f"{missing_columns}"
            )

    combined_df = pd.concat(
        [erp_df, manual_df],
        ignore_index=True
    )

    logger.info(f"ERP records loaded: {len(erp_df)}")
    logger.info(f"Manual procurement records loaded: {len(manual_df)}")
    logger.info(f"Total combined records: {len(combined_df)}")
    logger.info("Step 1 completed successfully")

    return combined_df


# =============================================================================
# STEP 2: STRING & COUNTRY STANDARDIZATION
# =============================================================================

def standardize_strings_and_countries(
    df: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Step 2:
    Standardize whitespace, vendor IDs, vendor names, and country values.
    """

    logger.info("Step 2: String and country standardization started")

    df = df.copy()

    # Strip leading and trailing whitespace from all text columns.
    text_columns = df.select_dtypes(
        include=["object", "string"]
    ).columns

    for column in text_columns:
        df[column] = df[column].apply(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    # Standardize vendor ID.
    df["vendor_id"] = (
        df["vendor_id"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    # Standardize vendor name to Title Case.
    df["vendor_name"] = (
        df["vendor_name"]
        .astype("string")
        .str.strip()
        .str.title()
    )

    # Standardize country names; preserve values not present in mapping.
    df["country"] = (
        df["country"]
        .astype("string")
        .str.strip()
        .replace(COUNTRY_MAPPING)
    )

    logger.info("Whitespace standardization completed")
    logger.info("Vendor IDs converted to uppercase")
    logger.info("Vendor names converted to Title Case")
    logger.info("Country mapping applied")
    logger.info("Step 2 completed successfully")

    return df


# =============================================================================
# STEP 3: NUMERIC & DATE PARSING
# =============================================================================

def parse_mixed_date(value):
    """
    Parse the date formats:
    
    """

    if pd.isna(value) or str(value).strip() == "":
        return pd.NaT

    value = str(value).strip()

    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y"
    ]

    for date_format in date_formats:
        try:
            return pd.to_datetime(
                value,
                format=date_format,
                errors="raise"
            )
        except (ValueError, TypeError):
            continue

    # Fallback for any additional recognizable date representation.
    return pd.to_datetime(value, errors="coerce", format="mixed")


def parse_types_and_dates(
    df: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Step 3:
    Clean currency values and parse numeric and date columns.
    """

    logger.info("Step 3: Numeric and date parsing started")

    df = df.copy()

    # Clean contract values: remove $, commas, USD and extra spaces.
    def clean_currency(value):
        if pd.isna(value):
            return np.nan

        cleaned_value = str(value).strip()
        cleaned_value = re.sub(r"(?i)\bUSD\b", "", cleaned_value)
        cleaned_value = cleaned_value.replace("$", "")
        cleaned_value = cleaned_value.replace(",", "")
        cleaned_value = cleaned_value.strip()

        return cleaned_value if cleaned_value else np.nan

    df["contract_value_usd"] = (
        df["contract_value_usd"]
        .apply(clean_currency)
    )

    df["contract_value_usd"] = pd.to_numeric(
        df["contract_value_usd"],
        errors="coerce"
    )

    # Convert lead time to numeric so negative values and missing values
    # can be checked and imputed in the following stages.
    df["lead_time_days"] = pd.to_numeric(
        df["lead_time_days"],
        errors="coerce"
    )

    # Parse mixed onboarded date formats.
    df["onboarded_date"] = df["onboarded_date"].apply(
        parse_mixed_date
    )

    # Parse last_modified into pandas datetime values.
    df["last_modified"] = pd.to_datetime(
        df["last_modified"],
        errors="coerce",
        format="mixed"
    )

    logger.info("Contract currency values converted to numeric")
    logger.info("Lead time values converted to numeric")
    logger.info("Onboarded dates parsed")
    logger.info("Last modified timestamps parsed")
    logger.info("Step 3 completed successfully")

    return df


# =============================================================================
# STEP 4: QUARANTINE SPLIT (DATA QUALITY GATE)
# =============================================================================

def split_quarantine_records(
    df: pd.DataFrame,
    logger: logging.Logger
):
    """
    Step 4:
    Separate invalid records from valid records before imputation
    and deduplication.

    Rejection rules:
        MISSING_VENDOR_NAME
        INVALID_CONTRACT_VALUE
        NEGATIVE_LEAD_TIME
    """

    logger.info("Step 4: Data quality gate and quarantine split started")

    df = df.copy()

    # Prepare rejection reason for every record.
    rejection_reason = pd.Series(
        pd.NA,
        index=df.index,
        dtype="string"
    )

    # Rule 1: Missing or blank vendor name.
    missing_vendor_name = (
        df["vendor_name"].isna()
        | df["vendor_name"].astype("string").str.strip().eq("").fillna(True)
    )

    rejection_reason.loc[
        missing_vendor_name
    ] = "MISSING_VENDOR_NAME"

    # Rule 2: Invalid or unparseable contract value.
    invalid_contract_value = df["contract_value_usd"].isna()

    rejection_reason.loc[
        rejection_reason.isna() & invalid_contract_value
    ] = "INVALID_CONTRACT_VALUE"

    # Rule 3: Negative lead time.
    negative_lead_time = df["lead_time_days"] < 0

    rejection_reason.loc[
        rejection_reason.isna() & negative_lead_time
    ] = "NEGATIVE_LEAD_TIME"

    # Split invalid and valid records.
    quarantine_mask = rejection_reason.notna()

    df_quarantine = df.loc[quarantine_mask].copy()
    df_quarantine["rejection_reason"] = (
        rejection_reason.loc[quarantine_mask]
    )

    df_clean = df.loc[~quarantine_mask].copy()

    logger.info(
        f"Missing vendor name records: {int(missing_vendor_name.sum())}"
    )
    logger.info(
        f"Invalid contract value records: {int(invalid_contract_value.sum())}"
    )
    logger.info(
        f"Negative lead time records: {int(negative_lead_time.sum())}"
    )
    logger.info(f"Valid records: {len(df_clean)}")
    logger.warning(f"Quarantined records: {len(df_quarantine)}")

    logger.info("Step 4 completed successfully")

    return df_clean, df_quarantine


# =============================================================================
# STEP 5: MISSING VALUE IMPUTATION & DEDUPLICATION
# =============================================================================

def impute_and_deduplicate(
    df_clean: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Step 5:
    Impute missing lead times using the rounded median of valid records,
    then retain the latest record for each vendor.
    """

    logger.info("Step 5: Imputation and deduplication started")

    df_clean = df_clean.copy()

    # Flag rows whose lead time is missing before filling.
    df_clean["is_lead_time_imputed"] = (
        df_clean["lead_time_days"].isna()
    )

    # Calculate median using available valid lead-time values.
    median_lead_time = df_clean["lead_time_days"].median()

    if pd.isna(median_lead_time):
        raise ValueError(
            "Cannot impute lead_time_days because all valid lead times are missing"
        )

    median_lead_time = int(round(median_lead_time))

    missing_lead_time_count = int(
        df_clean["lead_time_days"].isna().sum()
    )

    df_clean["lead_time_days"] = (
        df_clean["lead_time_days"].fillna(median_lead_time).round().astype(int)
    )

    logger.info(f"Median lead time used for imputation: {median_lead_time}")
    logger.info(
        f"Missing lead times imputed: {missing_lead_time_count}"
    )

    # Sort by last_modified and keep the latest record for each vendor.
    if df_clean["last_modified"].isna().any():
        raise ValueError(
            "One or more valid records have an unparseable last_modified value; "
            "cannot safely determine the latest vendor record"
        )

    df_clean = df_clean.sort_values(
        by="last_modified",
        ascending=True
    )

    before_deduplication = len(df_clean)

    df_clean = df_clean.drop_duplicates(
        subset=["vendor_id"],
        keep="last"
    ).reset_index(drop=True)

    duplicates_removed = before_deduplication - len(df_clean)

    logger.info(f"Duplicate vendor records removed: {duplicates_removed}")
    logger.info(f"Unique vendors after deduplication: {len(df_clean)}")
    logger.info("Step 5 completed successfully")

    return df_clean


# =============================================================================
# STEP 6A: CATEGORY NORMALIZATION USING EXPLODE()
# =============================================================================

def build_normalized_category_bridge(
    df_clean: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Step 6A:
    Split comma-separated material categories and explode them into
    normalized vendor-category rows.
    """

    logger.info("Step 6A: Category normalization started")

    category_df = df_clean[
        ["vendor_id", "material_categories"]
    ].copy()

    # Split category strings into lists.
    category_df["material_category"] = (
        category_df["material_categories"]
        .fillna("")
        .astype("string")
        .str.split(",")
    )

    # Remove material_categories because the normalized list is now available.
    category_df = category_df.drop(
        columns=["material_categories"]
    )

    # Strip whitespace from each category token.
    category_df["material_category"] = (
        category_df["material_category"]
        .apply(
            lambda categories: [
                category.strip() for category in categories
            ]
        )
    )

    # Explode lists into individual vendor-category rows.
    df_bridge = category_df.explode(
        "material_category"
    )

    # Remove null and empty category values.
    df_bridge["material_category"] = (
        df_bridge["material_category"].astype("string").str.strip()
    )

    df_bridge = df_bridge[
        df_bridge["material_category"].notna()
        & df_bridge["material_category"].ne("")
    ].copy()

    df_bridge = df_bridge.reset_index(drop=True)

    logger.info(
        f"Normalized vendor-category rows created: {len(df_bridge)}"
    )
    logger.info("Step 6A completed successfully")

    return df_bridge


# =============================================================================
# STEP 6B: DATABASE TABLE CREATION
# =============================================================================

def create_database_tables(conn: sqlite3.Connection):
    """
    Create the three target relational tables.

    processed_at is included in all three output tables to retain
    processing metadata as required by the pipeline specification.
    """

    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
            vendor_id TEXT PRIMARY KEY,
            vendor_name TEXT NOT NULL,
            country TEXT NOT NULL,
            onboarded_date TEXT NOT NULL,
            lead_time_days INTEGER NOT NULL,
            is_lead_time_imputed INTEGER NOT NULL,
            contract_value_usd REAL NOT NULL,
            source_system TEXT NOT NULL,
            last_modified TEXT NOT NULL,
            processed_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CATEGORY_TABLE} (
            vendor_id TEXT NOT NULL,
            material_category TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (vendor_id, material_category),
            FOREIGN KEY (vendor_id)
                REFERENCES {MASTER_TABLE}(vendor_id)
        )
        """
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id TEXT,
            vendor_name TEXT,
            country TEXT,
            onboarded_date TEXT,
            lead_time_days REAL,
            contract_value_usd TEXT,
            material_categories TEXT,
            source_system TEXT,
            last_modified TEXT,
            rejection_reason TEXT NOT NULL,
            processed_at TEXT NOT NULL
        )
        """
    )


# =============================================================================
# STEP 6B: DATABASE LOADING
# =============================================================================

def persist_to_database(
    df_clean: pd.DataFrame,
    df_bridge: pd.DataFrame,
    df_quarantine: pd.DataFrame,
    db_path: Path,
    logger: logging.Logger
):
    """
    Step 6B:
    Create the SQLite database and populate all three target tables.
    """

    logger.info("Step 6B: Database creation and loading started")

    processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    master_df = df_clean.copy()
    bridge_df = df_bridge.copy()
    quarantine_df = df_quarantine.copy()

    # Add processing timestamp to all output DataFrames.
    master_df["processed_at"] = processed_at
    bridge_df["processed_at"] = processed_at
    quarantine_df["processed_at"] = processed_at

    # Convert datetime columns to SQL-compatible strings.
    for frame in [master_df, quarantine_df]:
        if "onboarded_date" in frame.columns:
            frame["onboarded_date"] = pd.to_datetime(
                frame["onboarded_date"],
                errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        if "last_modified" in frame.columns:
            frame["last_modified"] = pd.to_datetime(
                frame["last_modified"],
                errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert boolean flag to SQLite-compatible integer values.
    master_df["is_lead_time_imputed"] = (
        master_df["is_lead_time_imputed"].astype(int)
    )

    # Keep only columns belonging to the master table.
    master_columns = [
        "vendor_id",
        "vendor_name",
        "country",
        "onboarded_date",
        "lead_time_days",
        "is_lead_time_imputed",
        "contract_value_usd",
        "source_system",
        "last_modified",
        "processed_at"
    ]
    master_df = master_df[master_columns]

    # Keep only columns belonging to the category bridge table.
    bridge_df = bridge_df[
        ["vendor_id", "material_category", "processed_at"]
    ]

    # Keep only columns belonging to the quarantine table.
    quarantine_columns = [
        "vendor_id",
        "vendor_name",
        "country",
        "onboarded_date",
        "lead_time_days",
        "contract_value_usd",
        "material_categories",
        "source_system",
        "last_modified",
        "rejection_reason",
        "processed_at"
    ]
    quarantine_df = quarantine_df[quarantine_columns]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        create_database_tables(conn)

        # Clear child tables before the parent table to respect the
        # foreign-key relationship. This makes reruns repeatable.
        conn.execute(f"DELETE FROM {CATEGORY_TABLE}")
        conn.execute(f"DELETE FROM {QUARANTINE_TABLE}")
        conn.execute(f"DELETE FROM {MASTER_TABLE}")

        master_df.to_sql(
            MASTER_TABLE,
            conn,
            if_exists="append",
            index=False
        )

        bridge_df.to_sql(
            CATEGORY_TABLE,
            conn,
            if_exists="append",
            index=False
        )

        quarantine_df.to_sql(
            QUARANTINE_TABLE,
            conn,
            if_exists="append",
            index=False
        )

        master_count = conn.execute(
            f"SELECT COUNT(*) FROM {MASTER_TABLE}"
        ).fetchone()[0]

        category_count = conn.execute(
            f"SELECT COUNT(*) FROM {CATEGORY_TABLE}"
        ).fetchone()[0]

        quarantine_count = conn.execute(
            f"SELECT COUNT(*) FROM {QUARANTINE_TABLE}"
        ).fetchone()[0]

    logger.info(f"Database created/updated: {db_path.name}")
    logger.info(f"Rows loaded into {MASTER_TABLE}: {master_count}")
    logger.info(f"Rows loaded into {CATEGORY_TABLE}: {category_count}")
    logger.info(f"Rows loaded into {QUARANTINE_TABLE}: {quarantine_count}")
    logger.info("Step 6B completed successfully")

    return master_count, category_count, quarantine_count


# =============================================================================
# QUARANTINE REPORT EXPORT
# =============================================================================

def export_quarantine_report(
    df_quarantine: pd.DataFrame,
    report_path: Path,
    logger: logging.Logger
):
    """
    Export quarantined records to quarantine_report.csv.
    """

    logger.info("Quarantine CSV export started")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df = df_quarantine.copy()
    report_df["processed_at"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    if "onboarded_date" in report_df.columns:
        report_df["onboarded_date"] = pd.to_datetime(
            report_df["onboarded_date"],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "last_modified" in report_df.columns:
        report_df["last_modified"] = pd.to_datetime(
            report_df["last_modified"],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    report_df.to_csv(
        report_path,
        index=False
    )

    logger.info(f"Quarantine report created: {report_path}")
    logger.info(f"Quarantine report records: {len(report_df)}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():

    start_time = datetime.now()
    logger = setup_logger()
    status = "FAILED"

    logger.info(
        f"consolidated_vendor_master.py started at "
        f"{start_time.strftime('%d-%m-%Y %H:%M:%S')}"
    )

    try:

        # ---------------------------------------------------------------------
        # INPUT FILE VALIDATION
        # ---------------------------------------------------------------------

        logger.info("Pre-processing: Input CSV file validation started")

        validate_input_files(
            ERP_FILE_PATH,
            MANUAL_FILE_PATH
        )

        logger.info(f"ERP input file exists: {ERP_FILE_PATH.name}")
        logger.info(f"Manual input file exists: {MANUAL_FILE_PATH.name}")
        logger.info("Pre-processing: Input file validation completed successfully")

        # ---------------------------------------------------------------------
        # STEP 1: INGESTION & SOURCE LINEAGE TAGGING
        # ---------------------------------------------------------------------

        raw_df = load_raw_sources(
            ERP_FILE_PATH,
            MANUAL_FILE_PATH,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 2: STRING & COUNTRY STANDARDIZATION
        # ---------------------------------------------------------------------

        std_df = standardize_strings_and_countries(
            raw_df,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 3: NUMERIC & DATE PARSING
        # ---------------------------------------------------------------------

        parsed_df = parse_types_and_dates(
            std_df,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 4: QUARANTINE SPLIT (DATA QUALITY GATE)
        # ---------------------------------------------------------------------

        df_clean, df_quarantine = split_quarantine_records(
            parsed_df,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 5: MISSING VALUE IMPUTATION & DEDUPLICATION
        # ---------------------------------------------------------------------

        df_clean_dedup = impute_and_deduplicate(
            df_clean,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 6A: CATEGORY NORMALIZATION USING EXPLODE()
        # ---------------------------------------------------------------------

        df_bridge = build_normalized_category_bridge(
            df_clean_dedup,
            logger
        )

        # ---------------------------------------------------------------------
        # STEP 6B: DATABASE TABLE CREATION & LOADING
        # ---------------------------------------------------------------------

        master_count, category_count, quarantine_count = persist_to_database(
            df_clean_dedup,
            df_bridge,
            df_quarantine,
            DB_FILE_PATH,
            logger
        )

        # ---------------------------------------------------------------------
        # FINAL DELIVERABLE: QUARANTINE REPORT CSV EXPORT
        # ---------------------------------------------------------------------

        export_quarantine_report(
            df_quarantine,
            QUARANTINE_REPORT_PATH,
            logger
        )

        # ---------------------------------------------------------------------
        # EXECUTION SUMMARY
        # ---------------------------------------------------------------------

        logger.info("=" * 75)
        logger.info("VENDOR MASTER PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 75)

        logger.info(f"Total source records: {len(raw_df)}")
        logger.info(f"Valid records before deduplication: {len(df_clean)}")
        logger.info(f"Quarantined records: {len(df_quarantine)}")
        logger.info(f"Unique vendors loaded: {master_count}")
        logger.info(f"Category bridge rows loaded: {category_count}")
        logger.info(f"Quarantine rows loaded: {quarantine_count}")
        logger.info(f"SQLite database: {DB_FILE_PATH}")
        logger.info(f"Quarantine CSV: {QUARANTINE_REPORT_PATH}")

        status = "SUCCESS"

    except Exception:
        logger.exception("Vendor master data wrangling pipeline failed")
        raise

    finally:

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"Execution status: {status}")
        logger.info(f"Total execution time: {duration:.2f} seconds")
        logger.info(
            f"consolidated_vendor_master.py completed at "
            f"{end_time.strftime('%d-%m-%Y %H:%M:%S')}"
        )


if __name__ == "__main__":
    main()
