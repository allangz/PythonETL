from datetime import datetime

import pandas as pd
import sqlite3
import argparse
from pathlib import Path
import logging
import config

logging.basicConfig(filename=config.LOG_FILE_NAME,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract(file_name):
    try:
        df = pd.read_csv(file_name)
        logger.info(f"Extracted Dataframe with {len(df)} columns")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from {file_name}: {e}")
        raise e


def clean_up(data_frame):
    data_frame.drop(["Index", "Organization Id", "Country", "Description", "Industry"], axis='columns', inplace=True)
    data_frame.drop_duplicates(inplace=True)
    data_frame.dropna(inplace=True)
    data_frame['Founded'] = pd.to_datetime(data_frame['Founded'], format='%Y', errors='coerce')
    logger.info(f"Cleaned Dataframe with {len(data_frame)} columns")


def possible_lead(row):
    age = row['age']
    headcount = row['Number of employees']
    if age >= config.HIGH_AGE_LIMIT and headcount >= config.HIGH_HEADCOUNT_LIMIT:
        return 'High'
    elif age >= config.MEDIUM_AGE_LIMIT and headcount >= config.MEDIUM_HEADCOUNT_LIMIT:
        return 'Medium'
    return 'Low'


def business_rules(data_frame):
    today = datetime.now()
    data_frame['age'] = data_frame['Founded'].apply(lambda founded_date: (today.year - founded_date.year))
    data_frame.query(f"age >= {config.AGE_LIMIT} and `Number of employees` >= {config.EMPLOYEES_LIMIT}", inplace=True)
    data_frame['Possible Lead'] = data_frame.apply(possible_lead, axis=1)
    logger.info(f"Applied Business Rules to Dataframe with {len(data_frame)} columns")


def transform_data(data_frame):
    """
        Marketing campaign, contact companies older than 10 years old and with more than 1000 employees
         and by the quantity of employees, make the following classification:
        - headcount >= 5000 & age >= 30, High
        - headcount >= 3000 & age >= 20, Medium
        - headcount >= 1000 & age >= 10, Low
    """
    clean_up(data_frame)
    business_rules(data_frame)
    logger.info(f"Found {len(data_frame)} possible leads")


def load_data(data_frame):
    logger.info("Loading data to Database")
    try:
        with sqlite3.connect(config.DATABASE_NAME) as db_conn:
            data_frame.to_sql(name=config.TABLE_NAME, con=db_conn, index=False, if_exists='replace')
        logger.info(f"Loaded {len(data_frame)} to Database {config.DATABASE_NAME} into table {config.TABLE_NAME}")
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error loading data: {e}")
        raise e


def process(file_name):
    try:
        df = extract(file_name)
        transform_data(df)
        load_data(df)
    except Exception as e:
        logger.error(f"Pipeline failed for {file_name}: {e}")
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A Python ETL")
    parser.add_argument('source_file', help='File to extract data')
    args = parser.parse_args()
    if not Path(args.source_file).exists():
        logger.error(f"File not found {args.source_file}")
        raise FileNotFoundError("File Does not Exist")
    logger.info(f"Processing file {args.source_file}")
    process(file_name=args.source_file)
