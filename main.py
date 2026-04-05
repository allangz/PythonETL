from datetime import datetime

import pandas as pd
import sqlite3


def extract(file_name):
    df = pd.read_csv(file_name)
    return df


def clean_up(data_frame):
    data_frame.drop(["Index", "Organization Id", "Country", "Description", "Industry"], axis='columns', inplace=True)
    data_frame.drop_duplicates(inplace=True)
    data_frame.dropna(inplace=True)
    data_frame['Founded'] = pd.to_datetime(data_frame['Founded'], format='%Y', errors='coerce')


def possible_lead(row):
    HIGH_HEADCOUNT_LIMIT = 5000
    HIGH_AGE_LIMIT = 30
    MEDIUM_HEADCOUNT_LIMIT = 3000
    MEDIUM_AGE_LIMIT = 20

    age = row['age']
    headcount = row['Number of employees']
    if age >= HIGH_AGE_LIMIT and headcount >= HIGH_HEADCOUNT_LIMIT:
        return 'High'
    elif age >= MEDIUM_AGE_LIMIT and headcount >= MEDIUM_HEADCOUNT_LIMIT:
        return 'Medium'
    return 'Low'


def business_rules(data_frame):
    AGE_LIMIT = 10
    EMPLOYEES_LIMIT = 1000
    today = datetime.now()
    data_frame['age'] = data_frame['Founded'].apply(lambda founded_date: (today.year - founded_date.year))
    data_frame.query(f"age >= {AGE_LIMIT} and `Number of employees` >= {EMPLOYEES_LIMIT}", inplace=True)
    data_frame['Possible Lead'] = data_frame.apply(possible_lead, axis=1)


def transform_data(data_frame):
    """
        Marketing campaign, contact companies older than 10 years and with more than 1000 employees
         and by the quantity of employees, make the following classification:
        - headcount >= 5000  &  age >= 30, High
        - headcount >= 3000 & age >= 20, Medium
        - headcount >= 1000 & age >= 10, Low
    """
    clean_up(data_frame)
    business_rules(data_frame)


def load_data(data_frame):
    db_conn = sqlite3.connect('leads_data.db')
    data_frame.to_sql(name='possible_leads', con=db_conn, index=False, if_exists='replace')
    db_conn.close()


def process(file_name):
    df = extract(file_name)
    transform_data(df)
    load_data(df)


if __name__ == '__main__':
    # TODO make a command line argument to pass on the script path/name
    process(file_name='data/organizations-1000000.csv')
