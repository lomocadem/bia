import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import ftplib
from utils.udf import *
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PW = os.getenv("SNOWFLAKE_PW")
SNOWFLAKE_ACCOUNT = 'wv21960.southeast-asia.azure'


def ftpfiles_to_pd(ti, place, logical_date):
    # print(f"LOGICAL DATE: {logical_date}")
    ytd = dt.strptime(logical_date, "%Y-%m-%d") - datetime.timedelta(days=1)
    server = 'ftp.bom.gov.au'
    ftp = ftplib.FTP(server)
    ftp.login()
    directory = f'anon/gen/clim_data/IDCKWCDEA0/tables/nsw/{place}'
    ftp.cwd(directory)
    files = ftp.nlst()
    target_month = ytd.strftime('%Y%m')
    filename = [file for file in files if target_month in file][0]
    with open(filename, "wb") as file:
        # use FTP's RETR command to download the file
        ftp.retrbinary(f"RETR {filename}", file.write)
    df = process_file_daily(filename, ytd)
    os.remove(filename)
    ftp.close()
    # print(df.head())
    ti.xcom_push(key="new_data", value=df)


def save_to_snowflake_func(ti):
    parent_result = ti.xcom_pull(key="new_data",
                                 task_ids=[f'get_{place.replace("(", "").replace(")", "")}_data' for place in places])
    if not parent_result:
        raise Exception('No Data')
    logging.info(f"THERE ARE {len(parent_result)} datasets")
    sqlalchemy_url = 'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}' \
        .format(user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PW,
                account_identifier=SNOWFLAKE_ACCOUNT,
                database_name=SNOWFLAKE_WAREHOUSE,
                schema_name=SNOWFLAKE_SCHEMA,
                warehouse_name=SNOWFLAKE_WAREHOUSE)
    engine = create_engine(url=sqlalchemy_url)
    connection = engine.connect()

    for data_df in parent_result:
        # SAVE TO SNOWFLAKE (using sqlalchemy engine)
        assert isinstance(data_df, pd.DataFrame)
        # Check with DB
        existing_data = pd.read_sql(
            sql=f"SELECT * FROM TEST_TAB WHERE date = '{data_df.iloc[0,1]}' AND station_name = '{data_df.iloc[0,0]}'",
            con=connection)
        if existing_data.empty:
            # New data not existed in the db
            data_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
            logging.info(f"INSERTING {data_df.iloc[0, 0]} {data_df.iloc[0, 1]}")
            data_df.to_sql(con=connection, name=SNOWFLAKE_TABLE,
                           if_exists='append', index=False)
        # else:
        #     # TODO: UPDATE if necessary
    connection.close()
    engine.dispose()


with DAG('snowflake_update',
         schedule_interval='@daily',
         start_date=dt(2022, 9, 10),
         catchup=True,
         ) as dag:
    get_ftp_tasks = [
        PythonOperator(
            task_id=f'get_{place.replace("(", "").replace(")", "")}_data',
            python_callable=ftpfiles_to_pd,
            # op_kwargs={"place": place, "logical_date": "2022-07-01"}  # Choose a specific date
            op_kwargs={"place": place, "logical_date": "{{ ds }}"}  # Access default params (logical_date) using Jinja Template
        ) for place in places]

    save_to_snowflake = PythonOperator(
        task_id='save_to_snowflake',
        python_callable=save_to_snowflake_func,
    )
    get_ftp_tasks >> save_to_snowflake
