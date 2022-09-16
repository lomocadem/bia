import datetime
from datetime import datetime as dt
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

first_day_cur_month = dt.today().replace(day=1)
cur_month = first_day_cur_month.strftime('%Y%m')
last_month = (first_day_cur_month - datetime.timedelta(days=1)).strftime('%Y%m')


def ftpfiles_to_pd(ti, place, month):
    server = 'ftp.bom.gov.au'
    ftp = ftplib.FTP(server)
    ftp.login()
    directory = f'anon/gen/clim_data/IDCKWCDEA0/tables/nsw/{place}'
    ftp.cwd(directory)
    files = ftp.nlst()
    filter_files = [file for file in files if month in file]
    filename = filter_files[0]
    with open(filename, "wb") as file:
        # use FTP's RETR command to download the file
        ftp.retrbinary(f"RETR {filename}", file.write)
    df = process_file(filename)
    os.remove(filename)
    ftp.close()
    print(df.head())
    ti.xcom_push(key="new_data", value=df)


def save_to_snowflake_func(ti):
    parent_result = ti.xcom_pull(key="new_data",
                                 task_ids=[f'get_{place.replace("(", "").replace(")", "")}_data' for place in places])
    if not parent_result:
        raise Exception('No Data')
    print(f"THERE ARE {len(parent_result)} datasets")
    sqlalchemy_url = 'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}' \
        .format(user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PW,
                account_identifier=SNOWFLAKE_ACCOUNT,
                database_name=SNOWFLAKE_WAREHOUSE,
                schema_name=SNOWFLAKE_SCHEMA,
                warehouse_name=SNOWFLAKE_WAREHOUSE)
    # try:
    engine = create_engine(url=sqlalchemy_url)
    connection = engine.connect()

    for data_df in parent_result:
        # data_df = parent_result[0]
        # SAVE TO SNOWFLAKE (using sqlalchemy engine)
        assert isinstance(data_df, pd.DataFrame)
        data_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        # print(data_df.head())
        data_df.to_sql(con=connection, name=SNOWFLAKE_TABLE,
                       if_exists='append', index=False)

    # finally:
    connection.close()
    engine.dispose()


with DAG('snowflake_update',
         schedule_interval='@monthly',
         start_date=dt(2022, 1, 1),
         catchup=False,
         ) as dag:
    get_ftp_tasks = [
        PythonOperator(
            task_id=f'get_{place.replace("(", "").replace(")", "")}_data',
            python_callable=ftpfiles_to_pd,
            op_kwargs={"place": place, "month": last_month}
        ) for place in places]

    save_to_snowflake = PythonOperator(
        task_id='save_to_snowflake',
        python_callable=save_to_snowflake_func,
    )
    get_ftp_tasks >> save_to_snowflake
