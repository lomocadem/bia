import datetime
import logging
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
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

first_day_cur_month = dt.today().replace(day=1)
cur_month = first_day_cur_month.strftime('%Y%m')
last_month = (first_day_cur_month - datetime.timedelta(days=1)).strftime('%Y%m')


def ftpfiles_to_pd(ti, place, start_month, end_month):
    start_month = dt(int(start_month[:4]), int(start_month[4:]), 1)
    end_month = dt(int(end_month[:4]), int(end_month[4:]), 1)
    server = 'ftp.bom.gov.au'
    ftp = ftplib.FTP(server)
    ftp.login()
    directory = f'anon/gen/clim_data/IDCKWCDEA0/tables/nsw/{place}'
    ftp.cwd(directory)
    files = ftp.nlst()
    months = []
    for file in files:
        if ".csv" in file:
            month = get_month(file)
            if end_month > month >= start_month:
                months.append(month)
                with open(file, "wb") as bufer:
                    # use FTP's RETR command to download the file
                    ftp.retrbinary(f"RETR {file}", bufer.write)
                df = process_file(file)
                os.remove(file)
                # print(month, df.head())
                ti.xcom_push(key=f"data_{str(month.strftime('%Y%m'))}", value=df)
    ti.xcom_push(key="months", value=months)
    ftp.close()


def save_to_snowflake_func(ti):
    place_dict = {}
    for place in places:
        months = ti.xcom_pull(key="months", task_ids=f'get_{place.replace("(", "").replace(")", "")}_data')
        data_df_full = pd.DataFrame()
        for month in months:
            data_df = ti.xcom_pull(key=f"data_{str(month.strftime('%Y%m'))}",
                                   task_ids=f'get_{place.replace("(", "").replace(")", "")}_data')
            data_df_full = data_df_full.append(data_df)
            place_dict.update({place: data_df})
    sqlalchemy_url = 'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}' \
        .format(user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PW,
                account_identifier=SNOWFLAKE_ACCOUNT,
                database_name=SNOWFLAKE_WAREHOUSE,
                schema_name=SNOWFLAKE_SCHEMA,
                warehouse_name=SNOWFLAKE_WAREHOUSE)
    engine = create_engine(url=sqlalchemy_url)
    connection = engine.connect()
    # SAVE TO SNOWFLAKE (using sqlalchemy engine)
    for place in places:
        logging.info(f"Updating {place}")
        data_df = place_dict[place]
        assert isinstance(data_df, pd.DataFrame)
        data_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        data_df.to_sql(con=connection, name=SNOWFLAKE_TABLE,
                       if_exists='append', index=False)
    connection.close()
    engine.dispose()


with DAG('snowflake_dump',
         schedule_interval='@monthly',
         start_date=dt(2022, 1, 1),
         catchup=False,
         ) as dag:
    get_ftp_tasks = [
        PythonOperator(
            task_id=f'get_{place.replace("(", "").replace(")", "")}_data',
            python_callable=ftpfiles_to_pd,
            op_kwargs={"place": place, "start_month": "202205", "end_month": "202209"}
        ) for place in places]

    save_to_snowflake = PythonOperator(
        task_id='save_to_snowflake',
        python_callable=save_to_snowflake_func,)
    get_ftp_tasks >> save_to_snowflake
