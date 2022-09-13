from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator

SNOWFLAKE_CONN_ID = ''
SNOWFLAKE_WAREHOUSE = 'test'
SNOWFLAKE_DATABASE = 'TEST'
SNOWFLAKE_TEST_SQL = f"select * from TEST.PUBLIC.TEST_TAB limit 100;"

snowflake_query = [
    f"""create table PUBLIC.TEST_TAB (id number, name string);""",
    f"""insert into public.test_employee values(1, “Sam”),(2, “Andy”),(3, “Gill”);""",
]

with DAG(
        'snowflake_test',
        start_date=datetime(2021, 1, 1),
        # default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        # tags=['example'],
        catchup=False,
) as dag:
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=SNOWFLAKE_TEST_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        # schema=SNOWFLAKE_SCHEMA,
        # role=SNOWFLAKE_ROLE,
    )
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    snowflake_op_sql_str >> t1


