from datetime import datetime

from airflow import DAG
from airflow.configuration import AIRFLOW_HOME
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.exceptions import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook

import logging
import json
import pyspark
import requests
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook

# from functions.load_to_bronze import load_to_bronze

# сдаюсь...
# я често пытался сделать в разных файлах, но хоть ты убейся, получаю
# ModuleNotFoundError: No module named 'functions'
# несколько дней пытался найти решение
# я видел в видео вариант решения с unset AIRFLOW_HOME - в моем случае это не помогло,
# просто Airflow тогда вообще не запускался
# оставляю рабочий вариант в одном файле


def load_to_bronze(table):

    connection = BaseHook.get_connection("aaa_postgres_conn_dshop_id")
    pg_creds = {
        'host': connection.host
        , 'port': connection.port
        , 'database': connection.schema
        , 'user': connection.login
        , 'password': connection.password
    }

    pg_url = "jdbc:postgresql://%s:%s/%s" % \
        (pg_creds['host'], pg_creds['port'], pg_creds['database'])

    pg_properties = {"user": pg_creds['user']
                    , "password": pg_creds['password']
                    , "driver": "org.postgresql.Driver"}

    spark = SparkSession.builder\
            .config("spark.driver.extraClassPath", '/opt/spark/jars/postgresql-42.2.20.jar')\
            .appName('lesson')\
            .master('local')\
            .getOrCreate()

    df = spark.read.jdbc(pg_url
                         , table=table
                         , properties=pg_properties)

    df.write.parquet(f"/bronze/{table}", mode="overwrite")
    logging.info(f"table {table} saved to bronze")



def load_to_silver(table):

    spark = SparkSession.builder\
            .config("spark.driver.extraClassPath", '/opt/spark/jars/postgresql-42.2.20.jar')\
            .appName('lesson')\
            .master('local')\
            .getOrCreate()
    
    df = spark.read.parquet(f"/bronze/{table}")
    df = df.distinct()
    df.write.parquet(f"/silver/{table}")
    logging.info(f"table {table} saved to silver")    

#######################################################

class AuthOperator(SimpleHttpOperator):
    def __init__(self, *args, **kwargs):
        super(AuthOperator, self).__init__(*args, **kwargs)
        self.xcom_push=True

    def execute(self, context):

        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)

        logging.info("Get token over 'AUTH'")
        stok_conn = BaseHook.get_connection(self.http_conn_id)

        response = http.run(endpoint="auth",
                            data=json.dumps({"username": stok_conn.login, "password": stok_conn.password}),
                            headers={"Content-Type": "application/json"})

        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

        token_json = json.loads(response.text)

        return token_json

class GetStokStateOperator(SimpleHttpOperator):
    def __init__(self, output_dir, *args, **kwargs):
        super(GetStokStateOperator, self).__init__(*args, **kwargs)
        self.output_dir = output_dir
    def execute(self, context):
        
        ti = context["ti"]
        auth_key = ti.xcom_pull(task_ids='auth')['access_token']

        token = "JWT " + auth_key
        headers = self.headers
        headers['Authorization'] = token
        
        http = HttpHook(method=self.method, http_conn_id=self.http_conn_id)

        logging.info("Get stok state")

        current_date = datetime.today().strftime('%Y-%m-%d')
        data = json.dumps({"date": current_date})

        response = requests.get("https://robot-dreams-de-api.herokuapp.com/out_of_stock", 
                                data=data, 
                                headers=headers)

        no_data_str = "No out_of_stock items for this date"
        json_string = json.dumps(response)
        if no_data_str in json_string:
            logging.info("No out_of_stock items for this date")
            return
                              
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        
        path_for_data_file = self.output_dir + '/' + current_date

        spark = SparkSession.builder\
            .config("spark.driver.extraClassPath", '/opt/spark/jars/postgresql-42.2.20.jar')\
            .appName('lesson')\
            .master('local')\
            .getOrCreate()

        df = spark.read.json(response.json())

        df.write.parquet(f"/bronze/{path_for_data_file}/data.json", mode="overwrite")
        logging.info("state of stok json saved to bronze")

#######################################################
default_args = {
    "owner": "garanin",
    "email": ["garanin@rikaol.by"],
    "email_ob_failure": False
}


def return_tables():
    return ['aisles', 'clients', 'departments', 
            'orders', 'products']


def to_bronze_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_bronze",
        python_callable=load_to_bronze,
        op_kwargs={"table": value},
        dag=dag
    )

def to_silver_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_silver",
        python_callable=load_to_silver,
        op_kwargs={"table": value},
        dag=dag
    )    

dag = DAG(
    dag_id="upload_dshop_from_postgres",
    description="Upload tables from PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2021, 6, 8),
    default_args=default_args
)

t1 = AuthOperator(
    task_id='auth',
    http_conn_id='aaa_stok_conn_id',
    dag=dag
)

t2 = GetStokStateOperator(
    task_id='get_data_from_stok',
    http_conn_id='aaa_stok_conn_id',
    endpoint='out_of_stock',
    method='GET',
    output_dir='/bronze/stok_state',
    headers = {'Content-Type': 'application/json'},
    dag=dag
)

dummy1 = DummyOperator(
    task_id="dummy_1",
    dag=dag
)

dummy2 = DummyOperator(
    task_id="dummy_2",
    dag=dag
)

dummy3 = DummyOperator(
    task_id="dummy_3",
    dag=dag
)

dummy4 = DummyOperator(
    task_id="dummy_4",
    dag=dag
)

dummy1 >> t1 >> t2 >> dummy2
for i in return_tables():
    dummy2 >> to_bronze_group(i) >> dummy3

for i in return_tables():
    dummy3 >> to_silver_group(i) >> dummy4    