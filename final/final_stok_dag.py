from datetime import datetime

from airflow import DAG
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


def load_to_silver(output_dir):
    
    current_date = datetime.today().strftime('%Y-%m-%d')
    path_for_data_file = output_dir + '/' + current_date

    spark = SparkSession.builder\
            .appName('lesson')\
            .master('local')\
            .getOrCreate()

    df = spark.read.parquet(f"/bronze/{path_for_data_file}")
    df = df.distinct()
    df.write.parquet(f"/silver/{path_for_data_file}")
    logging.info(f"out_of_stake on {current_date} saved to silver")

def load_to_warehouse(output_dir):

    connection = BaseHook.get_connection("aaa_greenplum_conn")
    gp_creds = {
        'host': connection.host
        , 'port': connection.port
        , 'database': connection.schema
        , 'user': connection.login
        , 'password': connection.password
    }

    gp_url = "jdbc:postgresql://%s:%s/%s" % \
        (gp_creds['host'], gp_creds['port'], gp_creds['database'])

    db_properties = {"user": gp_creds['user']
                    , "password": gp_creds['password']
                    , "driver": "org.postgresql.Driver"}
    
    spark = SparkSession.builder\
            .config("spark.driver.extraClassPath", '/opt/spark/jars/postgresql-42.2.20.jar')\
            .appName('lesson')\
            .master('local')\
            .getOrCreate()
    
    current_date = datetime.today().strftime('%Y-%m-%d')
    path_for_data_file = output_dir + '/' + current_date
    
    df = spark.read.parquet(f"/silver/{path_for_data_file}")
    df.write.jdbc(url=gp_url,table=path_for_data_file,properties=db_properties)
    logging.info(f"out_of_stake on {current_date} saved to warehouse")        

#######################################################
default_args = {
    "owner": "garanin",
    "email": ["garanin@rikaol.by"],
    "email_ob_failure": False
}


def to_silver_group():
    return PythonOperator(
        task_id="load_out_of_stok_to_silver",
        python_callable=load_to_silver,
        op_kwargs={"output_dir": "stok_state"},
        dag=dag
    )

def load_from_silver_to_warehouse():
    return PythonOperator(
        task_id="load_out_of_stok_to_silver",
        python_callable=load_to_warehouse,
        op_kwargs={"output_dir": "stok_state"},
        dag=dag
    )    

dag = DAG(
    dag_id="upload_out_of_stok_from_api",
    description="Upload json out_of_stok from Service",
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

dummy1 >> t1 >> t2 >> to_silver_group() >> load_from_silver_to_warehouse() >> dummy2  