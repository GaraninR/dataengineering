from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.base_hook import BaseHook

import logging
import pyspark
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook

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

def load_to_warehouse(table):

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
    
    df = spark.read.parquet(f"/silver/{table}")
    df.write.jdbc(url=gp_url,table=table,mode='overwrite',properties=db_properties)
    logging.info(f"table {table} saved to warehouse")          

#######################################################
default_args = {
    "owner": "garanin",
    "email": ["garanin@rikaol.by"],
    "email_ob_failure": False
}


def return_tables():
    return ['aisles', 'clients', 'departments', 
            'orders', 'products', 'stores', 'store_types', 'location_areas']


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

def load_from_silver_to_warehouse(value):
    return PythonOperator(
        task_id="load_"+value+"_from_silver_to_warehouse",
        python_callable=load_to_warehouse,
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

for i in return_tables():
    dummy1 >> to_bronze_group(i) >> dummy2

for i in return_tables():
    dummy2 >> to_silver_group(i) >> dummy3

for i in return_tables():
    dummy3 >> load_from_silver_to_warehouse(i) >> dummy4    