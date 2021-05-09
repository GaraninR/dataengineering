from datetime import datetime

from airflow import DAG
from getstokstateoperator import GetStokStateOperator
from savedatafromdatabase import SaveDataFromDatabase


default_args = {
    'owner': 'garanin',
    'email': ['garanin@rikaol.by'],
    'email_on_failure': True
}

dag = DAG(
    'get_stok_and_save_data_from_base',
    description='Dag for Task 7',
    schedule_interval='@daily',
    start_date=datetime(2021,5,9,0,0),
    default_args=default_args
)

t1 = GetStokStateOperator(
    task_id='get_data_from_stok',
    dag=dag
)

t2 = SaveDataFromDatabase(
    task_id='save_data_from_database',
    dag=dag
)

t1 >> t2