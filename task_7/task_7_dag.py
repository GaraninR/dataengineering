from datetime import datetime

from airflow import DAG
import json
import requests
import csv
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook


class AuthOperator(SimpleHttpOperator):
    def __init__(self, *args, **kwargs):
        super(AuthOperator, self).__init__(*args, **kwargs)
        self.xcom_push=True

    def execute(self, context):

        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)

        self.log.info(" --- Get token over 'AUTH' --- ")

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

        self.log.info(" --- Get stok state --- ")

        current_date = datetime.today().strftime('%Y-%m-%d')
        data = json.dumps({"date": current_date})

        
        # так не работает, возвращает "description": "Unsupported authorization type",
        # response = http.run(endpoint=self.endpoint,
        #                     data=data,
        #                     headers=headers,
        #                     extra_options=self.extra_options)

        # ну то есть я прямо сюда руками пишу валидный токен, который только что проверил в Postman
        # и всё равно пишет Unsupported authorization type
        # глянуть бы логи на стороне эндпоинта что именно туда приходит вместо этих хидеров

        # response = http.run(endpoint=self.endpoint,
        #                     data=data,
        #                     headers={'content-type': 'application/json', 'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MjA2MzMwNDIsImlhdCI6MTYyMDYzMjc0MiwibmJmIjoxNjIwNjMyNzQyLCJpZGVudGl0eSI6MX0.MoL--StghE1E1KxAkl92D9Giv6r6gmdT65H7nKoW_9k'},
        #                     extra_options=self.extra_options)

        # ...а вот так нормально работает ¯\_(ツ)_/¯...
        response = requests.get("https://robot-dreams-de-api.herokuapp.com/out_of_stock", 
                                data=data, 
                                headers=headers)

        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        
        path_for_data_file = self.output_dir + '/' + current_date

        # check output dir
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(path_for_data_file).mkdir(parents=True, exist_ok=True)

        with open(path_for_data_file + '/data.json', 'w') as outfile:
            json.dump(response.json(), outfile)


class SaveDataFromDatabase(PostgresOperator):
    def __init__(self, save_path, *args, **kwargs):
        super(SaveDataFromDatabase, self).__init__(*args, **kwargs)
        self.save_path = save_path

    def execute(self, context):

        self.log.info('Executing the query to the database')
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        result = cursor.fetchall()

        # check output dir
        Path(self.save_path).mkdir(parents=True, exist_ok=True)

        # generate filename
        current_date = datetime.today().strftime('%Y-%m-%d')
        backup_file_path = "{0}/{1}.csv".format(self.save_path, current_date)
        
        # bakup to csv
        temp_path = backup_file_path
        with open(temp_path, 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)


default_args = {
    'owner': 'garanin',
    'email': ['garanin@rikaol.by'],
    'email_on_failure': False
}

dag = DAG(
    'get_stok_and_save_data_from_base',
    description='Dag for Task 7',
    schedule_interval='@daily',
    start_date=datetime(2021, 5, 9, 0, 0),
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
    output_dir='stok_state',
    headers = {'Content-Type': 'application/json'},
    dag=dag
)

t3 = SaveDataFromDatabase(
    task_id='save_data_from_database',
    postgres_conn_id='aaa_postgres_conn_id',
    save_path="full_database_bakup",
    sql="""select * from orders o
                    join products p on o.product_id  = p.product_id
                    join aisles a on a.aisle_id = p.aisle_id 
                    join departments d on d.department_id = p.department_id 
                    join clients c on c.client_id = o.client_id""",
    dag=dag
)

t1 >> t2 >> t3
