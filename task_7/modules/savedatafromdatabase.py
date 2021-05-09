from airflow.models import BaseOperator

class SaveDataFromDatabase(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(SaveDataFromDatabase, self).__init__(*args, **kwargs)

    def execute(self, context):
        print("Execute!")