from airflow.models import BaseOperator

class GetStokStateOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(GetStokStateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        print("Execute!")