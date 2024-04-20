
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils import timezone

@dag(
    dag_id="everyday",
    schedule="@daily",
    start_date=timezone.datetime(2024, 4, 20),
    tags=("DEB", "Skooldio"),
    catchup=False,
)
def taskflow_dag():
    @task
    def hello() -> str:
        return 'echo "hello"'

    @task
    def world() -> str:
        print("world")

    chain(hello(), world())

taskflow_dag()
    