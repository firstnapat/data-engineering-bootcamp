from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _get_date_part(ds, **context):
    print(ds)
    # print(ds_format(ds, "%Y-%m-%d", "%Y/%m/%d/"))


with DAG(
    dag_id="play_with_templating",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    run_this = PythonOperator(
        task_id="get_date_part",
        python_callable=_get_date_part,
        op_kwargs={"ds": "{{ ds }}"},
    )