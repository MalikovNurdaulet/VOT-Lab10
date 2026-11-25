from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    tags=["lab10"],
)
def hello_airflow():
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Привет из Airflow! Запуск: {{ run_id }}"',
    )
    @task
    def print_info():
        import platform
        print("Задача выполняется на узле:", platform.node())
        print("Версия Python:", platform.python_version())
    say_hello >> print_info()
hello_airflow()
