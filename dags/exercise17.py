from datetime import datetime

from airflow.decorators import dag
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)
def exercise17():

  drop_table = SqliteOperator(task_id="drop_table", sql="sql/drop_table.sql")

  create_table = SqliteOperator(task_id="create_table", sql="sql/create_table.sql")

  add_data = SqliteOperator(task_id="add_data", sql="sql/add_data.sql")

  drop_table >> create_table >> add_data

_ = exercise17()