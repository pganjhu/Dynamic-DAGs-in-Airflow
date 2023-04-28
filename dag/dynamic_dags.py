# Creating DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello!")
    
def print_world():
    print("World..!")
    
def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id, schedule_interval=schedule_interval, start_date=start_date)

    # define tasks
    task1 = PythonOperator(
        task_id='task1',
        python_callable=print_hello,
        dag=dag
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=print_world,
        dag=dag
    )

    task1 >> task2

    return dag

# define a function that returns a list of DAG IDs
def get_dag_ids():
    dag_ids = ['dynamic_dag_1', 'dynamic_dag_2', 'dynamic_dag_3']
    return dag_ids

# define the main DAG that will generate the dynamic DAGs
main_dag = DAG(
    'main_dag',
    start_date=datetime(2023, 4, 28),
    schedule_interval='@daily'
)

# use a PythonOperator to generate the dynamic DAGs
generate_dags = PythonOperator(
    task_id='generate_dags',
    python_callable=get_dag_ids,
    op_kwargs={'start_date': datetime(2023, 4, 28)},
    dag=main_dag
)

# loop through the list of DAG IDs and generate a dynamic DAG for each one
for dag_id in get_dag_ids():
    dynamic_dag = PythonOperator(
        task_id=dag_id,
        python_callable=create_dag,
        op_kwargs={'dag_id': dag_id, 'schedule_interval': '@hourly', 'start_date': datetime(2023, 4, 28)},
        dag=main_dag
    )
    generate_dags >> dynamic_dag
