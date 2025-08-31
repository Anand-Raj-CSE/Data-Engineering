from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Defining our task 1 
def preprocessdata():
    print("Preprocess data ......")

# Defining our task 2
def trainmodel():
    print('Training Model....')

def evaluate_model():
    print('Evaluate Models')

# Defing DAG
with DAG(
    'ml_pipieline',
    start_date = datetime(2024,1,1),
    schedule='@weekly'
)as dag:
    #defining the task
    preprocess = PythonOperator(task_id="preprocess_task",python_callable = preprocessdata)
    train = PythonOperator(task_id='train_task',python_callable = trainmodel)
    evaluate = PythonOperator(task_id='evaluate_task',python_callable = evaluate_model)

    #Set dependedncies , on which order you want to process
    preprocess >> train >> evaluate