'''
Here we would try to do basic mathematical operations.
tsk1: Start with initial number
tsk2: Add 5 to number
tsk3: Multiply the result by 2
tsk4: Substract 3 from results
tsk5: Compute the square of the result.
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_number(**context):
    context["t1"].xcom_push(key='current_value',value=10,task_id = 'start_task')
    print("Starting number is 10")

def add5(**context):
    current_value = context['t1'].xcom_pull(key='current_value',task_id = 'add')
    new_value = current_value + 5
    context["t1"].xcom_push(key='current_value',value=new_value)
    print(f"Adding 5 to : {current_value}, so new value is : {new_value}")

def multiply_by_2(**context):
    current_value = context['t1'].xcom_pull(key='current_value',task_ids='multiply')
    new_value = current_value*2
    context["t1"].xcom_push(key='current_value',value=new_value)
    print(f"Multiplying 2 to : {current_value}, so new value is: {new_value}")

def subtarct3(**context):
    current_value = context['t1'].xcom_pull(key='current_value',task_ids='subtract')
    new_value = current_value-3
    context["t1"].xcom_push(key='current_value',value=new_value)
    print(f"subtracting 3 from : {current_value}, so new value is: {new_value}")    

def square2(**context):
    current_value = context['t1'].xcom_pull(key='current_value',task_ids='square')
    new_value = current_value*current_value
    context["t1"].xcom_push(key='current_value',value=new_value)
    print(f"Square of : {current_value}, so new value is: {new_value}") 

with DAG(
    'maths_operation',
    start_date = datetime(2025,8,31),
    schedule = '@once',
    catchup = False
) as dag:
    start = PythonOperator(task_id='start_task',python_callable = start_number)
    add = PythonOperator(task_id='add',python_callable = add5)
    mul = PythonOperator(task_id='multiply',python_callable = multiply_by_2)
    sub = PythonOperator(task_id='subtract',python_callable = subtarct3)
    square = PythonOperator(task_id='square',python_callable = square2)

    # Dependencies
    start >> add >> mul >> sub >> square
