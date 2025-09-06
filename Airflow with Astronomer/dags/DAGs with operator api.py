'''
Taskflow API allows us to create tasks using decorators like @task. This is cleaner and more intuitive way
of writing tasks without needing to manually use operators like PythonOperator.
'''
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define DAG
with DAG(
    dag_id = 'math_sequence_with_taskflow',
    start_date = datetime(2025,9,5),
    schedule='@once',
    catchup=False
) as dag:
    # TASK1:START with the initial number
    @task
    def start_number():
        initial_value = 10
        print(f"Starting Number: {initial_value}")
        return initial_value
    @task
    def add5(num):
        new_value = num+5
        print(f"Added 5 to : {num}, new umber : {new_value}")
        return new_value
    @task
    def multiply2(num):
        new_num= num*2
        print(f"New number after multiplying 2 to : {num} is {new_num}")
        return new_num
    @task
    def sub3(num):
        new_num= num-3
        print(f"New number after subtracting 3 from : {num} is {new_num}")
        return new_num
    @task
    def square(num):
        new_num= num*num
        print(f"New number after squaring : {num} is {new_num}")
        return new_num
    # Setting the task dependencies
    start_val = start_number()
    added_vaue = add5(start_val)
    multiplied_value = multiply2(added_vaue)
    subtracted_value = sub3(multiplied_value)
    squared_value = square(subtracted_value)
