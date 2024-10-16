from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

# Set default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "s3_list_objects_dag",
    default_args=default_args,
    description="DAG to list objects in S3 bucket and print them",
    schedule_interval=None,
) as dag:
    # Function to list objects in the S3 bucket
    def list_s3_objects(**kwargs):
        # Create a connection to S3 using the pre-configured 'aws_default' connection
        s3 = S3Hook(aws_conn_id="aws_default")

        # Specify the bucket name (replace with your bucket name)
        bucket_name = "your-s3-bucket-name"

        # Get a list of all object keys in the bucket
        object_keys = s3.list_keys(bucket_name=bucket_name)

        # Store the list of object keys in XCom for use in the next task
        kwargs["ti"].xcom_push(key="object_keys", value=object_keys)

    # Task 1: List objects in the S3 bucket
    task1 = PythonOperator(
        task_id="list_s3_objects",
        python_callable=list_s3_objects,
        provide_context=True,
    )

    # Function to receive object names and print them
    def print_object_names(**kwargs):
        # Retrieve the list of object keys from XCom
        ti = kwargs["ti"]
        object_keys = ti.xcom_pull(key="object_keys", task_ids="list_s3_objects")

        # Check if object_keys is not None
        if object_keys:
            # Print all object names via a for loop
            for obj_key in object_keys:
                print(f"Object: {obj_key}")
        else:
            print("No objects found in the bucket.")

    # Task 2: Print object names
    task2 = PythonOperator(
        task_id="print_object_names",
        python_callable=print_object_names,
        provide_context=True,
    )

    # Set the sequence of task execution
    task1 >> task2
