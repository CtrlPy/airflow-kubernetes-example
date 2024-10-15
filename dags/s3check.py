from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

# Set default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "s3_pdf_files_dag",
    default_args=default_args,
    description="DAG to count and download PDF files from S3",
    schedule_interval=None,
) as dag:
    # Function to count PDF files in the S3 bucket
    def count_pdf_files(**kwargs):
        # Create a connection to S3 using the pre-configured 'aws_default' connection
        s3 = S3Hook(aws_conn_id="aws_default")

        # Specify the bucket name (replace with your bucket name)
        bucket_name = "pinnacle-medical-records"

        # Get a list of all files in the bucket
        files = s3.list_keys(bucket_name=bucket_name)

        # Filter files, keeping only those that end with '.pdf'
        pdf_files = [f for f in files if f.endswith(".pdf")]

        # Count the number of PDF files
        pdf_count = len(pdf_files)

        # Store the list of files and their count in XCom for use in subsequent tasks
        kwargs["ti"].xcom_push(key="pdf_files", value=pdf_files)
        kwargs["ti"].xcom_push(key="pdf_count", value=pdf_count)

        print(f"Number of PDF files: {pdf_count}")

    # Task 1: Count the number of PDF files in S3
    task1 = PythonOperator(
        task_id="count_pdf_files",
        python_callable=count_pdf_files,
        provide_context=True,
    )

    # Function to download PDF files from S3
    def download_pdf_files(**kwargs):
        # Retrieve the list of PDF files from the previous task
        ti = kwargs["ti"]
        pdf_files = ti.xcom_pull(key="pdf_files", task_ids="count_pdf_files")

        # Create a connection to S3
        s3 = S3Hook(aws_conn_id="aws_default")

        # Specify the bucket name (replace with your bucket name)
        bucket_name = "pinnacle-medical-records"

        # Folder to save the files (replace with your local path)
        local_dir = "/opt/airflow/storage/pdf_files/"

        # Ensure the folder exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # Download each PDF file
        for file_key in pdf_files:
            # Determine the full path to the local file
            local_file_path = os.path.join(local_dir, os.path.basename(file_key))

            # Download the file
            s3.get_key(key=file_key, bucket_name=bucket_name).download_file(
                local_file_path
            )
            print(f"Downloaded file: {file_key}")

    # Task 2: Download PDF files from S3
    task2 = PythonOperator(
        task_id="download_pdf_files",
        python_callable=download_pdf_files,
        provide_context=True,
    )

    # Function to retrieve and process the downloaded PDF files
    def process_pdf_files(**kwargs):
        # Folder with PDF files (replace with your local path)
        local_dir = "/opt/airflow/storage/pdf_files/"

        # Get a list of files in the local directory
        pdf_files = [f for f in os.listdir(local_dir) if f.endswith(".pdf")]

        # Output the filenames or perform other necessary processing
        for pdf_file in pdf_files:
            print(f"Processing file: {pdf_file}")
            # Here you can add code to further process each PDF file

    # Task 3: Retrieve and process the PDF files
    task3 = PythonOperator(
        task_id="process_pdf_files",
        python_callable=process_pdf_files,
        provide_context=True,
    )

    # Set the sequence of task execution
    task1 >> task2 >> task3
