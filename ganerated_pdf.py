from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "generate_and_upload_pdfs",
    default_args=default_args,
    description="DAG to generate 1000 PDF files and upload them to S3",
    schedule_interval=None,
    catchup=False,
) as dag:
    # Function to generate PDF files
    def generate_pdfs(**kwargs):
        local_dir = "/opt/airflow/storage/generated_pdfs/"
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        for i in range(1, 1001):
            file_name = f"file_{i}.pdf"
            file_path = os.path.join(local_dir, file_name)

            # Generate PDF file
            c = canvas.Canvas(file_path, pagesize=letter)
            c.drawString(100, 750, f"This is PDF file number {i}")
            c.save()

            print(f"Generated file: {file_name}")

        # Push the local directory path to XCom for use in the next task
        kwargs["ti"].xcom_push(key="local_dir", value=local_dir)

    # Function to upload PDF files to S3
    def upload_pdfs_to_s3(**kwargs):
        # Retrieve the local directory path from XCom
        ti = kwargs["ti"]
        local_dir = ti.xcom_pull(key="local_dir", task_ids="generate_pdfs")

        # Connect to S3
        s3 = S3Hook(aws_conn_id="aws_default")
        bucket_name = "dag-test-pdf"

        # Check if the bucket exists
        if not s3.check_for_bucket(bucket_name):
            s3.create_bucket(bucket_name)

        # Get the list of PDF files
        pdf_files = [f for f in os.listdir(local_dir) if f.endswith(".pdf")]

        # Upload each file to S3
        for pdf_file in pdf_files:
            local_file_path = os.path.join(local_dir, pdf_file)
            s3_key = pdf_file  # You can change the key if needed

            s3.load_file(
                filename=local_file_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True,
            )

            print(f"Uploaded file to S3: {pdf_file}")

    # Task 1: Generate PDF files
    generate_pdfs_task = PythonOperator(
        task_id="generate_pdfs",
        python_callable=generate_pdfs,
    )

    # Task 2: Upload PDF files to S3
    upload_pdfs_task = PythonOperator(
        task_id="upload_pdfs_to_s3",
        python_callable=upload_pdfs_to_s3,
    )

    # Set the sequence of task execution
    generate_pdfs_task >> upload_pdfs_task
