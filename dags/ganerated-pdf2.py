from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

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
    # Function to generate dummy PDF files
    def generate_pdfs(**kwargs):
        local_dir = "/opt/airflow/storage/generated_pdfs/"
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        for i in range(1, 2001):
            file_name = f"file_{i}.pdf"
            file_path = os.path.join(local_dir, file_name)

            # Generate a dummy PDF file by writing minimal PDF content
            with open(file_path, "wb") as f:
                # Write minimal PDF header
                f.write(
                    b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
                )
                f.write(
                    b"2 0 obj\n<<\n/Type /Pages\n/Kids [3 0 R]\n/Count 1\n>>\nendobj\n"
                )
                f.write(
                    b"3 0 obj\n<<\n/Type /Page\n/Parent 2 0 R\n/MediaBox [0 0 612 792]\n"
                )
                f.write(b"/Contents 4 0 R\n>>\nendobj\n")
                f.write(
                    b"4 0 obj\n<<\n/Length 44\n>>\nstream\nBT\n/F1 24 Tf\n100 700 Td\n"
                )

                # Encode the string to bytes before writing
                line_content = f"({file_name}) Tj\nET\nendstream\nendobj\n"
                f.write(line_content.encode("utf-8"))

                f.write(
                    b"5 0 obj\n<<\n/Type /Font\n/Subtype /Type1\n/Name /F1\n/BaseFont /Helvetica\n"
                )
                f.write(
                    b">>\nendobj\nxref\n0 6\n0000000000 65535 f \n0000000010 00000 n \n"
                )
                f.write(
                    b"0000000079 00000 n \n0000000178 00000 n \n0000000295 00000 n \n"
                )
                f.write(
                    b"0000000410 00000 n \ntrailer\n<<\n/Size 6\n/Root 1 0 R\n>>\nstartxref\n"
                )
                f.write(b"522\n%%EOF\n")

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
