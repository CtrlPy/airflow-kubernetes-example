from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import io

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "process_and_copy_pdf_files_parallel",
    default_args=default_args,
    description="DAG to process and copy PDF files from one S3 bucket to another in parallel",
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def get_pdf_keys():
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"
        pdf_keys = s3.list_keys(bucket_name=source_bucket)
        return [key for key in pdf_keys if key.endswith(".pdf")]

    @task
    def process_pdf(pdf_key):
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"

        # Download PDF file into memory
        s3_conn = s3.get_conn()
        obj = s3_conn.get_object(Bucket=source_bucket, Key=pdf_key)
        pdf_content = obj["Body"].read()

        # Process the PDF file (placeholder for future processing)
        print(f"Processing PDF file: {pdf_key}")
        # Here you can add your transformation or recognition code
        # For now, we'll just pass the content as is
        processed_content = pdf_content

        # Return the processed content and the key
        return {"pdf_key": pdf_key, "processed_content": processed_content}

    @task
    def upload_pdf(processed_data):
        s3 = S3Hook(aws_conn_id="aws_default")
        target_bucket = "dag-processed-files"
        pdf_key = processed_data["pdf_key"]
        processed_content = processed_data["processed_content"]

        # Upload the processed PDF file to the target bucket
        s3_conn = s3.get_conn()
        s3_conn.put_object(Bucket=target_bucket, Key=pdf_key, Body=processed_content)

        print(f"Uploaded {pdf_key} to {target_bucket}")

    pdf_keys = get_pdf_keys()
    processed_pdfs = process_pdf.expand(pdf_key=pdf_keys)
    upload_pdf.expand(processed_data=processed_pdfs)
