from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "copy_pdf_files_parallel",
    default_args=default_args,
    description="DAG to copy PDF files from one S3 bucket to another in parallel",
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
    def copy_pdf(pdf_key):
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"
        target_bucket = "dag-processed-files"

        # Copy PDF file from source bucket to target bucket
        s3_conn = s3.get_conn()
        copy_source = {"Bucket": source_bucket, "Key": pdf_key}
        s3_conn.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=pdf_key)

        print(f"Copied {pdf_key} to {target_bucket}")

    pdf_keys = get_pdf_keys()
    copy_pdf.expand(pdf_key=pdf_keys)
