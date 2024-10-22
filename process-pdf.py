from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import io
from pdfminer.high_level import extract_text

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "process_pdf_files_parallel",
    default_args=default_args,
    description="DAG to process PDF files from S3 in parallel",
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
        target_bucket = "dag-processed-files"

        # Download PDF file into memory
        pdf_obj = s3.get_key(key=pdf_key, bucket_name=source_bucket)
        pdf_content = pdf_obj.get()["Body"].read()

        # Extract text from PDF
        pdf_stream = io.BytesIO(pdf_content)
        text = extract_text(pdf_stream)

        # Save extracted text to a string
        text_key = pdf_key.replace(".pdf", ".txt")

        # Upload the text file to the target bucket
        s3.load_string(
            string_data=text,
            key=text_key,
            bucket_name=target_bucket,
            replace=True,
        )

        print(f"Processed and uploaded: {text_key}")

    pdf_keys = get_pdf_keys()
    process_pdf.expand(pdf_key=pdf_keys)
