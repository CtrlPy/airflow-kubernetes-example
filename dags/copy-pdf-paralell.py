from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
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
        """Отримує список PDF-файлів з вихідного бакету."""
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"
        pdf_keys = s3.list_keys(bucket_name=source_bucket)
        return [key for key in pdf_keys if key.endswith(".pdf")]

    @task
    def process_pdf(pdf_key):
        """Завантажує та обробляє PDF-файл."""
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"

        # Завантаження PDF-файлу
        s3_conn = s3.get_conn()
        obj = s3_conn.get_object(Bucket=source_bucket, Key=pdf_key)
        pdf_content = obj["Body"].read()

        # Обробка PDF-файлу
        print(f"Processing PDF file: {pdf_key}")
        # Тут ви можете додати свій код обробки
        processed_content = pdf_content  # Поки що не змінюємо вміст

        # Повертаємо оброблений вміст та ключ файлу
        return {"pdf_key": pdf_key, "processed_content": processed_content}

    @task
    def upload_pdf(processed_data):
        """Завантажує оброблений PDF-файл у цільовий бакет."""
        s3 = S3Hook(aws_conn_id="aws_default")
        target_bucket = "dag-processed-files"
        pdf_key = processed_data["pdf_key"]
        processed_content = processed_data["processed_content"]

        # Завантаження обробленого файлу в цільовий бакет
        s3_conn = s3.get_conn()
        s3_conn.put_object(Bucket=target_bucket, Key=pdf_key, Body=processed_content)

        print(f"Uploaded {pdf_key} to {target_bucket}")

    # Визначаємо послідовність тасків
    pdf_keys = get_pdf_keys()
    processed_pdfs = process_pdf.expand(pdf_key=pdf_keys)
    upload_pdf.expand(processed_data=processed_pdfs)
