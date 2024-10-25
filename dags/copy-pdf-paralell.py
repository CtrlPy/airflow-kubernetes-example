from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import math

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Встановіть розмір пакету відповідно до ваших потреб
BATCH_SIZE = 500

with DAG(
    "process_and_copy_pdf_files_batched",
    default_args=default_args,
    description="DAG для обробки та копіювання PDF-файлів пакетами без зміни конфігурації Airflow",
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
    def split_into_batches(pdf_keys):
        """Розбиває список ключів на пакети."""
        num_batches = math.ceil(len(pdf_keys) / BATCH_SIZE)
        return [
            pdf_keys[i * BATCH_SIZE : (i + 1) * BATCH_SIZE] for i in range(num_batches)
        ]

    @task(pool="pdf_processing_pool")
    def process_batch(batch):
        """Обробляє пакет PDF-файлів."""
        s3 = S3Hook(aws_conn_id="aws_default")
        source_bucket = "dag-test-pdf"
        target_bucket = "dag-processed-files"
        s3_conn = s3.get_conn()

        for pdf_key in batch:
            print(f"Processing {pdf_key}")
            # Завантаження PDF-файлу
            obj = s3_conn.get_object(Bucket=source_bucket, Key=pdf_key)
            pdf_content = obj["Body"].read()

            # Обробка PDF-файлу (додайте свій код обробки тут)
            processed_content = pdf_content  # Поки що без змін

            # Завантаження обробленого файлу в цільовий бакет
            s3_conn.put_object(
                Bucket=target_bucket, Key=pdf_key, Body=processed_content
            )
            print(f"Uploaded {pdf_key} to {target_bucket}")

    pdf_keys = get_pdf_keys()
    batches = split_into_batches(pdf_keys)
    process_batch.expand(batch=batches)
