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
    "delete_files_in_buckets",
    default_args=default_args,
    description="DAG to delete all files in the specified S3 buckets",
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def delete_files():
        s3 = S3Hook(aws_conn_id="aws_default")
        buckets = ["dag-test-pdf", "dag-processed-files"]

        for bucket in buckets:
            keys = s3.list_keys(bucket_name=bucket)
            if keys:
                s3.delete_objects(bucket, keys)
                print(f"Delete  {len(keys)} file s3 bucket {bucket}")
            else:
                print(f"In the buckets {bucket} there are no files to delete")

    delete_files()
