FROM apache/airflow:2.10.2-python3.9

# Switch to airflow user
USER airflow

# Install the required package as airflow user
RUN pip install --no-cache-dir pdfminer.six==20221105
